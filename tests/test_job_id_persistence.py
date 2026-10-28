"""Persistence round-trip test.

Regression test for a real bug found while auditing the project: the
MINILAKE_PERSIST env var and persistence.py's save_state/load_state existed,
but were never actually invoked from app.py's lifespan — so setting
MINILAKE_PERSIST=1 silently did nothing. This test starts an isolated
minilake subprocess (not the shared session-scoped server used by other
tests, since this specifically needs a real process restart), creates real
state, restarts the process against the same data dir, and verifies the
state survived — using the real databricks-sdk client throughout.
"""

import shutil
import tempfile
from datetime import timedelta

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, SqlTask, SqlTaskQuery, Task
from databricks.sdk.service.sql import CreateQueryRequestQuery
from persistence_helpers import _free_port, _start_minilake, _stop_minilake


@pytest.mark.serial
def test_persisted_jobs_runs_and_outputs_survive_restart():
    """Persisted numeric job/run IDs and task output remain usable after restart."""
    data_dir = tempfile.mkdtemp(prefix="minilake_jobs_persist_")
    port = _free_port()
    proc = None
    try:
        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")

        warehouse = client.warehouses.create(name="persisted_job_wh", cluster_size="Small")
        query = client.queries.create(
            query=CreateQueryRequestQuery(
                display_name="persisted_job_query",
                query_text="SELECT 42 AS answer",
                warehouse_id=warehouse.id,
            )
        )
        job = client.jobs.create(
            name="persisted_job",
            tasks=[
                Task(
                    task_key="main",
                    sql_task=SqlTask(warehouse_id=warehouse.id, query=SqlTaskQuery(query_id=query.id)),
                )
            ],
        )
        run = client.jobs.run_now(job_id=job.job_id).result(timeout=timedelta(minutes=2))
        assert run.state.result_state.value == "SUCCESS"
        task_run_id = run.tasks[0].run_id
        assert "answer" in (client.jobs.get_run_output(run_id=task_run_id).logs or "")

        _stop_minilake(proc)
        proc = None

        # Restart against the same snapshot and verify the original numeric IDs
        # still address the job, run, and task output.
        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")
        assert job.job_id in {item.job_id for item in client.jobs.list()}
        assert client.jobs.get(job_id=job.job_id).settings.name == "persisted_job"

        restored_run = client.jobs.get_run(run_id=run.run_id)
        assert restored_run.run_id == run.run_id
        assert restored_run.state.result_state.value == "SUCCESS"
        assert "answer" in (client.jobs.get_run_output(run_id=run.run_id).logs or "")
        assert "answer" in (client.jobs.get_run_output(run_id=task_run_id).logs or "")

        client.jobs.update(job_id=job.job_id, new_settings=JobSettings(name="persisted_job_updated"))
        assert client.jobs.get(job_id=job.job_id).settings.name == "persisted_job_updated"

        # Restored counters must not reuse an existing key and overwrite state.
        next_job = client.jobs.create(name="created_after_restart", tasks=[])
        assert next_job.job_id != job.job_id
        assert client.jobs.get(job_id=job.job_id).settings.name == "persisted_job_updated"

        rerun = client.jobs.run_now(job_id=job.job_id).result(timeout=timedelta(minutes=2))
        assert rerun.run_id != run.run_id
        assert client.jobs.get_run(run_id=run.run_id).run_id == run.run_id

        print("✓ persisted jobs, runs, counters, and SQL task output survive a process restart")
    finally:
        if proc is not None:
            _stop_minilake(proc)
        shutil.rmtree(data_dir, ignore_errors=True)
