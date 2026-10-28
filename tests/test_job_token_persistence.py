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
from databricks.sdk.service.catalog import DataSourceFormat, TableType
from databricks.sdk.service.sql import ColumnInfo
from persistence_helpers import _free_port, _start_minilake, _stop_minilake


@pytest.mark.serial
def test_run_now_token_survives_restart():
    """Test: MINILAKE_PERSIST=1 really saves state on shutdown and restores
    it (including re-attaching the catalog's own DuckDB database) on the
    next startup against the same data dir."""
    data_dir = tempfile.mkdtemp(prefix="minilake_persist_")
    port = _free_port()
    proc = None
    try:
        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")
        catalog_name = "persisted_catalog"
        client.catalogs.create(name=catalog_name)
        job = client.jobs.create(name="persisted_idempotent_job", tasks=[])
        token = "persisted-run-now-token"
        original_run = client.jobs.run_now(job_id=job.job_id, idempotency_token=token).result(
            timeout=timedelta(seconds=30)
        )
        _stop_minilake(proc)
        proc = None

        # Restart against the SAME data dir - metadata must survive.
        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")
        restored = client.catalogs.get(name=catalog_name)
        assert restored.name == catalog_name

        # JSON snapshots stringify dictionary keys. Job and run IDs must be
        # restored as integers so both direct lookups and token retries work.
        assert client.jobs.get(job_id=job.job_id).job_id == job.job_id
        assert client.jobs.get_run(run_id=original_run.run_id).run_id == original_run.run_id
        retried_run = client.jobs.run_now(job_id=job.job_id, idempotency_token=token).result(
            timeout=timedelta(seconds=30)
        )
        assert retried_run.run_id == original_run.run_id
        assert [r.run_id for r in client.jobs.list_runs(job_id=job.job_id)] == [original_run.run_id]

        # And it must be a REAL, still-queryable catalog after restart, not
        # just restored metadata - proves the catalog's DuckDB database was
        # really re-ATTACHed, not just its name remembered.
        client.schemas.create(name="s", catalog_name=catalog_name)
        client.tables.create(
            name="t",
            catalog_name=catalog_name,
            schema_name="s",
            storage_location=f"/data/{catalog_name}/s/t",
            columns=[ColumnInfo(name="id", type_text="INTEGER")],
            table_type=TableType.MANAGED,
            data_source_format=DataSourceFormat.DELTA,
        )

        print("✓ MINILAKE_PERSIST=1 really survives a process restart")
    finally:
        if proc is not None:
            _stop_minilake(proc)
        shutil.rmtree(data_dir, ignore_errors=True)
