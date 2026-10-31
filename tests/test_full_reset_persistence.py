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
import urllib.request
import uuid

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from persistence_helpers import _free_port, _start_minilake, _stop_minilake


@pytest.mark.serial
def test_full_reset_removes_default_database_across_restart():
    """A full reset must remove unqualified SQL tables from the shared database,
    including after a process restart against the same data directory."""
    data_dir = tempfile.mkdtemp(prefix="minilake_full_reset_")
    port = _free_port()
    proc = None
    table_name = f"reset_survivor_{uuid.uuid4().hex}"
    try:
        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")
        warehouse = client.warehouses.create(name="full_reset_before")
        created = client.statement_execution.execute_statement(
            warehouse_id=warehouse.id,
            statement=f"CREATE TABLE {table_name} AS SELECT 777 AS value",
        )
        assert created.status.state.value == "SUCCEEDED"

        request = urllib.request.Request(f"http://127.0.0.1:{port}/_minilake/reset?full=true", method="POST")
        assert urllib.request.urlopen(request, timeout=10).status == 200

        def assert_table_is_gone(client: WorkspaceClient, warehouse_name: str) -> None:
            warehouse = client.warehouses.create(name=warehouse_name)
            with pytest.raises(DatabricksError) as exc:
                client.statement_execution.execute_statement(
                    warehouse_id=warehouse.id,
                    statement=f"SELECT value FROM {table_name}",
                )
            assert "does not exist" in str(exc.value).lower() or "not found" in str(exc.value).lower()

        assert_table_is_gone(client, "full_reset_immediate")

        _stop_minilake(proc)
        proc = None
        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")
        assert_table_is_gone(client, "full_reset_after_restart")
    finally:
        if proc is not None:
            _stop_minilake(proc)
        shutil.rmtree(data_dir, ignore_errors=True)
