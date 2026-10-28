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

import asyncio
import base64
import json
import shutil
import tempfile

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import DataSourceFormat, TableType
from databricks.sdk.service.sql import ColumnInfo
from persistence_helpers import _free_port, _start_minilake, _stop_minilake

from minilake import persistence
from minilake.persistence import save_state


def test_save_state_serialization_failure_preserves_previous_snapshot(tmp_path):
    snapshot_path = tmp_path / "snapshot.json"
    previous_snapshot = {"format_version": 1, "modules": {"catalogs": {"existing": True}}}
    snapshot_path.write_text(json.dumps(previous_snapshot))

    asyncio.run(save_state(snapshot_path, {"dbfs": lambda: {"buffer": bytearray(b"pending")}}))

    assert json.loads(snapshot_path.read_text()) == previous_snapshot
    assert not list(tmp_path.glob(".snapshot.json.*.tmp"))


def test_save_state_replace_failure_preserves_previous_snapshot(tmp_path, monkeypatch):
    snapshot_path = tmp_path / "snapshot.json"
    previous_snapshot = {"format_version": 1, "modules": {"catalogs": {"existing": True}}}
    snapshot_path.write_text(json.dumps(previous_snapshot))

    def fail_replace(source, destination):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(persistence.os, "replace", fail_replace)
    asyncio.run(save_state(snapshot_path, {"catalogs": lambda: {"current": True}}))

    assert json.loads(snapshot_path.read_text()) == previous_snapshot
    assert not list(tmp_path.glob(".snapshot.json.*.tmp"))


@pytest.mark.serial
def test_unfinished_upload_preserves_snapshot_after_restart():
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
        _stop_minilake(proc)
        proc = None

        # Restart against the SAME data dir - metadata must survive.
        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")
        restored = client.catalogs.get(name=catalog_name)
        assert restored.name == catalog_name

        # An open upload is transient. Shutting down with a bytearray buffer
        # must not make the snapshot unreadable or discard other saved state.
        empty_handle = client.dbfs.create("/unfinished_empty.txt", overwrite=True).handle
        upload_handle = client.dbfs.create("/unfinished.txt", overwrite=True).handle
        client.dbfs.add_block(upload_handle, base64.b64encode(b"pending").decode("ascii"))
        _stop_minilake(proc)
        proc = None

        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")
        restored = client.catalogs.get(name=catalog_name)
        assert restored.name == catalog_name
        for path, handle in (
            ("/unfinished_empty.txt", empty_handle),
            ("/unfinished.txt", upload_handle),
        ):
            with pytest.raises(Exception):
                client.dbfs.get_status(path)
            with pytest.raises(Exception):
                client.dbfs.add_block(handle, base64.b64encode(b"stale").decode("ascii"))

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
