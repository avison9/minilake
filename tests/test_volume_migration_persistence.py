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

import base64
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.catalog import VolumeType
from persistence_helpers import _free_port, _start_minilake, _stop_minilake


@pytest.mark.serial
def test_legacy_volume_files_migrate_on_restart():
    """Files created under the pre-UC volume path remain readable after upgrade."""
    data_dir = tempfile.mkdtemp(prefix="minilake_volume_migration_")
    port = _free_port()
    proc = None
    try:
        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")
        catalog_name = f"migration_{uuid.uuid4().hex[:8]}"
        schema_name = "migration_schema"
        volume_name = "migration_volume"
        client.catalogs.create(name=catalog_name)
        client.schemas.create(name=schema_name, catalog_name=catalog_name)
        client.volumes.create(
            name=volume_name,
            catalog_name=catalog_name,
            schema_name=schema_name,
            volume_type=VolumeType.MANAGED,
        )

        payload = b"legacy volume data survives upgrade\n"
        legacy_file = Path(data_dir) / "dbfs" / "Volumes" / catalog_name / schema_name / volume_name / "legacy.txt"
        legacy_file.parent.mkdir(parents=True)
        legacy_file.write_bytes(payload)
        # The process running before an upgrade cannot have written the new marker.
        (Path(data_dir) / "volumes" / ".legacy_volume_migration_complete").unlink()
        volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/legacy.txt"
        full_volume_name = f"{catalog_name}.{schema_name}.{volume_name}"

        _stop_minilake(proc)
        proc = None
        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")

        assert client.files.download(volume_path).contents.read() == payload
        dbfs_data = client.dbfs.read(path=volume_path).data
        assert base64.b64decode(dbfs_data) == payload
        assert (
            Path(data_dir) / "volumes" / catalog_name / schema_name / volume_name / "legacy.txt"
        ).read_bytes() == payload
        assert not legacy_file.exists()

        client.volumes.delete(name=full_volume_name)
        with pytest.raises(DatabricksError):
            client.files.download(volume_path)
        with pytest.raises(DatabricksError):
            client.dbfs.read(path=volume_path)
    finally:
        if proc is not None:
            _stop_minilake(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


@pytest.mark.serial
def test_deleted_legacy_volume_files_stay_deleted_after_restart():
    """An upgrade must not import orphaned files when a volume name is reused."""
    data_dir = tempfile.mkdtemp(prefix="minilake_orphaned_volume_")
    port = _free_port()
    proc = None
    try:
        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")
        catalog_name = f"orphan_{uuid.uuid4().hex[:8]}"
        schema_name = "orphan_schema"
        volume_name = "reused_volume"
        full_volume_name = f"{catalog_name}.{schema_name}.{volume_name}"
        volume_path = f"/Volumes/{full_volume_name.replace('.', '/')}/old.txt"
        client.catalogs.create(name=catalog_name)
        client.schemas.create(name=schema_name, catalog_name=catalog_name)
        client.volumes.create(
            name=volume_name,
            catalog_name=catalog_name,
            schema_name=schema_name,
            volume_type=VolumeType.MANAGED,
        )

        # Simulate an old release: volume deletion removed its UC directory but
        # left Files/DBFS content under the legacy dbfs/Volumes path.
        legacy_file = Path(data_dir) / "dbfs" / "Volumes" / catalog_name / schema_name / volume_name / "old.txt"
        legacy_file.parent.mkdir(parents=True)
        legacy_file.write_bytes(b"content from deleted volume")
        client.volumes.delete(name=full_volume_name)
        migration_marker = Path(data_dir) / "volumes" / ".legacy_volume_migration_complete"
        migration_marker.unlink()

        _stop_minilake(proc)
        proc = None
        # First upgraded startup must not migrate a volume that had already been deleted.
        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")
        client.volumes.create(
            name=volume_name,
            catalog_name=catalog_name,
            schema_name=schema_name,
            volume_type=VolumeType.MANAGED,
        )

        _stop_minilake(proc)
        proc = None
        # A later restart must not reinterpret the new volume as a pre-upgrade one.
        proc = _start_minilake(port, data_dir)
        client = WorkspaceClient(host=f"http://127.0.0.1:{port}", token="dev", auth_type="pat")
        with pytest.raises(DatabricksError):
            client.files.download(volume_path)
        with pytest.raises(DatabricksError):
            client.dbfs.read(path=volume_path)
        assert legacy_file.read_bytes() == b"content from deleted volume"
    finally:
        if proc is not None:
            _stop_minilake(proc)
        shutil.rmtree(data_dir, ignore_errors=True)
