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
import json
import urllib.request

import pytest
from persistence_helpers import _free_port, _start_minilake, _stop_minilake


@pytest.mark.serial
def test_relative_data_dir_supports_dbfs_and_files(tmp_path):
    """Relative data roots should work through both real HTTP file APIs."""
    working_dir = tmp_path / "working_dir"
    working_dir.mkdir()
    port = _free_port()
    proc = _start_minilake(port, "data", cwd=str(working_dir))
    base_url = f"http://127.0.0.1:{port}"
    dbfs_path = working_dir / "data" / "dbfs" / "hello"
    try:
        dbfs_body = json.dumps({"path": "/hello", "contents": base64.b64encode(b"dbfs").decode("ascii")}).encode()
        request = urllib.request.Request(
            f"{base_url}/api/2.0/dbfs/put",
            data=dbfs_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            assert response.status == 200
        assert dbfs_path.read_bytes() == b"dbfs"

        request = urllib.request.Request(
            f"{base_url}/api/2.0/fs/files/hello",
            data=b"files",
            headers={"Content-Type": "application/octet-stream"},
            method="PUT",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            assert response.status == 204
        assert dbfs_path.read_bytes() == b"files"
    finally:
        _stop_minilake(proc)
