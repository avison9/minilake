"""Process helpers for isolated persistence regressions."""

import os
import socket
import subprocess
import time
import urllib.request


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _drain(proc: subprocess.Popen) -> str:
    """Whatever the subprocess printed, without blocking on a live process."""
    if proc.poll() is None:
        return "(process still running)"
    out, err = proc.communicate(timeout=5)
    return (out or b"").decode(errors="replace") + (err or b"").decode(errors="replace")


def _wait_healthy(proc: subprocess.Popen, url: str, timeout: float = 60.0) -> None:
    """Poll until minilake answers, failing early and loudly if it died instead.

    Checking `proc.poll()` matters: without it a crashed server is indistinguishable from a
    slow one, and the test reports a timeout while the actual traceback sits unread in a
    pipe. CI is also several times slower to start than a laptop, hence the generous
    timeout — this waits on a real process spawn, not a request.
    """
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"minilake exited with code {proc.returncode} before serving:\n{_drain(proc)}")
        try:
            if urllib.request.urlopen(f"{url}/_minilake/health", timeout=1).status == 200:
                return
        except Exception as e:
            last_error = e
            time.sleep(0.3)
    raise RuntimeError(f"minilake at {url} did not become healthy in time: {last_error}\n{_drain(proc)}")


def _start_minilake(port: int, data_dir: str, *, cwd: str | None = None) -> subprocess.Popen:
    env = os.environ.copy()
    env["MINILAKE_DATA_DIR"] = data_dir
    env["MINILAKE_PERSIST"] = "1"
    proc = subprocess.Popen(
        ["minilake", "--port", str(port), "--host", "127.0.0.1"],
        env=env,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_healthy(proc, f"http://127.0.0.1:{port}")
    except Exception:
        _stop_minilake(proc)
        raise
    return proc


def _stop_minilake(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
