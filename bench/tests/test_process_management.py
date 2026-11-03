"""No model is required to verify process ownership and failure cleanup."""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys

import pytest

from scripts import manage_backends as manager


@pytest.fixture
def settings(tmp_path, monkeypatch):
    run_dir = tmp_path / "run"
    monkeypatch.setattr(manager, "RUN_DIR", run_dir)
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    model = tmp_path / "fake.gguf"
    model.touch()
    monkeypatch.setenv("GATEWAY_MODEL", str(model))
    monkeypatch.setenv("START_TIMEOUT", "0.6")
    return tmp_path


def available_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_changed_identity_never_signals_unrelated_process(settings):
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        manager.RUN_DIR.mkdir()
        path = manager.state_path(19000)
        path.write_text(json.dumps({"pid": process.pid, "port": 19000,
                                    "identity": {"started": "wrong", "command_sha256": "wrong"}}))
        with pytest.raises(RuntimeError, match="no signal sent"):
            manager.stop(19000)
        assert process.poll() is None
        assert path.exists()
    finally:
        process.terminate()
        process.wait(timeout=5)


def test_pid_only_record_is_rejected(settings):
    manager.RUN_DIR.mkdir()
    (manager.RUN_DIR / "backend-19000.pid").write_text(str(os.getpid()))
    with pytest.raises(RuntimeError, match="PID-only"):
        manager.stop(19000)


def test_busy_port_is_not_taken_over(settings):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        with pytest.raises(RuntimeError, match="in use"):
            manager.check_port(sock.getsockname()[1])


def test_startup_failure_cleans_owned_process_and_record(settings, monkeypatch):
    pidfile = settings / "started.pid"
    fake = settings / "fake-server"
    fake.write_text(f"#!{sys.executable}\nimport os, pathlib, time\npathlib.Path({str(pidfile)!r}).write_text(str(os.getpid()))\ntime.sleep(30)\n")
    fake.chmod(0o755)
    monkeypatch.setenv("LLAMA_SERVER", str(fake))
    port = available_port()
    with pytest.raises(RuntimeError, match="did not become healthy"):
        manager.start(port, "test")
    pid = int(pidfile.read_text())
    assert manager.identity(pid) is None
    assert not manager.state_path(port).exists()


def test_healthy_child_is_recorded_and_stopped(settings, monkeypatch):
    fake = settings / "fake-server"
    fake.write_text(f'''#!{sys.executable}
import http.server, sys
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{{}}')
    def log_message(self, *args): pass
port = int(sys.argv[sys.argv.index('--port') + 1])
http.server.HTTPServer(('127.0.0.1', port), Handler).serve_forever()
''')
    fake.chmod(0o755)
    monkeypatch.setenv("LLAMA_SERVER", str(fake))
    monkeypatch.setenv("START_TIMEOUT", "5")
    port = available_port()
    manager.start(port, "test")
    record = json.loads(manager.state_path(port).read_text())
    pid = record["pid"]
    try:
        assert record["identity"] == manager.identity(pid)
        args = record["args"]
        assert args[args.index("-ngl") + 1] == "0"
        assert args[args.index("--device") + 1] == "none"
        assert "--no-op-offload" in args
        assert args[args.index("--parallel") + 1] == "1"
        assert args[args.index("--threads-batch") + 1] == "4"
        manager.stop(port)
        assert not manager.state_path(port).exists()
    finally:
        # Reap the detached child in this test process (normal CLI starters exit).
        try:
            os.waitpid(pid, 0)
        except ChildProcessError:
            pass


def test_inspection_failure_keeps_record_and_never_signals(settings, monkeypatch):
    manager.RUN_DIR.mkdir()
    path = manager.state_path(19000)
    path.write_text(json.dumps({"pid": os.getpid(), "port": 19000,
                                "identity": {"started": "wrong", "command_sha256": "wrong"}}))
    def denied(*args, **kwargs):
        raise PermissionError("process inspection denied")
    monkeypatch.setattr(subprocess, "check_output", denied)
    with pytest.raises(RuntimeError, match="record retained"):
        manager.stop(19000)
    assert path.exists()
