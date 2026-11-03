"""Fast-fail and owned-process cleanup checks without launching a model."""

import subprocess

import pytest

from tests.integration import test_real_backend as real


def test_backend_startup_reports_early_process_exit(monkeypatch, tmp_path):
    class ExitedProcess:
        returncode = 17

        def poll(self):
            return self.returncode

    monkeypatch.setattr(
        real.subprocess, "Popen", lambda *args, **kwargs: ExitedProcess()
    )

    def unexpected_probe(*args, **kwargs):
        raise AssertionError("an exited backend must not wait for health")

    monkeypatch.setattr(real.urllib.request, "urlopen", unexpected_probe)
    with pytest.raises(RuntimeError, match="exited with 17"):
        real.start_backend(12345, str(tmp_path / "server.log"))


def test_backend_cleanup_falls_back_to_owned_process_handle(monkeypatch):
    class Process:
        pid = 12345
        terminated = False
        killed = False
        waits = 0

        def poll(self):
            return None

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.killed = True

        def wait(self, timeout):
            self.waits += 1
            if self.waits == 1:
                raise subprocess.TimeoutExpired("test", timeout)
            return 0

    def denied_group_signal(*args):
        raise PermissionError("sandbox denies group signals")

    monkeypatch.setattr(real.os, "killpg", denied_group_signal)
    process = Process()
    real.stop_backend(process)
    assert process.terminated and process.killed and process.waits == 2
