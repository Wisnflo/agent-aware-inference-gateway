#!/usr/bin/env python3
"""Start local llama.cpp processes and stop only a matching recorded identity."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
RUN_DIR = Path(os.environ.get("RUN_DIR", ROOT / ".run"))


def identity(pid: int) -> dict[str, str] | None:
    """Return None only after the kernel confirms exit; inspection errors fail closed."""
    def exists() -> bool:
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError as exc:
            raise RuntimeError(f"cannot inspect PID {pid}; process record retained") from exc

    if not exists():
        return None
    try:
        started = subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "lstart="], text=True, stderr=subprocess.DEVNULL,
        ).strip()
        command = subprocess.check_output(
            ["ps", "-ww", "-p", str(pid), "-o", "command="], text=True, stderr=subprocess.DEVNULL,
        ).strip()
        if not started or not command:
            raise RuntimeError("empty process inspection result")
        # Linux exposes a stronger, tick-resolution process creation identity.
        stat_path = Path(f"/proc/{pid}/stat")
        if stat_path.exists():
            started = stat_path.read_text().rsplit(")", 1)[1].split()[19]
        return {"started": started, "command_sha256": hashlib.sha256(command.encode()).hexdigest()}
    except (OSError, subprocess.CalledProcessError, RuntimeError) as exc:
        if not exists():
            return None
        raise RuntimeError(f"cannot verify identity of PID {pid}; process record retained") from exc


def check_port(port: int) -> None:
    if not 1 <= port <= 65535:
        raise RuntimeError(f"invalid port: {port}")
    with socket.socket() as sock:
        try:
            sock.bind(("127.0.0.1", port))
        except OSError as exc:
            raise RuntimeError(f"port {port} is in use; refusing to start") from exc


def state_path(port: int) -> Path:
    return RUN_DIR / f"backend-{port}.json"


def stop(port: int) -> None:
    path = state_path(port)
    if not path.exists():
        legacy = RUN_DIR / f"backend-{port}.pid"
        if legacy.exists():
            raise RuntimeError(f"{legacy}: PID-only record cannot prove ownership; no signal sent")
        print(f"No recorded backend on port {port}.")
        return
    state = json.loads(path.read_text())
    pid = state.get("pid")
    expected = state.get("identity")
    if not isinstance(pid, int) or pid <= 1 or not isinstance(expected, dict):
        raise RuntimeError(f"invalid process record {path}; no signal sent")
    current = identity(pid)
    if current is None:
        path.unlink()
        print(f"Backend on port {port} has already exited; removed its record.")
        return
    if current != expected or state.get("port") != port:
        raise RuntimeError(f"process identity changed for PID {pid}; no signal sent; inspect {path}")
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline and identity(pid) == expected:
        time.sleep(0.1)
    if identity(pid) == expected:
        os.kill(pid, signal.SIGKILL)
    path.unlink()
    print(f"Stopped backend port={port} pid={pid}.")


def start(port: int, backend_id: str) -> None:
    workspace = Path(os.environ.get("GATEWAY_WORKSPACE", Path.home() / "gateway-assets"))
    binary = Path(os.environ.get("LLAMA_SERVER", workspace / "llama.cpp/build/bin/llama-server"))
    model = Path(os.environ.get("GATEWAY_MODEL", workspace / "models/qwen2.5-0.5b-instruct-q4_0.gguf"))
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(f"llama-server not executable: {binary}")
    if not model.is_file():
        raise RuntimeError(f"model not found: {model}")
    check_port(port)
    RUN_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = state_path(port)
    if (RUN_DIR / f"backend-{port}.pid").exists():
        raise RuntimeError(f"legacy PID-only record exists for port {port}; inspect it before starting")
    # Reserve the record exclusively so another starter cannot overwrite ownership.
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    process = None
    log_dir = Path(os.environ.get("LOG_DIR", ROOT / "bench/logs"))
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"backend-{port}.log"
        threads = os.environ.get("THREADS", "4")
        args = [
            str(binary.resolve()), "-m", str(model.resolve()), "--host", "127.0.0.1",
            "--port", str(port), "-c", os.environ.get("CTX", "4096"),
            "-n", os.environ.get("N_PREDICT", "16"), "-t", threads,
            "--threads-batch", threads, "-ngl", "0", "--device", "none",
            "--no-op-offload", "--parallel", "1",
            "--metrics", "--no-webui",
        ]
        with log_path.open("w") as log:
            process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + float(os.environ.get("START_TIMEOUT", "120"))
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"backend {backend_id} exited with {process.returncode}; see {log_path}")
            try:
                with opener.open(f"http://127.0.0.1:{port}/health", timeout=1) as response:
                    if response.status == 200:
                        process_identity = identity(process.pid)
                        if process_identity is None:
                            raise RuntimeError("cannot verify backend process identity")
                        path.write_text(json.dumps({"pid": process.pid, "identity": process_identity,
                                                   "port": port, "args": args}, indent=2) + "\n")
                        print(f"Started backend {backend_id}: port={port} pid={process.pid} CPU threads={threads}; log={log_path}")
                        return
            except (urllib.error.URLError, TimeoutError):
                pass
            time.sleep(0.25)
        raise RuntimeError(f"backend {backend_id} did not become healthy; see {log_path}")
    except BaseException:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        path.unlink(missing_ok=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["start", "stop", "start-pair", "stop-pair"])
    parser.add_argument("port", type=int, nargs="?")
    parser.add_argument("backend_id", nargs="?")
    args = parser.parse_args()
    def terminate(signum, frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, terminate)
    try:
        if args.action.endswith("-pair"):
            ports = [int(os.environ.get("PORT_A", "18090")), int(os.environ.get("PORT_B", "18091"))]
            if len(set(ports)) != 2:
                raise RuntimeError("PORT_A and PORT_B must differ")
            if args.action == "start-pair":
                for port in ports:
                    check_port(port)
                    if state_path(port).exists():
                        raise RuntimeError(f"record already exists: {state_path(port)}")
                started = []
                try:
                    for port, name in zip(ports, ("a", "b")):
                        start(port, name)
                        started.append(port)
                except BaseException:
                    for port in reversed(started):
                        stop(port)
                    raise
            else:
                failures = []
                for port in ports:
                    try:
                        stop(port)
                    except Exception as exc:
                        failures.append(str(exc))
                if failures:
                    raise RuntimeError("; ".join(failures))
        else:
            if args.port is None:
                parser.error("PORT is required for start/stop")
            if args.action == "start":
                start(args.port, args.backend_id or str(args.port))
            else:
                stop(args.port)
    except (RuntimeError, OSError, ValueError) as exc:
        parser.exit(1, f"Error: {exc}\n")


if __name__ == "__main__":
    main()
