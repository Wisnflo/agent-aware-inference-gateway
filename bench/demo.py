#!/usr/bin/env python3
"""Show session cache reuse and health-driven failover with two CPU backends."""
from __future__ import annotations

import argparse
import asyncio
import json
import platform
import signal
import time
from pathlib import Path

import httpx

from bench.run_bench import (
    BACKEND_ARGS, LLAMA_SERVER, MODEL, PROJECT_ROOT, now_iso, port_open,
    asset_metadata, run_request, start_backend, start_gateway, stop_all,
)
from bench.workload import WorkloadRequest
from gateway.config import load_config

PROMPT = (
    "You are a concise travel assistant. Respect the budget, account for travel "
    "time, and recommend accessible museums with indoor options. "
) * 12 + " Give one short suggestion."



async def run(args: argparse.Namespace) -> None:
    ports = (args.port_a, args.port_b, args.gateway_port)
    if len(set(ports)) != 3 or any(not 1 <= port <= 65535 for port in ports):
        raise RuntimeError("choose three distinct ports between 1 and 65535")
    for port in ports:
        if port_open(port):
            raise RuntimeError(f"port {port} is in use; choose unused ports")
    outdir = args.outdir.resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    summary_path = outdir / "summary.json"
    if summary_path.exists():
        raise RuntimeError(f"results already exist in {outdir}; choose a new --outdir")
    config_path = outdir / "config.toml"
    config_path.write_text(f'''[server]
host = "127.0.0.1"
port = {args.gateway_port}
strategy = "session_affinity"
health_interval_s = 0.25
upstream_connect_timeout_s = 1.0
[deadline]
request_timeout_ms = 120000
execution_timeout_ms = 90000
[health]
failure_threshold = 2
success_threshold = 2
[[backends]]
id = "a"
url = "http://127.0.0.1:{args.port_a}"
[[backends]]
id = "b"
url = "http://127.0.0.1:{args.port_b}"
''')
    config = load_config(config_path)
    handles = []
    summary = {
        "created_at": now_iso(), "status": "running", "platform": platform.platform(),
        "backend_args": BACKEND_ARGS, "ports": list(ports), "requests": [],
    }
    try:
        print("[1/5] Starting two llama.cpp CPU backends (4 threads, 1 slot each).", flush=True)
        summary.update(asset_metadata(LLAMA_SERVER, MODEL))
        # Append each handle immediately so cancellation always has an owner.
        for backend in config.backends:
            handles.append(await start_backend(backend, "demo", outdir / "logs"))
        handles.append(await start_gateway(
            config_path, "session_affinity", args.gateway_port, "demo", outdir / "logs"
        ))
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{args.gateway_port}", timeout=120,
        ) as client:
            async def request(turn: int) -> dict:
                result = await run_request(
                    client, WorkloadRequest("demo-session", turn, PROMPT, f"demo-{turn}"),
                    time.perf_counter(), 1,
                )
                summary["requests"].append(result)
                if not result["success"]:
                    raise RuntimeError(f"request {turn} failed: {result['error']}")
                print(
                    f"  Request {turn}: backend={result['backend']} cache_n={result['cache_n']} "
                    f"prompt_n={result['prompt_n']} complete={result['stream_completed']}", flush=True,
                )
                return result

            print("[2/5] Sending the same prompt twice with X-Session-ID: demo-session.", flush=True)
            first = await request(1)
            second = await request(2)
            if first["backend"] != second["backend"] or not second["cache_n"] > 0:
                raise RuntimeError("same-session backend stability or prompt cache reuse was not observed")
            summary["cache_reuse_observed"] = True
            preferred = first["backend"]
            print(f"[3/5] Stopping this demo's preferred backend {preferred}.", flush=True)
            target = next(handle for handle in handles if handle.name == f"backend-{preferred}")
            await target.stop()
            print("[4/5] Waiting for health probes to remove the stopped backend.", flush=True)
            started = time.monotonic()
            while time.monotonic() - started < 15:
                health = (await client.get("/health")).json()
                if health["backends"][preferred]["healthy"] is False:
                    break
                await asyncio.sleep(0.1)
            else:
                raise RuntimeError("stopped backend was not removed within 15 seconds")
            summary["health_after_removal"] = health
            summary["health_removal_s"] = round(time.monotonic() - started, 3)
            print(f"  backend={preferred} healthy=false after {summary['health_removal_s']}s", flush=True)
            print("[5/5] Sending the same session again to show failover.", flush=True)
            third = await request(3)
            if third["backend"] == preferred:
                raise RuntimeError("request did not move away from the stopped backend")
            summary["failover_observed"] = True
        summary["status"] = "passed"
    except BaseException as exc:
        summary["status"] = "failed"
        summary["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        try:
            await stop_all(handles)
        except BaseException as exc:
            summary["status"] = "failed"
            summary["cleanup_error"] = f"{type(exc).__name__}: {exc}"
            raise
        else:
            print("Cleaned up the processes started by this demo.", flush=True)
        finally:
            summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"PASS: cache reuse and health-driven failover observed. Results: {summary_path}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outdir", type=Path, default=PROJECT_ROOT / "bench/results/demo")
    parser.add_argument("--port-a", type=int, default=18090)
    parser.add_argument("--port-b", type=int, default=18091)
    parser.add_argument("--gateway-port", type=int, default=18080)
    args = parser.parse_args()

    async def entry() -> None:
        # asyncio.run handles Ctrl-C; give SIGTERM the same cleanup path.
        task = asyncio.current_task()
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
        await run(args)

    try:
        asyncio.run(entry())
    except (RuntimeError, OSError, httpx.HTTPError) as exc:
        parser.exit(1, f"Demo failed: {exc}\n")
    except (KeyboardInterrupt, asyncio.CancelledError):
        parser.exit(130, "Demo interrupted.\n")


if __name__ == "__main__":
    main()
