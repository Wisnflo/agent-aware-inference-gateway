#!/usr/bin/env python3
"""Reproducible B0/B1 benchmark for the Week 1 gateway.

Protocol (kept identical to Phase 2.1 where it matters):

* two llama-server processes, same model/config, restarted before every run so
  the prompt cache is cold;
* requests are streamed ``/completion`` calls, ``n_predict=1``,
  ``temperature=0``, ``seed=42``, ``cache_prompt=true``;
* concurrency is 1 and there is no queue/admission in either strategy;
* the only difference between B0 and B1 is the gateway routing strategy.

B0 = round_robin, B1 = session_affinity.

Outputs, under ``--outdir`` (default ``bench/results/<run-id>``):
``meta.json``, ``raw.json``, ``summary.json``, ``overhead.json``, ``logs/``.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import signal
import socket
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx
from prometheus_client.parser import text_string_to_metric_families

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = Path(
    os.environ.get("GATEWAY_WORKSPACE", Path.home() / "PersonalHub" / "workspace")
)
LLAMA_SERVER = Path(
    os.environ.get("LLAMA_SERVER", WORKSPACE / "tools/llama.cpp/build/bin/llama-server")
)
MODEL = Path(
    os.environ.get("GATEWAY_MODEL", WORKSPACE / "models/qwen2.5-0.5b-instruct-q4_0.gguf")
)

sys.path.insert(0, str(PROJECT_ROOT / "src"))
from gateway.config import load_config  # noqa: E402
from gateway.sse import SSEEventDecoder, first_content  # noqa: E402

from bench.workload import SEED, WORKLOADS, WorkloadRequest, build_workload  # noqa: E402

STRATEGIES = {"b0": "round_robin", "b1": "session_affinity"}
BACKEND_ARGS = ["-c", "4096", "-n", "16", "-t", "4", "--metrics", "--no-webui"]


@dataclass
class ProcessHandle:
    name: str
    process: subprocess.Popen
    log: object

    async def stop(self) -> None:
        if self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                await asyncio.to_thread(self.process.wait, 8)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                await asyncio.to_thread(self.process.wait, 3)
        self.log.close()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def wait_http(url: str, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient(timeout=2) as client:
        while time.monotonic() < deadline:
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.25)
    raise RuntimeError(f"timeout waiting for {url}")


async def start_backend(config_backend, run_id: str, log_dir: Path) -> ProcessHandle:
    port = urlparse(config_backend.url).port
    if port is None:
        raise RuntimeError(f"backend url has no port: {config_backend.url}")
    if port_open(port):
        raise RuntimeError(
            f"port {port} is already in use; refusing to kill an unknown process"
        )
    log_path = log_dir / f"backend-{run_id}-{port}.log"
    log = open(log_path, "w")
    process = subprocess.Popen(
        [
            str(LLAMA_SERVER),
            "-m",
            str(MODEL),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            *BACKEND_ARGS,
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    handle = ProcessHandle(
        name=f"backend-{config_backend.id}", process=process, log=log
    )
    try:
        await wait_http(f"{config_backend.base_url}/health")
    except Exception:
        await handle.stop()
        raise
    return handle


async def start_backends(config, run_id: str, log_dir: Path) -> list[ProcessHandle]:
    handles = []
    try:
        for backend in config.backends:
            handles.append(await start_backend(backend, run_id, log_dir))
    except Exception:
        await stop_all(handles)
        raise
    return handles


async def start_gateway(
    config_path: Path,
    strategy: str,
    port: int,
    run_id: str,
    log_dir: Path,
    extra_args: list[str] | None = None,
) -> ProcessHandle:
    if port_open(port):
        raise RuntimeError(f"gateway port {port} is already in use")
    log = open(log_dir / f"gateway-{run_id}.log", "w")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "gateway",
            "--config",
            str(config_path),
            "--strategy",
            strategy,
            "--port",
            str(port),
            "--log-level",
            "warning",
            *(extra_args or []),
        ],
        cwd=str(PROJECT_ROOT),
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    handle = ProcessHandle(name=f"gateway-{run_id}", process=process, log=log)
    try:
        await wait_http(f"http://127.0.0.1:{port}/health", timeout=30)
    except Exception:
        await handle.stop()
        raise
    return handle


async def stop_all(handles: list[ProcessHandle]) -> None:
    for handle in reversed(handles):
        await handle.stop()


def parse_prometheus(text: str) -> dict[tuple[str, tuple], float]:
    values: dict[tuple[str, tuple], float] = {}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            values[(sample.name, tuple(sorted(sample.labels.items())))] = sample.value
    return values


def metric_sum(values: dict, name: str, **labels) -> float:
    total = 0.0
    for (sample_name, sample_labels), value in values.items():
        if sample_name != name:
            continue
        label_map = dict(sample_labels)
        if all(label_map.get(key) == expected for key, expected in labels.items()):
            total += value
    return total


async def fetch_text(client: httpx.AsyncClient, url: str) -> str:
    response = await client.get(url)
    response.raise_for_status()
    return response.text


BACKEND_COUNTERS = (
    "llamacpp:prompt_tokens_total",
    "llamacpp:prompt_tokens_cached_total",
    "llamacpp:tokens_predicted_total",
    "llamacpp:requests_processing",
    "llamacpp:requests_deferred",
)


async def backend_snapshot(client: httpx.AsyncClient, base_url: str) -> dict:
    text = await fetch_text(client, f"{base_url}/metrics")
    values: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) == 2 and parts[0] in BACKEND_COUNTERS:
            try:
                values[parts[0]] = float(parts[1])
            except ValueError:
                pass
    return values


async def run_request(
    client: httpx.AsyncClient,
    request: WorkloadRequest,
    run_started: float,
    n_predict: int,
) -> dict:
    body = {
        "prompt": request.prompt,
        "n_predict": n_predict,
        "temperature": 0,
        "seed": 42,
        "cache_prompt": True,
        "stream": True,
    }
    started = time.perf_counter()
    record = {
        "request_id": request.request_id,
        "session": request.session,
        "turn": request.turn,
        "start_offset_s": round(started - run_started, 6),
        "duration_s": None,
        "first_content_s": None,
        "backend": None,
        "status": None,
        "sse_events": 0,
        "cache_n": None,
        "prompt_n": None,
        "prompt_ms": None,
        "predicted_n": None,
        "predicted_ms": None,
        "success": False,
        "error": None,
    }
    try:
        async with client.stream(
            "POST",
            "/completion",
            json=body,
            headers={"X-Session-ID": request.session},
        ) as response:
            record["status"] = response.status_code
            record["backend"] = response.headers.get("x-gateway-backend")
            if response.status_code != 200:
                await response.aread()
                raise RuntimeError(f"HTTP {response.status_code}")
            decoder = SSEEventDecoder()
            async for chunk in response.aiter_raw():
                for event in decoder.feed(chunk):
                    record["sse_events"] += 1
                    obj = event.json()
                    if record["first_content_s"] is None and first_content(obj) is not None:
                        record["first_content_s"] = round(
                            time.perf_counter() - started, 6
                        )
                    if isinstance(obj, dict) and isinstance(obj.get("timings"), dict):
                        timings = obj["timings"]
                        record["cache_n"] = timings.get("cache_n")
                        record["prompt_n"] = timings.get("prompt_n")
                        record["prompt_ms"] = timings.get("prompt_ms")
                        record["predicted_n"] = timings.get("predicted_n")
                        record["predicted_ms"] = timings.get("predicted_ms")
            for event in decoder.flush():
                record["sse_events"] += 1
        record["success"] = record["cache_n"] is not None
        if not record["success"]:
            record["error"] = "no timings in stream"
    except Exception as exc:  # noqa: BLE001 - recorded as benchmark evidence
        record["error"] = f"{type(exc).__name__}: {exc}"
    record["duration_s"] = round(time.perf_counter() - started, 6)
    return record


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((len(ordered) - 1) * fraction)))
    return round(ordered[index], 6)


def summarize_requests(records: list[dict]) -> dict:
    successes = [record for record in records if record["success"]]
    cached = sum(record["cache_n"] for record in successes)
    fresh = sum(record["prompt_n"] for record in successes)
    prompt_ms = [r["prompt_ms"] for r in successes if r["prompt_ms"] is not None]
    first_ms = [
        1000.0 * r["first_content_s"]
        for r in successes
        if r["first_content_s"] is not None
    ]
    duration_ms = [1000.0 * r["duration_s"] for r in successes]
    distribution: dict[str, int] = {}
    for record in successes:
        distribution[record["backend"]] = distribution.get(record["backend"], 0) + 1
    return {
        "requests": len(records),
        "successes": len(successes),
        "errors": len(records) - len(successes),
        "error_rate": round((len(records) - len(successes)) / len(records), 6)
        if records
        else None,
        "cache_hit_pct": round(100.0 * cached / (cached + fresh), 3)
        if cached + fresh
        else None,
        "prompt_ms_p50": percentile(prompt_ms, 0.50),
        "prompt_ms_p95": percentile(prompt_ms, 0.95),
        "first_content_ms_p50": percentile(first_ms, 0.50),
        "first_content_ms_p95": percentile(first_ms, 0.95),
        "duration_ms_p50": percentile(duration_ms, 0.50),
        "duration_ms_p95": percentile(duration_ms, 0.95),
        "backend_counts": distribution,
    }


def build_summary(runs: list[dict]) -> dict:
    groups: dict[tuple[str, str], list[dict]] = {}
    for run in runs:
        groups.setdefault((run["workload"], run["strategy"]), []).append(run)

    summary = {}
    for (workload, strategy), group_runs in sorted(groups.items()):
        all_records = [record for run in group_runs for record in run["requests"]]
        base = summarize_requests(all_records)
        wall = sum(run["wall_s"] for run in group_runs)
        base["requests_per_s"] = (
            round(base["successes"] / wall, 4) if wall else None
        )
        overheads = [
            run["gateway_overhead_ms"]
            for run in group_runs
            if run.get("gateway_overhead_ms") is not None
        ]
        base["gateway_overhead_ms_mean"] = (
            round(statistics.mean(overheads), 3) if overheads else None
        )
        base["per_repeat"] = [
            {
                "repeat": run["repeat"],
                **{
                    key: summarize_requests(run["requests"])[key]
                    for key in ("cache_hit_pct", "first_content_ms_p50", "errors")
                },
                "backend_counts": summarize_requests(run["requests"])["backend_counts"],
            }
            for run in group_runs
        ]
        summary[f"{workload}/{strategy}"] = base
    return summary


async def run_matrix(args, config, outdir: Path) -> tuple[list[dict], dict]:
    log_dir = outdir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    runs: list[dict] = []
    gateway_port = args.gateway_port or free_port()
    workloads = [item.strip() for item in args.workloads.split(",") if item.strip()]
    strategies = [item.strip() for item in args.strategies.split(",") if item.strip()]

    for workload in workloads:
        if workload not in WORKLOADS:
            raise SystemExit(f"unknown workload: {workload}")
    for strategy in strategies:
        if strategy not in STRATEGIES:
            raise SystemExit(f"unknown strategy: {strategy}")

    for repeat in range(1, args.repeat + 1):
        for workload in workloads:
            requests = build_workload(workload)
            for strategy in strategies:
                run_id = f"r{repeat}-{workload}-{strategy}"
                print(f"[{now_iso()}] start {run_id}", flush=True)
                run = {
                    "run_id": run_id,
                    "repeat": repeat,
                    "workload": workload,
                    "strategy": strategy,
                    "gateway_strategy": STRATEGIES[strategy],
                    "started_at": now_iso(),
                    "requests": [],
                }
                handles: list[ProcessHandle] = []
                try:
                    handles = await start_backends(config, run_id, log_dir)
                    gateway = await start_gateway(
                        Path(args.config), STRATEGIES[strategy], gateway_port, run_id, log_dir
                    )
                    handles.append(gateway)
                    gateway_url = f"http://127.0.0.1:{gateway_port}"
                    async with httpx.AsyncClient(
                        base_url=gateway_url, timeout=300
                    ) as client:
                        before_backend = {
                            backend.id: await backend_snapshot(client, backend.base_url)
                            for backend in config.backends
                        }
                        before_gateway = parse_prometheus(
                            await fetch_text(client, f"{gateway_url}/metrics")
                        )
                        run_started = time.perf_counter()
                        for request in requests:
                            record = await run_request(
                                client, request, run_started, args.n_predict
                            )
                            run["requests"].append(record)
                        run["wall_s"] = round(time.perf_counter() - run_started, 6)
                        after_gateway = parse_prometheus(
                            await fetch_text(client, f"{gateway_url}/metrics")
                        )
                        after_backend = {
                            backend.id: await backend_snapshot(client, backend.base_url)
                            for backend in config.backends
                        }
                    run["backend_metrics_before"] = before_backend
                    run["backend_metrics_after"] = after_backend
                    run["gateway_metrics_before"] = {
                        _metric_key(key): value for key, value in before_gateway.items()
                    }
                    run["gateway_metrics_after"] = {
                        _metric_key(key): value for key, value in after_gateway.items()
                    }
                    request_sum = metric_sum(
                        after_gateway, "gateway_request_duration_seconds_sum"
                    ) - metric_sum(
                        before_gateway, "gateway_request_duration_seconds_sum"
                    )
                    upstream_sum = metric_sum(
                        after_gateway, "gateway_upstream_duration_seconds_sum"
                    ) - metric_sum(
                        before_gateway, "gateway_upstream_duration_seconds_sum"
                    )
                    count = metric_sum(
                        after_gateway, "gateway_request_duration_seconds_count"
                    ) - metric_sum(
                        before_gateway, "gateway_request_duration_seconds_count"
                    )
                    run["gateway_overhead_ms"] = (
                        round(1000.0 * (request_sum - upstream_sum) / count, 3)
                        if count
                        else None
                    )
                    run["gateway_metrics"] = {
                        "requests": metric_sum(
                            after_gateway, "gateway_requests_total"
                        ),
                        "errors": metric_sum(
                            after_gateway,
                            "gateway_upstream_errors_total",
                        ),
                        "ttft_count": metric_sum(
                            after_gateway, "gateway_ttft_seconds_count"
                        ),
                        "ttft_sum": metric_sum(
                            after_gateway, "gateway_ttft_seconds_sum"
                        ),
                    }
                finally:
                    await stop_all(handles)
                run["finished_at"] = now_iso()
                summary = summarize_requests(run["requests"])
                run["summary"] = summary
                runs.append(run)
                print(
                    f"[{now_iso()}] done {run_id} errors={summary['errors']} "
                    f"cache={summary['cache_hit_pct']}% "
                    f"first_p50={summary['first_content_ms_p50']}ms",
                    flush=True,
                )
                with open(outdir / "raw.json", "w") as handle:
                    json.dump({"runs": runs}, handle, indent=2)

    summary = build_summary(runs)
    with open(outdir / "summary.json", "w") as handle:
        json.dump(summary, handle, indent=2)
    return runs, summary


def _metric_key(key: tuple[str, tuple]) -> str:
    name, labels = key
    if not labels:
        return name
    rendered = ",".join(f"{k}={v}" for k, v in labels)
    return f"{name}{{{rendered}}}"


async def measure_overhead(args, config, outdir: Path) -> dict:
    """Direct backend vs gateway latency for identical warm non-stream calls."""
    log_dir = outdir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    run_id = "overhead"
    handles: list[ProcessHandle] = []
    gateway_port = args.gateway_port or free_port()
    body = {
        "prompt": "Overhead probe: " + ("The gateway forwards this fixed prompt. " * 60),
        "n_predict": 1,
        "temperature": 0,
        "seed": 42,
        "cache_prompt": True,
        "stream": False,
    }
    try:
        handles = await start_backends(config, run_id, log_dir)
        gateway = await start_gateway(
            Path(args.config), "session_affinity", gateway_port, run_id, log_dir
        )
        handles.append(gateway)
        gateway_url = f"http://127.0.0.1:{gateway_port}"
        gateway_target = None
        async with httpx.AsyncClient(timeout=120) as client:
            probe = await client.post(
                f"{gateway_url}/completion",
                json=body,
                headers={"X-Session-ID": "overhead-session"},
            )
            probe.raise_for_status()
            gateway_target = probe.headers["x-gateway-backend"]
            backend_url = {
                backend.id: backend.base_url for backend in config.backends
            }[gateway_target]

            for _ in range(3):
                await client.post(f"{backend_url}/completion", json=body)
                await client.post(
                    f"{gateway_url}/completion",
                    json=body,
                    headers={"X-Session-ID": "overhead-session"},
                )

            direct: list[float] = []
            proxied: list[float] = []
            for index in range(args.overhead_requests):
                if index % 2 == 0:
                    direct.append(await timed_post(client, f"{backend_url}/completion", body))
                    proxied.append(
                        await timed_post(
                            client,
                            f"{gateway_url}/completion",
                            body,
                            {"X-Session-ID": "overhead-session"},
                        )
                    )
                else:
                    proxied.append(
                        await timed_post(
                            client,
                            f"{gateway_url}/completion",
                            body,
                            {"X-Session-ID": "overhead-session"},
                        )
                    )
                    direct.append(await timed_post(client, f"{backend_url}/completion", body))
        result = {
            "requests": args.overhead_requests,
            "backend": gateway_target,
            "direct_ms_p50": percentile([1000 * value for value in direct], 0.50),
            "gateway_ms_p50": percentile([1000 * value for value in proxied], 0.50),
            "direct_ms_p95": percentile([1000 * value for value in direct], 0.95),
            "gateway_ms_p95": percentile([1000 * value for value in proxied], 0.95),
        }
        result["delta_ms_p50"] = round(
            (result["gateway_ms_p50"] or 0) - (result["direct_ms_p50"] or 0), 3
        )
        result["delta_ms_p95"] = round(
            (result["gateway_ms_p95"] or 0) - (result["direct_ms_p95"] or 0), 3
        )
        with open(outdir / "overhead.json", "w") as handle:
            json.dump(result, handle, indent=2)
        return result
    finally:
        await stop_all(handles)


async def timed_post(
    client: httpx.AsyncClient,
    url: str,
    body: dict,
    headers: dict | None = None,
) -> float:
    started = time.perf_counter()
    response = await client.post(url, json=body, headers=headers)
    response.raise_for_status()
    return time.perf_counter() - started


def write_meta(args, config, outdir: Path) -> None:
    meta = {
        "created_at": now_iso(),
        "host": platform.platform(),
        "python": sys.version,
        "llama_server": str(LLAMA_SERVER),
        "llama_cpp_commit": "785322d17f2d51e5185cf996a1f4e46d87640841",
        "model": str(MODEL),
        "model_sha256": "7671c0c304e6ce5a7fc577bcb12aba01e2c155cc2efd29b2213c95b18edaf6ed",
        "backend_args": BACKEND_ARGS,
        "backends": [asdict(backend) for backend in config.backends],
        "repeat": args.repeat,
        "n_predict": args.n_predict,
        "workloads": args.workloads,
        "strategies": args.strategies,
        "shuffle_seed": SEED,
        "concurrency": 1,
        "gateway_overhead_definition": (
            "gateway_request_duration_seconds minus gateway_upstream_duration_seconds, "
            "mean over all requests in a run"
        ),
    }
    with open(outdir / "meta.json", "w") as handle:
        json.dump(meta, handle, indent=2)


async def async_main(args) -> None:
    config = load_config(args.config)
    outdir = Path(args.outdir) if args.outdir else (
        PROJECT_ROOT / "bench/results" / datetime.now().strftime("%Y%m%d-%H%M%S")
    )
    outdir.mkdir(parents=True, exist_ok=True)
    write_meta(args, config, outdir)
    print(f"output: {outdir}", flush=True)

    runs, summary = await run_matrix(args, config, outdir)
    print(json.dumps(summary, indent=2), flush=True)
    if not args.skip_overhead:
        overhead = await measure_overhead(args, config, outdir)
        print("overhead: " + json.dumps(overhead), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Week 1 gateway B0/B1 benchmark")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config.example.toml"))
    parser.add_argument("--outdir", default=None)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--n-predict", type=int, default=1)
    parser.add_argument("--workloads", default="sequential,shuffled,negative")
    parser.add_argument("--strategies", default="b0,b1")
    parser.add_argument("--gateway-port", type=int, default=0)
    parser.add_argument("--overhead-requests", type=int, default=30)
    parser.add_argument("--skip-overhead", action="store_true")
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
