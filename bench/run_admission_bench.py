#!/usr/bin/env python3
"""Week 2 admission benchmark: fixed routing, variable concurrency/capacity.

Each scenario restarts both llama-server backends (cold prompt cache) and the
gateway.  Routing is always B1 session affinity; only admission settings and
client submission concurrency change.

The six scenarios are:

* ``baseline``   - admission disabled (Week 1 behavior)
* ``below``      - 2 client slots, max_in_flight 4
* ``near``       - 4 client slots, max_in_flight 4
* ``over``       - 16 client slots, max_in_flight 4, queue 8
* ``far_over``   - 24 client slots, max_in_flight 4, queue 2
* ``timeout``    - 16 client slots, max_in_flight 4, queue 8, timeout 20 ms

All metrics for completed requests exclude rejected/timed-out requests, and the
report states that explicitly.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import httpx
from prometheus_client.parser import text_string_to_metric_families

from bench.run_bench import (
    PROJECT_ROOT,
    ProcessHandle,
    fetch_text,
    free_port,
    metric_sum,
    now_iso,
    parse_prometheus,
    percentile,
    start_backends,
    start_gateway,
    stop_all,
)
from bench.workload import WorkloadRequest
from gateway.config import load_config
from gateway.sse import SSEEventDecoder, first_content


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    clients: int
    args: tuple[str, ...]


SCENARIOS = (
    Scenario(
        "baseline",
        "Week 1 baseline: admission disabled, 16 client slots",
        16,
        ("--no-admission",),
    ),
    Scenario(
        "below",
        "2 client slots, max_in_flight=4, queue=8",
        2,
        ("--max-in-flight", "4", "--max-queue-size", "8", "--queue-timeout-ms", "2000"),
    ),
    Scenario(
        "near",
        "4 client slots, max_in_flight=4, queue=8",
        4,
        ("--max-in-flight", "4", "--max-queue-size", "8", "--queue-timeout-ms", "2000"),
    ),
    Scenario(
        "over",
        "16 client slots, max_in_flight=4, queue=8",
        16,
        ("--max-in-flight", "4", "--max-queue-size", "8", "--queue-timeout-ms", "2000"),
    ),
    Scenario(
        "far_over",
        "24 client slots, max_in_flight=4, queue=2",
        24,
        ("--max-in-flight", "4", "--max-queue-size", "2", "--queue-timeout-ms", "2000"),
    ),
    Scenario(
        "timeout",
        "16 client slots, max_in_flight=4, queue=8, timeout=20ms",
        16,
        ("--max-in-flight", "4", "--max-queue-size", "8", "--queue-timeout-ms", "20"),
    ),
)

N_REQUESTS = 24


def build_requests() -> list[WorkloadRequest]:
    requests = []
    for index in range(N_REQUESTS):
        unique = (
            "Session %d has a unique long policy for region %d, budget tier %d, "
            "accessibility profile %d, and preferred activity mix %d. "
            % (index, index, index % 4, index % 2, index % 3)
        ) * 14
        requests.append(
            WorkloadRequest(
                session=f"admission-{index}",
                turn=0,
                prompt=unique + " Turn one: propose an outline.",
                request_id=f"adm-{index}",
            )
        )
    return requests


async def run_one(
    client: httpx.AsyncClient,
    request: WorkloadRequest,
    semaphore: asyncio.Semaphore,
    scenario_started: float,
) -> dict:
    async with semaphore:
        submitted = time.perf_counter()
        record = {
            "request_id": request.request_id,
            "session": request.session,
            "offered_at_s": round(submitted - scenario_started, 6),
            "duration_s": None,
            "status": None,
            "backend": None,
            "queue_ms": None,
            "first_content_s": None,
            "cache_n": None,
            "prompt_n": None,
            "error_type": None,
            "error": None,
            "success": False,
        }
        try:
            async with client.stream(
                "POST",
                "/completion",
                json={
                    "prompt": request.prompt,
                    "n_predict": 1,
                    "temperature": 0,
                    "seed": 42,
                    "cache_prompt": True,
                    "stream": True,
                },
                headers={"X-Session-ID": request.session},
            ) as response:
                record["status"] = response.status_code
                record["backend"] = response.headers.get("x-gateway-backend")
                queue_header = response.headers.get("x-gateway-queue-ms")
                if queue_header is not None:
                    record["queue_ms"] = float(queue_header)
                if response.status_code == 200:
                    decoder = SSEEventDecoder()
                    async for chunk in response.aiter_raw():
                        for event in decoder.feed(chunk):
                            obj = event.json()
                            if (
                                record["first_content_s"] is None
                                and first_content(obj) is not None
                            ):
                                record["first_content_s"] = round(
                                    time.perf_counter() - submitted, 6
                                )
                            if isinstance(obj, dict) and isinstance(
                                obj.get("timings"), dict
                            ):
                                timings = obj["timings"]
                                record["cache_n"] = timings.get("cache_n")
                                record["prompt_n"] = timings.get("prompt_n")
                    for event in decoder.flush():
                        obj = event.json()
                        if isinstance(obj, dict) and isinstance(obj.get("timings"), dict):
                            timings = obj["timings"]
                            record["cache_n"] = timings.get("cache_n")
                            record["prompt_n"] = timings.get("prompt_n")
                    record["success"] = record["cache_n"] is not None
                    if not record["success"]:
                        record["error"] = "no timings in stream"
                else:
                    raw = await response.aread()
                    try:
                        payload = json.loads(raw)
                        record["error_type"] = payload.get("error", {}).get("type")
                        record["error"] = payload.get("error", {}).get("message")
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        record["error"] = f"HTTP {response.status_code}"
        except Exception as exc:  # noqa: BLE001 - recorded as evidence
            record["error"] = f"{type(exc).__name__}: {exc}"
        record["duration_s"] = round(time.perf_counter() - submitted, 6)
        return record


def summarize(records: list[dict], wall_s: float, metrics: dict) -> dict:
    completed = [record for record in records if record["success"]]
    rejected = [
        record
        for record in records
        if record["status"] == 429 and record["error_type"] == "queue_full"
    ]
    timed_out = [
        record
        for record in records
        if record["status"] == 429 and record["error_type"] == "queue_timeout"
    ]
    errors = [
        record
        for record in records
        if not record["success"] and record not in rejected and record not in timed_out
    ]
    durations = [1000.0 * record["duration_s"] for record in completed]
    queue_waits = [
        record["queue_ms"] for record in completed if record["queue_ms"] is not None
    ]
    backend_counts: dict[str, int] = {}
    for record in completed:
        backend_counts[record["backend"]] = backend_counts.get(record["backend"], 0) + 1
    return {
        "offered": len(records),
        "admitted": sum(1 for record in records if record["status"] == 200),
        "completed": len(completed),
        "rejected": len(rejected),
        "timed_out": len(timed_out),
        "errors": len(errors),
        "goodput_req_s": round(len(completed) / wall_s, 4) if wall_s else None,
        "completed_duration_ms_p50": percentile(durations, 0.50),
        "completed_duration_ms_p95": percentile(durations, 0.95),
        "completed_first_content_ms_p50": percentile(
            [1000.0 * record["first_content_s"] for record in completed
             if record["first_content_s"] is not None],
            0.50,
        ),
        "queue_wait_ms_p50": percentile(queue_waits, 0.50),
        "queue_wait_ms_p95": percentile(queue_waits, 0.95),
        "backend_counts": backend_counts,
        "peak_in_flight": metric_sum(metrics, "gateway_admission_peak_in_flight"),
        "peak_queue_depth": metric_sum(metrics, "gateway_admission_peak_queue_depth"),
        "gateway_admitted_counter": metric_sum(
            metrics, "gateway_admission_admitted_total"
        ),
        "gateway_rejected_counter": metric_sum(
            metrics, "gateway_admission_rejected_total"
        ),
        "gateway_timed_out_counter": metric_sum(
            metrics, "gateway_admission_timed_out_total"
        ),
        "gateway_active_at_end": metric_sum(metrics, "gateway_active_requests"),
        "gateway_queue_at_end": metric_sum(metrics, "gateway_queue_depth"),
    }


def aggregate_runs(scenario_name: str, runs: list[dict]) -> dict:
    records = [record for run in runs for record in run["requests"]]
    completed = [record for record in records if record["success"]]
    rejected = [
        record
        for record in records
        if record["status"] == 429 and record["error_type"] == "queue_full"
    ]
    timed_out = [
        record
        for record in records
        if record["status"] == 429 and record["error_type"] == "queue_timeout"
    ]
    errors = [
        record
        for record in records
        if not record["success"] and record not in rejected and record not in timed_out
    ]
    durations = [1000.0 * record["duration_s"] for record in completed]
    queue_waits = [
        record["queue_ms"] for record in completed if record["queue_ms"] is not None
    ]
    backend_counts: dict[str, int] = {}
    for record in completed:
        backend_counts[record["backend"]] = backend_counts.get(record["backend"], 0) + 1
    total_wall = sum(run["wall_s"] for run in runs)
    return {
        "scenario": scenario_name,
        "repeats": len(runs),
        "offered": len(records),
        "admitted": sum(1 for record in records if record["status"] == 200),
        "completed": len(completed),
        "rejected": len(rejected),
        "timed_out": len(timed_out),
        "errors": len(errors),
        "goodput_req_s": round(len(completed) / total_wall, 4) if total_wall else None,
        "completed_duration_ms_p50": percentile(durations, 0.50),
        "completed_duration_ms_p95": percentile(durations, 0.95),
        "completed_first_content_ms_p50": percentile(
            [
                1000.0 * record["first_content_s"]
                for record in completed
                if record["first_content_s"] is not None
            ],
            0.50,
        ),
        "queue_wait_ms_p50": percentile(queue_waits, 0.50),
        "queue_wait_ms_p95": percentile(queue_waits, 0.95),
        "backend_counts": backend_counts,
        "peak_in_flight": max(
            (run["summary"]["peak_in_flight"] or 0 for run in runs), default=0
        ),
        "peak_queue_depth": max(
            (run["summary"]["peak_queue_depth"] or 0 for run in runs), default=0
        ),
        "gateway_admitted_counter": sum(
            run["summary"]["gateway_admitted_counter"] for run in runs
        ),
        "gateway_rejected_counter": sum(
            run["summary"]["gateway_rejected_counter"] for run in runs
        ),
        "gateway_timed_out_counter": sum(
            run["summary"]["gateway_timed_out_counter"] for run in runs
        ),
        "gateway_active_at_end": max(
            (run["summary"]["gateway_active_at_end"] for run in runs), default=0
        ),
        "gateway_queue_at_end": max(
            (run["summary"]["gateway_queue_at_end"] for run in runs), default=0
        ),
        "per_repeat": [
            {"repeat": run["repeat"], **run["summary"]} for run in runs
        ],
    }


async def run_scenario(
    scenario: Scenario, config, config_path: Path, outdir: Path
) -> dict:
    log_dir = outdir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    gateway_port = free_port()
    handles: list[ProcessHandle] = []
    requests = build_requests()
    try:
        handles = await start_backends(config, scenario.name, log_dir)
        gateway = await start_gateway(
            config_path,
            "session_affinity",
            gateway_port,
            scenario.name,
            log_dir,
            extra_args=list(scenario.args),
        )
        handles.append(gateway)
        gateway_url = f"http://127.0.0.1:{gateway_port}"
        semaphore = asyncio.Semaphore(scenario.clients)
        async with httpx.AsyncClient(base_url=gateway_url, timeout=120) as client:
            started = time.perf_counter()
            records = await asyncio.gather(
                *(run_one(client, request, semaphore, started) for request in requests)
            )
            wall_s = time.perf_counter() - started
            metrics_text = await fetch_text(client, f"{gateway_url}/metrics")
            metrics = parse_prometheus(metrics_text)
        summary = summarize(records, wall_s, metrics)
        return {
            "scenario": {
                "name": scenario.name,
                "description": scenario.description,
                "clients": scenario.clients,
                "args": list(scenario.args),
            },
            "started_at": now_iso(),
            "wall_s": round(wall_s, 6),
            "requests": records,
            "summary": summary,
        }
    finally:
        await stop_all(handles)


async def async_main(outdir: Path, repeats: int = 1) -> None:
    config_path = PROJECT_ROOT / "config.example.toml"
    config = load_config(config_path)
    outdir.mkdir(parents=True, exist_ok=True)
    meta = {
        "created_at": now_iso(),
        "workload": f"{N_REQUESTS} distinct long-prefix sessions, one turn each",
        "routing": "session_affinity",
        "scenarios": [
            {
                "name": scenario.name,
                "description": scenario.description,
                "clients": scenario.clients,
                "args": list(scenario.args),
            }
            for scenario in SCENARIOS
        ],
        "n_predict": 1,
        "temperature": 0,
        "seed": 42,
        "repeats": repeats,
    }
    with open(outdir / "meta.json", "w") as handle:
        json.dump(meta, handle, indent=2)

    runs = []
    for repeat in range(1, repeats + 1):
        for scenario in SCENARIOS:
            print(f"[{now_iso()}] start {scenario.name} repeat={repeat}", flush=True)
            run = await run_scenario(scenario, config, config_path, outdir)
            run["repeat"] = repeat
            runs.append(run)
            summary = run["summary"]
            print(
                f"[{now_iso()}] done {scenario.name} repeat={repeat} "
                f"offered={summary['offered']} completed={summary['completed']} "
                f"rejected={summary['rejected']} timeout={summary['timed_out']} "
                f"goodput={summary['goodput_req_s']}",
                flush=True,
            )
            with open(outdir / "raw.json", "w") as handle:
                json.dump({"runs": runs}, handle, indent=2)

    summary = {}
    for scenario in SCENARIOS:
        group = [run for run in runs if run["scenario"]["name"] == scenario.name]
        summary[scenario.name] = aggregate_runs(scenario.name, group)
    with open(outdir / "summary.json", "w") as handle:
        json.dump(summary, handle, indent=2)
    print(json.dumps(summary, indent=2), flush=True)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default=None)
    parser.add_argument("--repeat", type=int, default=1)
    args = parser.parse_args()
    outdir = (
        Path(args.outdir)
        if args.outdir
        else PROJECT_ROOT
        / "bench/results"
        / ("week2-admission-" + datetime.now().strftime("%Y%m%d-%H%M%S"))
    )
    asyncio.run(async_main(outdir, repeats=args.repeat))


if __name__ == "__main__":
    main()
