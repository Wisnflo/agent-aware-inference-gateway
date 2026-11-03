#!/usr/bin/env python3
"""Sustained-load and resource-stability experiment.

Phases: normal (below capacity), overload (beyond admission capacity),
client-disconnect storms, then idle.  Samples gateway/backend metrics, health
state and RSS every 0.5 s.

This is one run, not proof of absence of memory leaks.  It records trends,
peak bounds, final idle state and result-count conservation so a leak or
capacity violation would be visible.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from bench.run_bench import (
    PROJECT_ROOT,
    BACKEND_ARGS,
    asset_metadata,
    ProcessHandle,
    backend_snapshot,
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
from gateway.config import load_config
from gateway.sse import SSEEventDecoder
from bench.run_reliability_bench import stream_request

SHARED_PREFIX = (
    "You are a concise travel assistant. Respect the budget and keep answers "
    "short. "
) * 6


def rss_kb(pid: int) -> int | None:
    try:
        output = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(pid)], text=True
        ).strip()
        return int(output) if output else None
    except (subprocess.CalledProcessError, ValueError):
        return None


@dataclass
class WorkerStats:
    phase: str
    worker: int
    offered: int = 0
    completed: int = 0
    rejected: int = 0
    timed_out: int = 0
    cancelled: int = 0
    errors: int = 0
    error_types: dict[str, int] = field(default_factory=dict)
    durations: list[float] = field(default_factory=list)
    first_contents: list[float] = field(default_factory=list)

    def record(
        self, status: int | None, duration: float, first: float | None,
        *, completed: bool = False, intentional_cancel: bool = False,
        error: str | None = None,
    ):
        self.offered += 1
        if status == 200 and completed and error is None:
            self.completed += 1
            self.durations.append(duration)
            if first is not None:
                self.first_contents.append(first)
        elif status == 429:
            self.rejected += 1
        elif status == 504:
            self.timed_out += 1
        elif intentional_cancel and error is None:
            self.cancelled += 1
        else:
            self.errors += 1
            kind = error or ("incomplete_sse" if status == 200 else f"http_{status}")
            self.error_types[kind] = self.error_types.get(kind, 0) + 1


async def normal_worker(
    client: httpx.AsyncClient,
    worker_id: int,
    stop_at: float,
    stats: WorkerStats,
) -> None:
    index = 0
    while time.monotonic() < stop_at:
        prompt = (
            SHARED_PREFIX
            + f" Worker {worker_id} request {index}: give one short sentence."
        )
        result = await stream_request(
            client, prompt, session=f"soak-normal-{worker_id}-{index}"
        )
        stats.record(
            result.status, result.total_s, result.first_content_s,
            completed=result.completed, error=result.error,
        )
        index += 1


async def disconnect_worker(
    client: httpx.AsyncClient,
    worker_id: int,
    stop_at: float,
    stats: WorkerStats,
) -> None:
    index = 0
    while time.monotonic() < stop_at:
        prompt = SHARED_PREFIX + f" Disconnect worker {worker_id} request {index}."
        started = time.perf_counter()
        status = None
        intentional_cancel = False
        completed = False
        error = None
        try:
            async with client.stream(
                "POST", "/completion",
                json={"prompt": prompt, "n_predict": 32, "temperature": 0,
                      "stream": True, "cache_prompt": True},
                headers={"X-Session-ID": f"soak-cancel-{worker_id}-{index}"},
            ) as response:
                status = response.status_code
                decoder = SSEEventDecoder()
                async for chunk in response.aiter_raw():
                    events = decoder.feed(chunk)
                    if status == 200 and events:
                        completed = any(
                            isinstance(event.json(), dict)
                            and event.json().get("stop") is True for event in events
                        )
                        intentional_cancel = not completed
                        break
        except httpx.TimeoutException as exc:
            error = f"transport_timeout:{type(exc).__name__}"
        except Exception as exc:  # noqa: BLE001 - not an intentional cancellation
            error = f"transport_error:{type(exc).__name__}"
        stats.record(
            status, time.perf_counter() - started, None, completed=completed,
            intentional_cancel=intentional_cancel, error=error,
        )
        index += 1


async def sampler(
    client: httpx.AsyncClient,
    gateway_url: str,
    handles: list[ProcessHandle],
    phase_ref: dict,
    samples: list[dict],
    stop_event: asyncio.Event,
) -> None:
    last_health: dict[str, bool | None] = {}
    transitions = 0
    while not stop_event.is_set():
        try:
            metrics = parse_prometheus(await fetch_text(client, f"{gateway_url}/metrics"))
            health = (await client.get(f"{gateway_url}/health")).json()
            sample = {
                "t": round(time.monotonic(), 3),
                "phase": phase_ref["name"],
                "active": metric_sum(metrics, "gateway_active_requests"),
                "queue": metric_sum(metrics, "gateway_queue_depth"),
                "requests_total": metric_sum(metrics, "gateway_requests_total"),
                "admission_admitted": metric_sum(
                    metrics, "gateway_admission_admitted_total"
                ),
                "admission_rejected": metric_sum(
                    metrics, "gateway_admission_rejected_total"
                ),
                "admission_timed_out": metric_sum(
                    metrics, "gateway_admission_timed_out_total"
                ),
                "execution_timeouts": metric_sum(
                    metrics, "gateway_execution_timeouts_total"
                ),
                "client_cancelled": metric_sum(
                    metrics, "gateway_client_cancelled_total"
                ),
                "stream_terminations": metric_sum(
                    metrics, "gateway_stream_terminations_total"
                ),
                "backend_health": {
                    backend_id: item.get("healthy")
                    for backend_id, item in health.get("backends", {}).items()
                },
                "rss_kb": {
                    handle.name: rss_kb(handle.process.pid) for handle in handles
                },
            }
            for backend_id, healthy in sample["backend_health"].items():
                if backend_id in last_health and healthy != last_health[backend_id]:
                    transitions += 1
                last_health[backend_id] = healthy
            sample["health_transitions"] = transitions
            samples.append(sample)
        except Exception as exc:  # noqa: BLE001 - recorded in the sample
            samples.append(
                {
                    "t": round(time.monotonic(), 3),
                    "phase": phase_ref["name"],
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=0.5)
        except asyncio.TimeoutError:
            continue


async def run_phase(
    name: str,
    workers: int,
    duration_s: float,
    worker_fn,
    client: httpx.AsyncClient,
    phase_ref: dict,
) -> list[WorkerStats]:
    phase_ref["name"] = name
    stats = [WorkerStats(name, index) for index in range(workers)]
    stop_at = time.monotonic() + duration_s
    tasks = [
        asyncio.create_task(worker_fn(client, index, stop_at, stats[index]))
        for index in range(workers)
    ]
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    failures = [value for value in outcomes if isinstance(value, BaseException)]
    if failures:
        raise RuntimeError(f"{name} worker failed: {failures[0]!r}")
    return stats


def phase_summary(phase: str, stats: list[WorkerStats], wall_s: float) -> dict:
    offered = sum(item.offered for item in stats)
    completed = sum(item.completed for item in stats)
    durations = [duration for item in stats for duration in item.durations]
    firsts = [value for item in stats for value in item.first_contents]
    return {
        "phase": phase,
        "wall_s": round(wall_s, 3),
        "offered": offered,
        "completed": completed,
        "rejected": sum(item.rejected for item in stats),
        "timed_out": sum(item.timed_out for item in stats),
        "cancelled": sum(item.cancelled for item in stats),
        "errors": sum(item.errors for item in stats),
        "error_types": {
            key: sum(item.error_types.get(key, 0) for item in stats)
            for key in sorted({key for item in stats for key in item.error_types})
        },
        "goodput_req_s": round(completed / wall_s, 4) if wall_s else None,
        "duration_ms_p50": percentile([1000 * value for value in durations], 0.50),
        "duration_ms_p95": percentile([1000 * value for value in durations], 0.95),
        "first_content_ms_p50": percentile(
            [1000 * value for value in firsts], 0.50
        ),
    }


async def async_main(outdir: Path) -> None:
    config_path = PROJECT_ROOT / "config.reliability.toml"
    config = load_config(config_path)
    outdir.mkdir(parents=True, exist_ok=True)
    log_dir = outdir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    gateway_port = free_port()
    handles: list[ProcessHandle] = []
    phase_ref = {"name": "startup"}
    samples: list[dict] = []
    stop_event = asyncio.Event()
    try:
        handles = await start_backends(config, "soak", log_dir)
        gateway = await start_gateway(
            config_path,
            "session_affinity",
            gateway_port,
            "soak",
            log_dir,
        )
        handles.append(gateway)
        gateway_url = f"http://127.0.0.1:{gateway_port}"
        async with httpx.AsyncClient(base_url=gateway_url, timeout=120) as client:
            sampler_task = asyncio.create_task(
                sampler(client, gateway_url, handles, phase_ref, samples, stop_event)
            )
            phase_results = []
            for name, workers, duration, worker_fn in (
                ("normal", 4, 15.0, normal_worker),
                ("overload", 40, 15.0, normal_worker),
                ("disconnect", 4, 10.0, disconnect_worker),
            ):
                started = time.monotonic()
                stats = await run_phase(
                    name, workers, duration, worker_fn, client, phase_ref
                )
                phase_results.append(
                    phase_summary(name, stats, time.monotonic() - started)
                )

            phase_ref["name"] = "idle"
            started = time.monotonic()
            await asyncio.sleep(5.0)
            idle_wall = time.monotonic() - started
            stop_event.set()
            await sampler_task

            metrics = parse_prometheus(await fetch_text(client, f"{gateway_url}/metrics"))
            health = (await client.get(f"{gateway_url}/health")).json()
            final = {
                "active": metric_sum(metrics, "gateway_active_requests"),
                "queue": metric_sum(metrics, "gateway_queue_depth"),
                "requests_total": metric_sum(metrics, "gateway_requests_total"),
                "admission_admitted": metric_sum(
                    metrics, "gateway_admission_admitted_total"
                ),
                "admission_rejected": metric_sum(
                    metrics, "gateway_admission_rejected_total"
                ),
                "admission_timed_out": metric_sum(
                    metrics, "gateway_admission_timed_out_total"
                ),
                "execution_timeouts": metric_sum(
                    metrics, "gateway_execution_timeouts_total"
                ),
                "client_cancelled": metric_sum(
                    metrics, "gateway_client_cancelled_total"
                ),
                "stream_terminations": metric_sum(
                    metrics, "gateway_stream_terminations_total"
                ),
                "backend_health": {
                    backend_id: item.get("healthy")
                    for backend_id, item in health.get("backends", {}).items()
                },
            }

        rss_series = {
            handle.name: [
                sample["rss_kb"].get(handle.name)
                for sample in samples
                if sample.get("rss_kb", {}).get(handle.name) is not None
            ]
            for handle in handles
        }
        rss_summary = {}
        for name, values in rss_series.items():
            if not values:
                continue
            midpoint = max(1, len(values) // 2)
            rss_summary[name] = {
                "start_kb": values[0],
                "end_kb": values[-1],
                "min_kb": min(values),
                "max_kb": max(values),
                "first_half_mean_kb": round(sum(values[:midpoint]) / midpoint, 1),
                "second_half_mean_kb": round(
                    sum(values[midpoint:]) / max(1, len(values) - midpoint), 1
                ),
            }

        total_outcomes = {
            key: sum(result[key] for result in phase_results)
            for key in ("offered", "completed", "rejected", "timed_out", "cancelled", "errors")
        }
        first_requests = next(
            (sample.get("requests_total") for sample in samples if "requests_total" in sample),
            0,
        )
        gateway_delta = final["requests_total"] - (first_requests or 0)
        summary = {
            "created_at": now_iso(),
            "assets": asset_metadata(),
            "backend_args": BACKEND_ARGS,
            "config": str(config_path),
            "phases": phase_results,
            "idle_wall_s": round(idle_wall, 3),
            "final": final,
            "rss": rss_summary,
            "samples": len(samples),
            "peak_active_sampled": max(
                (sample.get("active") or 0 for sample in samples), default=0
            ),
            "peak_queue_sampled": max(
                (sample.get("queue") or 0 for sample in samples), default=0
            ),
            "configured_max_in_flight": config.admission.max_in_flight,
            "configured_max_queue_size": config.admission.max_queue_size,
            "health_transitions": samples[-1].get("health_transitions") if samples else None,
            "client_outcomes": total_outcomes,
            "result_conservation": {
                "client_offered": total_outcomes["offered"],
                "client_outcome_sum": (
                    total_outcomes["completed"]
                    + total_outcomes["rejected"]
                    + total_outcomes["timed_out"]
                    + total_outcomes["cancelled"]
                    + total_outcomes["errors"]
                ),
                "gateway_requests_delta": gateway_delta,
                "offered_minus_gateway_delta": total_outcomes["offered"]
                - gateway_delta,
            },
            "notes": [
                "One sustained run; absence of a visible leak is not proof.",
                "RSS is process resident memory sampled every 0.5 s.",
                "Disconnect phase closes the client after the first SSE event.",
            ],
        }
        with open(outdir / "summary.json", "w") as handle:
            json.dump(summary, handle, indent=2)
        with open(outdir / "timeseries.json", "w") as handle:
            json.dump({"samples": samples}, handle, indent=2)
        print(json.dumps(summary, indent=2))
    finally:
        stop_event.set()
        await stop_all(handles)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default=None)
    args = parser.parse_args()
    outdir = (
        Path(args.outdir)
        if args.outdir
        else PROJECT_ROOT / "bench/results/week4-soak"
    )
    asyncio.run(async_main(outdir))


if __name__ == "__main__":
    main()
