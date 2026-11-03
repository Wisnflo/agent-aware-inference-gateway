#!/usr/bin/env python3
"""Week 3 real-backend reliability experiments.

Scenarios:

1. normal traffic through the gateway (SSE);
2. an artificially short execution deadline;
3. stopping backend A, observing health removal, routing to B, restarting A,
   observing recovery and immediate failback;
4. disconnecting a real streaming client and observing gateway capacity plus
   backend processing state.

Raw request records, gateway/backend metrics snapshots and logs are written
under the out directory.  Behaviors that cannot be proven with the real
llama-server are explicitly marked as unverified in the summary.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
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
    start_backend,
    start_backends,
    start_gateway,
    stop_all,
)
from gateway.backends import Backend
from gateway.config import load_config
from gateway.router import SessionAffinityRouter
from gateway.sse import SSEEventDecoder


LONG_PREFIX = (
    "You are a careful travel planning assistant. Respect the budget, avoid "
    "impossible transfers, keep daily activity time bounded, and explain every "
    "factual claim with a tool result. "
) * 10

CONFIG_PATH: Path | None = None


def body(prompt: str, *, stream: bool = False, n_predict: int = 1) -> dict:
    return {
        "prompt": prompt,
        "n_predict": n_predict,
        "temperature": 0,
        "seed": 42,
        "cache_prompt": True,
        "stream": stream,
    }


async def wait_condition(check, timeout: float, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = await check()
        if result:
            return True
        await asyncio.sleep(interval)
    return False


async def gateway_health(client: httpx.AsyncClient, gateway: str) -> dict:
    response = await client.get(f"{gateway}/health")
    return response.json()


async def gateway_metric(client: httpx.AsyncClient, gateway: str, name: str, **labels):
    metrics = parse_prometheus(await fetch_text(client, f"{gateway}/metrics"))
    return metric_sum(metrics, name, **labels)


@dataclass
class StreamResult:
    status: int
    backend: str | None
    first_content_s: float | None
    total_s: float
    events: int
    raw: bytes
    queue_ms: float | None
    completed: bool = False
    error: str | None = None


async def stream_request(
    client: httpx.AsyncClient, prompt: str, *, n_predict: int = 1,
    session: str | None = None,
) -> StreamResult:
    """Measure native llama.cpp SSE; only a framed ``stop=true`` completes it."""
    started = time.perf_counter()
    first = None
    events = 0
    raw_bytes = b""
    backend = None
    queue_ms = None
    status = 0
    completed = False
    error = None
    try:
        async with client.stream(
            "POST", "/completion", json=body(prompt, stream=True, n_predict=n_predict),
            headers={"X-Session-ID": session} if session else None,
        ) as response:
            status = response.status_code
            backend = response.headers.get("x-gateway-backend")
            header_queue = response.headers.get("x-gateway-queue-ms")
            if header_queue is not None:
                queue_ms = float(header_queue)
            decoder = SSEEventDecoder()
            async for chunk in response.aiter_raw():
                raw_bytes += chunk
                for event in decoder.feed(chunk):
                    events += 1
                    obj = event.json()
                    if isinstance(obj, dict):
                        if obj.get("stop") is True:
                            completed = True
                        if first is None and obj.get("content"):
                            first = time.perf_counter() - started
            # An unterminated trailing event is not proof of protocol completion.
            if status != 200:
                error = f"http_{status}"
            elif not completed:
                error = "incomplete_sse"
    except httpx.TimeoutException as exc:
        error = f"transport_timeout:{type(exc).__name__}"
    except Exception as exc:  # noqa: BLE001 - persist a failed measurement
        error = f"transport_error:{type(exc).__name__}"
    return StreamResult(
        status=status, backend=backend, first_content_s=first,
        total_s=time.perf_counter() - started, events=events,
        raw=raw_bytes, queue_ms=queue_ms,
        completed=status == 200 and completed and error is None, error=error,
    )


async def scenario_normal(config, gateway_port: int, log_dir: Path) -> dict:
    handles = []
    try:
        handles = await start_backends(config, "reliability-normal", log_dir)
        gateway = await start_gateway(
            CONFIG_PATH,
            "session_affinity",
            gateway_port,
            "reliability-normal",
            log_dir,
        )
        handles.append(gateway)
        gateway_url = f"http://127.0.0.1:{gateway_port}"
        records = []
        async with httpx.AsyncClient(base_url=gateway_url, timeout=120) as client:
            for index in range(12):
                prompt = LONG_PREFIX + f" Unique normal request {index}: outline a trip."
                result = await stream_request(client, prompt)
                records.append(
                    {
                        "index": index,
                        "status": result.status,
                        "backend": result.backend,
                        "first_content_s": result.first_content_s,
                        "total_s": result.total_s,
                        "events": result.events,
                        "queue_ms": result.queue_ms,
                        "completed": result.completed,
                        "error": result.error,
                    }
                )
            metrics = parse_prometheus(await fetch_text(client, f"{gateway_url}/metrics"))
        durations = [1000.0 * record["total_s"] for record in records if record.get("completed") is True]
        firsts = [
            1000.0 * record["first_content_s"]
            for record in records
            if record.get("completed") is True and record["first_content_s"] is not None
        ]
        return {
            "scenario": "normal",
            "requests": records,
            "summary": {
                "offered": len(records),
                "completed": sum(1 for record in records if record.get("completed") is True),
                "errors": sum(1 for record in records if record.get("completed") is not True),
                "duration_ms_p50": percentile(durations, 0.50),
                "duration_ms_p95": percentile(durations, 0.95),
                "first_content_ms_p50": percentile(firsts, 0.50),
                "active_at_end": metric_sum(metrics, "gateway_active_requests"),
                "queue_at_end": metric_sum(metrics, "gateway_queue_depth"),
            },
        }
    finally:
        await stop_all(handles)


async def scenario_deadline(config, gateway_port: int, log_dir: Path) -> dict:
    handles = []
    try:
        handles = await start_backends(config, "reliability-deadline", log_dir)
        gateway = await start_gateway(
            CONFIG_PATH,
            "session_affinity",
            gateway_port,
            "reliability-deadline",
            log_dir,
            extra_args=["--execution-timeout-ms", "50", "--request-timeout-ms", "2000"],
        )
        handles.append(gateway)
        gateway_url = f"http://127.0.0.1:{gateway_port}"
        records = []
        async with httpx.AsyncClient(base_url=gateway_url, timeout=120) as client:
            for index in range(5):
                prompt = LONG_PREFIX + f" Deadline request {index}: write a long answer."
                started = time.perf_counter()
                response = await client.post(
                    "/completion", json=body(prompt, n_predict=32)
                )
                records.append(
                    {
                        "index": index,
                        "status": response.status_code,
                        "duration_s": round(time.perf_counter() - started, 6),
                        "body": response.text[:200],
                    }
                )
            await asyncio.sleep(1.0)
            processing = {}
            for backend in config.backends:
                snapshot = await backend_snapshot(client, backend.base_url)
                processing[backend.id] = snapshot.get(
                    "llamacpp:requests_processing"
                )
            metrics = parse_prometheus(await fetch_text(client, f"{gateway_url}/metrics"))
        return {
            "scenario": "deadline",
            "requests": records,
            "summary": {
                "offered": len(records),
                "timeout_504": sum(1 for record in records if record["status"] == 504),
                "completed_200": sum(1 for record in records if record["status"] == 200),
                "other_status": sum(
                    1 for record in records if record["status"] not in (200, 504)
                ),
                "backend_requests_processing_after": processing,
                "timeouts_metric": metric_sum(
                    metrics, "gateway_execution_timeouts_total"
                ),
                "active_at_end": metric_sum(metrics, "gateway_active_requests"),
                "queue_at_end": metric_sum(metrics, "gateway_queue_depth"),
            },
        }
    finally:
        await stop_all(handles)


async def scenario_failover_recovery(config, gateway_port: int, log_dir: Path) -> dict:
    handles = []
    restarted_a = None
    try:
        handles = await start_backends(config, "reliability-failover", log_dir)
        gateway = await start_gateway(
            CONFIG_PATH,
            "session_affinity",
            gateway_port,
            "reliability-failover",
            log_dir,
        )
        handles.append(gateway)
        gateway_url = f"http://127.0.0.1:{gateway_port}"

        backend_objects = [
            Backend("a", config.backends[0].base_url),
            Backend("b", config.backends[1].base_url),
        ]
        router = SessionAffinityRouter(backend_objects)
        sessions = [f"failover-session-{index}" for index in range(20)]
        mapping = router.mapping_for(backend_objects, sessions)
        session_a = next(session for session in sessions if mapping[session] == "a")
        session_b = next(session for session in sessions if mapping[session] == "b")

        async with httpx.AsyncClient(base_url=gateway_url, timeout=120) as client:
            first_on_a = (await client.post(
                "/completion",
                json=body(LONG_PREFIX + " session A initial", n_predict=1),
                headers={"X-Session-ID": session_a},
            )).headers.get("x-gateway-backend")

            stop_started = time.monotonic()
            await handles[0].stop()  # backend a
            removed = await wait_condition(
                lambda: _backend_unhealthy(client, gateway_url, "a"),
                timeout=15,
            )
            removed_s = time.monotonic() - stop_started

            failover_started = time.monotonic()
            failover_response = await client.post(
                "/completion",
                json=body(LONG_PREFIX + " after failure", n_predict=1),
                headers={"X-Session-ID": session_a},
            )
            failover_s = time.monotonic() - failover_started
            failover_backend = failover_response.headers.get("x-gateway-backend")

            restart_started = time.monotonic()
            restarted_a = await start_backend(
                config.backends[0], "recovery-a", log_dir
            )
            recovered = await wait_condition(
                lambda: _backend_recovered(client, gateway_url, "a"),
                timeout=15,
            )
            recovered_s = time.monotonic() - restart_started

            assert first_on_a == "a", first_on_a
            failback_response = await client.post(
                "/completion",
                json=body(LONG_PREFIX + " after recovery", n_predict=1),
                headers={"X-Session-ID": session_a},
            )
            failback_backend = failback_response.headers.get("x-gateway-backend")
            metrics = parse_prometheus(await fetch_text(client, f"{gateway_url}/metrics"))

        return {
            "scenario": "failover_recovery",
            "session_a": session_a,
            "session_b": session_b,
            "initial_backend_for_session_a": first_on_a,
            "summary": {
                "removed": removed,
                "removal_s": round(removed_s, 3),
                "failover_status": failover_response.status_code,
                "failover_backend": failover_backend,
                "failover_s": round(failover_s, 3),
                "recovered": recovered,
                "recovery_s": round(recovered_s, 3),
                "failback_status": failback_response.status_code,
                "failback_backend": failback_backend,
                "routing_failovers": metric_sum(
                    metrics, "gateway_routing_failovers_total"
                ),
                "backend_removed": metric_sum(metrics, "gateway_backend_removed_total"),
                "backend_restored": metric_sum(metrics, "gateway_backend_restored_total"),
                "active_at_end": metric_sum(metrics, "gateway_active_requests"),
            },
        }
    finally:
        if restarted_a is not None:
            await restarted_a.stop()
        await stop_all(handles)


async def _backend_unhealthy(client, gateway_url: str, backend_id: str) -> bool:
    payload = await gateway_health(client, gateway_url)
    return payload["backends"][backend_id]["healthy"] is False


async def _backend_recovered(client, gateway_url: str, backend_id: str) -> bool:
    payload = await gateway_health(client, gateway_url)
    return payload["backends"][backend_id]["healthy"] is True


async def scenario_cancel(config, gateway_port: int, log_dir: Path) -> dict:
    handles = []
    try:
        handles = await start_backends(config, "reliability-cancel", log_dir)
        gateway = await start_gateway(
            CONFIG_PATH,
            "session_affinity",
            gateway_port,
            "reliability-cancel",
            log_dir,
        )
        handles.append(gateway)
        gateway_url = f"http://127.0.0.1:{gateway_port}"
        async with httpx.AsyncClient(base_url=gateway_url, timeout=120) as client:
            first_event = asyncio.Event()
            bytes_read = 0
            close_started = None
            async with client.stream(
                "POST",
                "/completion",
                json=body(
                    LONG_PREFIX + " Cancel this long stream.",
                    stream=True,
                    n_predict=64,
                ),
            ) as response:
                async for chunk in response.aiter_raw():
                    bytes_read += len(chunk)
                    if b"\n\n" in chunk:
                        first_event.set()
                        break
            close_started = time.monotonic()
            await wait_condition(
                lambda: _gateway_idle(client, gateway_url), timeout=10
            )
            released_s = time.monotonic() - close_started
            backend_idle = {}
            for backend in config.backends:
                idle = await wait_condition(
                    lambda backend=backend: _backend_idle(client, backend.base_url),
                    timeout=10,
                )
                snapshot = await backend_snapshot(client, backend.base_url)
                backend_idle[backend.id] = {
                    "idle_observed": idle,
                    "requests_processing": snapshot.get(
                        "llamacpp:requests_processing"
                    ),
                }
            metrics = parse_prometheus(await fetch_text(client, f"{gateway_url}/metrics"))
        return {
            "scenario": "cancel_stream",
            "summary": {
                "first_event_received": first_event.is_set(),
                "bytes_before_close": bytes_read,
                "gateway_released_s": round(released_s, 3),
                "client_cancelled": metric_sum(
                    metrics, "gateway_client_cancelled_total"
                ),
                "stream_terminations": metric_sum(
                    metrics, "gateway_stream_terminations_total"
                ),
                "backend_idle": backend_idle,
                "active_at_end": metric_sum(metrics, "gateway_active_requests"),
                "backend_cancel_confirmed": all(
                    item["idle_observed"] for item in backend_idle.values()
                ),
            },
        }
    finally:
        await stop_all(handles)


async def _gateway_idle(client, gateway_url: str) -> bool:
    return await gateway_metric(client, gateway_url, "gateway_active_requests") == 0


async def _backend_idle(client, backend_url: str) -> bool:
    snapshot = await backend_snapshot(client, backend_url)
    return snapshot.get("llamacpp:requests_processing") == 0


def aggregate_reliability(results: list[dict]) -> dict:
    summary: dict[str, dict] = {}

    normal = [result for result in results if result["scenario"] == "normal"]
    records = [record for result in normal for record in result["requests"]]
    completed = [record for record in records if record.get("completed") is True]
    durations = [1000.0 * record["total_s"] for record in completed]
    firsts = [
        1000.0 * record["first_content_s"]
        for record in completed
        if record["first_content_s"] is not None
    ]
    summary["normal"] = {
        "repeats": len(normal),
        "offered": len(records),
        "completed": len(completed),
        "errors": len(records) - len(completed),
        "duration_ms_p50": percentile(durations, 0.50),
        "duration_ms_p95": percentile(durations, 0.95),
        "first_content_ms_p50": percentile(firsts, 0.50),
        "active_at_end": max(
            (result["summary"]["active_at_end"] for result in normal), default=0
        ),
        "queue_at_end": max(
            (result["summary"]["queue_at_end"] for result in normal), default=0
        ),
        "per_repeat": [result["summary"] for result in normal],
    }

    deadline = [result for result in results if result["scenario"] == "deadline"]
    summary["deadline"] = {
        "repeats": len(deadline),
        "offered": sum(result["summary"]["offered"] for result in deadline),
        "timeout_504": sum(
            result["summary"]["timeout_504"] for result in deadline
        ),
        "completed_200": sum(
            result["summary"]["completed_200"] for result in deadline
        ),
        "other_status": sum(
            result["summary"]["other_status"] for result in deadline
        ),
        "backend_requests_processing_after": {
            backend: max(
                (result["summary"]["backend_requests_processing_after"].get(backend, 0)
                 or 0 for result in deadline), default=0,
            )
            for backend in ("a", "b")
        },
        "timeouts_metric": sum(
            result["summary"]["timeouts_metric"] for result in deadline
        ),
        "active_at_end": max(
            (result["summary"]["active_at_end"] for result in deadline), default=0
        ),
        "queue_at_end": max(
            (result["summary"]["queue_at_end"] for result in deadline), default=0
        ),
        "per_repeat": [result["summary"] for result in deadline],
    }

    failover = [
        result for result in results if result["scenario"] == "failover_recovery"
    ]
    removals = [result["summary"]["removal_s"] for result in failover]
    failovers = [result["summary"]["failover_s"] for result in failover]
    recoveries = [result["summary"]["recovery_s"] for result in failover]
    summary["failover_recovery"] = {
        "repeats": len(failover),
        "removed_all": all(result["summary"]["removed"] for result in failover),
        "recovered_all": all(result["summary"]["recovered"] for result in failover),
        "removal_s_p50": percentile(removals, 0.50),
        "removal_s_min": min(removals) if removals else None,
        "removal_s_max": max(removals) if removals else None,
        "failover_s_p50": percentile(failovers, 0.50),
        "failover_s_min": min(failovers) if failovers else None,
        "failover_s_max": max(failovers) if failovers else None,
        "recovery_s_p50": percentile(recoveries, 0.50),
        "recovery_s_min": min(recoveries) if recoveries else None,
        "recovery_s_max": max(recoveries) if recoveries else None,
        "failover_statuses": sorted(
            {result["summary"]["failover_status"] for result in failover}
        ),
        "failover_backends": sorted(
            {result["summary"]["failover_backend"] for result in failover}
        ),
        "failback_statuses": sorted(
            {result["summary"]["failback_status"] for result in failover}
        ),
        "failback_backends": sorted(
            {result["summary"]["failback_backend"] for result in failover}
        ),
        "routing_failovers": sum(
            result["summary"]["routing_failovers"] for result in failover
        ),
        "backend_removed": sum(
            result["summary"]["backend_removed"] for result in failover
        ),
        "backend_restored": sum(
            result["summary"]["backend_restored"] for result in failover
        ),
        "active_at_end": max(
            (result["summary"]["active_at_end"] for result in failover), default=0
        ),
        "per_repeat": [result["summary"] for result in failover],
    }

    cancel = [result for result in results if result["scenario"] == "cancel_stream"]
    releases = [result["summary"]["gateway_released_s"] for result in cancel]
    bytes_read = [result["summary"]["bytes_before_close"] for result in cancel]
    summary["cancel_stream"] = {
        "repeats": len(cancel),
        "first_event_received_all": all(
            result["summary"]["first_event_received"] for result in cancel
        ),
        "gateway_released_s_p50": percentile(releases, 0.50),
        "gateway_released_s_min": min(releases) if releases else None,
        "gateway_released_s_max": max(releases) if releases else None,
        "bytes_before_close_p50": percentile(bytes_read, 0.50),
        "client_cancelled": sum(
            result["summary"]["client_cancelled"] for result in cancel
        ),
        "stream_terminations": sum(
            result["summary"]["stream_terminations"] for result in cancel
        ),
        "backend_cancel_confirmed_all": all(
            result["summary"]["backend_cancel_confirmed"] for result in cancel
        ),
        "active_at_end": max(
            (result["summary"]["active_at_end"] for result in cancel), default=0
        ),
        "per_repeat": [result["summary"] for result in cancel],
    }
    return summary


async def async_main(outdir: Path, repeats: int = 1) -> None:
    config_path = PROJECT_ROOT / "config.reliability.toml"
    config = load_config(config_path)
    global CONFIG_PATH
    CONFIG_PATH = config_path
    outdir.mkdir(parents=True, exist_ok=True)
    log_dir = outdir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    gateway_port = free_port()
    meta = {
        "assets": asset_metadata(),
        "backend_args": BACKEND_ARGS,
        "created_at": now_iso(),
        "config": str(config_path),
        "gateway_port": gateway_port,
        "scenarios": ["normal", "deadline", "failover_recovery", "cancel_stream"],
        "repeats": repeats,
    }
    with open(outdir / "meta.json", "w") as handle:
        json.dump(meta, handle, indent=2)

    results = []
    for repeat in range(1, repeats + 1):
        for name, runner in (
            ("normal", scenario_normal),
            ("deadline", scenario_deadline),
            ("failover_recovery", scenario_failover_recovery),
            ("cancel_stream", scenario_cancel),
        ):
            print(f"[{now_iso()}] start {name} repeat={repeat}", flush=True)
            result = await runner(config, gateway_port, log_dir)
            result["repeat"] = repeat
            results.append(result)
            print(
                f"[{now_iso()}] done {name} repeat={repeat}: "
                f"{json.dumps(result['summary'])}",
                flush=True,
            )
            with open(outdir / "raw.json", "w") as handle:
                json.dump({"scenarios": results}, handle, indent=2)
    summary = aggregate_reliability(results)
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
        else PROJECT_ROOT / "bench/results/week3-reliability"
    )
    asyncio.run(async_main(outdir, repeats=args.repeat))


if __name__ == "__main__":
    main()
