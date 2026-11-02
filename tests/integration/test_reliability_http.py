"""Deterministic reliability tests: deadlines, cancellation, health, retry."""
from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager

import httpx
from prometheus_client.parser import text_string_to_metric_families

from gateway.config import (
    AdmissionConfig,
    BackendConfig,
    DeadlineConfig,
    GatewayConfig,
    HealthConfig,
    RetryConfig,
)
from gateway.server import create_app
from tests.support import FakeBackend, FakeBackends, RawBackend, start_server


@asynccontextmanager
async def gateway_env(
    backends,
    *,
    strategy="session_affinity",
    admission=None,
    deadline=None,
    retry=None,
    health=None,
    health_interval=3600.0,
):
    config = GatewayConfig(
        backends=tuple(backends),
        strategy=strategy,
        health_interval_s=health_interval,
        upstream_timeout_s=10.0,
        upstream_connect_timeout_s=1.0,
        admission=admission or AdmissionConfig(),
        deadline=deadline or DeadlineConfig(),
        retry=retry or RetryConfig(),
        health=health or HealthConfig(),
    )
    app = create_app(config)
    server = await start_server(app)
    try:
        yield app, server
    finally:
        await server.stop()


def samples(text: str) -> dict:
    result = {}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            result[(sample.name, tuple(sorted(sample.labels.items())))] = sample.value
    return result


def value(samples_map: dict, name: str, **labels) -> float:
    return samples_map.get((name, tuple(sorted(labels.items()))), 0.0)


async def wait_until(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition was not reached before timeout")
        await asyncio.sleep(0.005)


def body(**extra):
    payload = {"prompt": "x", "n_predict": 1, "temperature": 0, "stream": False}
    payload.update(extra)
    return payload


async def test_upstream_close_before_headers_is_not_retried():
    raw = await RawBackend("a", "close_before_headers").start()
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [BackendConfig("a", raw.url), BackendConfig("b", fakes.urls[1])],
            strategy="round_robin",
            retry=RetryConfig(max_retries=1),
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                response = await client.post("/completion", json=body())
                assert response.status_code == 502
                assert response.json()["error"]["type"] == "upstream_error"
                metrics = samples((await client.get("/metrics")).text)
                assert value(metrics, "gateway_retries_total", backend="a", reason="connect") == 0
                assert fakes.b.requests == []
                assert app.state.admission.stats().active_total == 0
    finally:
        await raw.stop()
        await fakes.stop()


async def test_upstream_close_mid_stream_has_no_fake_done():
    raw = await RawBackend("a", "close_mid_stream").start()
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [BackendConfig("a", raw.url), BackendConfig("b", fakes.urls[1])],
            strategy="round_robin",
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                raw_bytes = b""
                with contextlib.suppress(httpx.HTTPError):
                    async with client.stream(
                        "POST",
                        "/completion",
                        json=body(stream=True, events=1),
                    ) as response:
                        async for chunk in response.aiter_raw():
                            raw_bytes += chunk
                assert b"raw-0" in raw_bytes
                assert b"[DONE]" not in raw_bytes
                await wait_until(lambda: app.state.admission.stats().active_total == 0)
                metrics = samples((await client.get("/metrics")).text)
                assert (
                    value(
                        metrics,
                        "gateway_stream_terminations_total",
                        backend="a",
                        reason="upstream_error",
                    )
                    == 1
                )
    finally:
        await raw.stop()
        await fakes.stop()


async def test_execution_deadline_before_headers_returns_504():
    fake = FakeBackend("a")
    fake_server = await start_server(fake.app())
    try:
        async with gateway_env(
            [BackendConfig("a", fake_server.url)],
            deadline=DeadlineConfig(
                request_timeout_ms=1000, execution_timeout_ms=80
            ),
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                response = await client.post(
                    "/completion", json=body(hold_before=True)
                )
                assert response.status_code == 504
                assert response.json()["error"]["type"] == "execution_timeout"
                assert app.state.admission.stats().active_total == 0
                metrics = samples((await client.get("/metrics")).text)
                assert (
                    value(
                        metrics,
                        "gateway_execution_timeouts_total",
                        backend="a",
                        kind="execution_timeout",
                    )
                    == 1
                )
                fake.release.set()
    finally:
        fake.release.set()
        await fake_server.stop()


async def test_request_deadline_while_queued_returns_504():
    fake = FakeBackend("a")
    fake_server = await start_server(fake.app())
    try:
        async with gateway_env(
            [BackendConfig("a", fake_server.url)],
            admission=AdmissionConfig(
                max_in_flight=1, max_queue_size=2, queue_timeout_ms=5000
            ),
            deadline=DeadlineConfig(
                request_timeout_ms=150, execution_timeout_ms=1000
            ),
        ) as (app, server):
            # Hold the only capacity directly, so the HTTP request is forced
            # to wait and consume its total deadline.  This isolates queue
            # deadline semantics from the holder's own deadline.
            held = app.state.admission.try_acquire("a")
            assert held is not None
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                response = await client.post("/completion", json=body())
                assert response.status_code == 504
                assert response.json()["error"]["type"] == "request_deadline"
                assert app.state.admission.stats().queued_total == 0
                assert app.state.admission.stats().active_total == 1
            app.state.admission.release(held)
            assert app.state.admission.stats().active_total == 0
    finally:
        await fake_server.stop()


async def test_streaming_deadline_truncates_without_done():
    fake = FakeBackend("a")
    fake_server = await start_server(fake.app())
    try:
        async with gateway_env(
            [BackendConfig("a", fake_server.url)],
            deadline=DeadlineConfig(
                request_timeout_ms=1000, execution_timeout_ms=120
            ),
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                raw_bytes = b""
                with contextlib.suppress(httpx.HTTPError):
                    async with client.stream(
                        "POST",
                        "/completion",
                        json=body(stream=True, events=10, hold_after=0),
                    ) as response:
                        async for chunk in response.aiter_raw():
                            raw_bytes += chunk
                assert b"a-0" in raw_bytes
                assert b"[DONE]" not in raw_bytes
                await wait_until(lambda: app.state.admission.stats().active_total == 0)
                metrics = samples((await client.get("/metrics")).text)
                assert (
                    value(
                        metrics,
                        "gateway_execution_timeouts_total",
                        backend="a",
                        kind="execution_timeout",
                    )
                    == 1
                )
                assert (
                    value(
                        metrics,
                        "gateway_stream_terminations_total",
                        backend="a",
                        reason="deadline",
                    )
                    == 1
                )
    finally:
        fake.release.set()
        await fake_server.stop()


async def test_health_debounce_removal_and_recovery():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fakes.urls[1]),
            ],
            health=HealthConfig(failure_threshold=2, success_threshold=2),
        ) as (app, server):
            session = "health-session"
            preferred = app.state.router.decide(
                session, app.state.pool.healthy_ids()
            ).backend.id
            other = "b" if preferred == "a" else "a"
            failing = fakes.a if preferred == "a" else fakes.b

            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                failing.health_ok = False
                await app.state.pool.check_health()
                assert app.state.pool.get(preferred).healthy is True
                response = await client.post(
                    "/completion", json=body(), headers={"X-Session-ID": session}
                )
                assert response.headers["x-gateway-backend"] == preferred

                await app.state.pool.check_health()
                assert app.state.pool.get(preferred).healthy is False
                response = await client.post(
                    "/completion", json=body(), headers={"X-Session-ID": session}
                )
                assert response.status_code == 200
                assert response.headers["x-gateway-backend"] == other
                metrics = samples((await client.get("/metrics")).text)
                assert (
                    value(
                        metrics,
                        "gateway_routing_failovers_total",
                        from_backend=preferred,
                        to_backend=other,
                    )
                    == 1
                )
                assert value(metrics, "gateway_backend_removed_total", backend=preferred) == 1

                failing.health_ok = True
                await app.state.pool.check_health()
                assert app.state.pool.get(preferred).healthy is False
                await app.state.pool.check_health()
                assert app.state.pool.get(preferred).healthy is True
                response = await client.post(
                    "/completion", json=body(), headers={"X-Session-ID": session}
                )
                assert response.headers["x-gateway-backend"] == preferred
                metrics = samples((await client.get("/metrics")).text)
                assert value(metrics, "gateway_backend_restored_total", backend=preferred) == 1
    finally:
        await fakes.stop()


async def test_all_backends_unhealthy_returns_503():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fakes.urls[1]),
            ],
            health=HealthConfig(failure_threshold=1, success_threshold=1),
        ) as (app, server):
            fakes.a.health_ok = False
            fakes.b.health_ok = False
            await app.state.pool.check_health()
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                response = await client.post("/completion", json=body())
                assert response.status_code == 503
                assert response.json()["error"]["type"] == "no_healthy_backend"
    finally:
        await fakes.stop()


async def test_connect_failure_retries_to_other_backend():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fakes.urls[1]),
            ],
            strategy="round_robin",
            retry=RetryConfig(max_retries=1),
        ) as (app, server):
            await fakes.servers[0].stop()
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                response = await client.post("/completion", json=body())
                assert response.status_code == 200
                assert response.headers["x-gateway-backend"] == "b"
                metrics = samples((await client.get("/metrics")).text)
                assert value(metrics, "gateway_retries_total", backend="a", reason="connect") == 1
                assert (
                    value(
                        metrics,
                        "gateway_retry_results_total",
                        from_backend="a",
                        to_backend="b",
                        result="success",
                    )
                    == 1
                )
                assert (
                    value(
                        metrics,
                        "gateway_requests_total",
                        route="completion",
                        status="200",
                        backend="b",
                    )
                    == 1
                )
                assert (
                    value(
                        metrics,
                        "gateway_requests_total",
                        route="completion",
                        status="502",
                        backend="a",
                    )
                    == 0
                )
                assert (
                    value(metrics, "gateway_admission_admitted_total", backend="a")
                    + value(metrics, "gateway_admission_admitted_total", backend="b")
                    == 2
                )
                assert app.state.admission.stats().active_total == 0
    finally:
        await fakes.stop()


async def test_retry_cap_and_no_unsafe_retry_after_second_failure():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fakes.urls[1]),
            ],
            strategy="round_robin",
            retry=RetryConfig(max_retries=1),
        ) as (app, server):
            await fakes.servers[0].stop()
            await fakes.servers[1].stop()
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                response = await client.post("/completion", json=body())
                assert response.status_code == 502
                metrics = samples((await client.get("/metrics")).text)
                assert value(metrics, "gateway_retries_total", backend="a", reason="connect") == 1
                assert (
                    value(
                        metrics,
                        "gateway_retry_results_total",
                        from_backend="a",
                        to_backend="b",
                        result="failed",
                    )
                    == 1
                )
                assert app.state.admission.stats().active_total == 0
    finally:
        await fakes.stop()


async def test_client_disconnect_before_headers_releases_capacity():
    fake = FakeBackend("a")
    fake_server = await start_server(fake.app())
    try:
        async with gateway_env(
            [BackendConfig("a", fake_server.url)]
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                task = asyncio.create_task(
                    client.post("/completion", json=body(hold_before=True))
                )
                await wait_until(lambda: fake.started.is_set())
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                await wait_until(lambda: app.state.admission.stats().active_total == 0)
                metrics = samples((await client.get("/metrics")).text)
                assert (
                    value(
                        metrics,
                        "gateway_client_cancelled_total",
                        route="completion",
                        stage="connecting",
                    )
                    == 1
                )
                fake.release.set()
    finally:
        fake.release.set()
        await fake_server.stop()


async def test_slow_consumer_does_not_receive_unbounded_stream():
    raw = await RawBackend(
        "a", "stream_many", events=400, padding=16384
    ).start()
    try:
        async with gateway_env(
            [BackendConfig("a", raw.url)]
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                async with client.stream(
                    "POST",
                    "/completion",
                    json=body(stream=True),
                ) as response:
                    async for chunk in response.aiter_raw():
                        if b"\n\n" in chunk:
                            break
                    await asyncio.sleep(0.5)
                    # The client is not reading.  With backpressure the raw
                    # backend cannot have written all 400 events into buffers.
                    assert raw.sent_events < raw.events
                await wait_until(lambda: app.state.admission.stats().active_total == 0)
    finally:
        await raw.stop()


async def test_deadline_completion_race_releases_capacity_once():
    fake = FakeBackend("a")
    fake_server = await start_server(fake.app())
    try:
        async with gateway_env(
            [BackendConfig("a", fake_server.url)],
            deadline=DeadlineConfig(
                request_timeout_ms=1000, execution_timeout_ms=100
            ),
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                statuses = []
                for _ in range(10):
                    response = await client.post(
                        "/completion", json=body(response_delay=0.1)
                    )
                    assert response.status_code in (200, 504)
                    statuses.append(response.status_code)
                    assert app.state.admission.stats().active_total == 0
                metrics = samples((await client.get("/metrics")).text)
                assert (
                    value(
                        metrics,
                        "gateway_requests_total",
                        route="completion",
                        status="200",
                        backend="a",
                    )
                    + value(
                        metrics,
                        "gateway_requests_total",
                        route="completion",
                        status="504",
                        backend="a",
                    )
                    == 10
                )
    finally:
        await fake_server.stop()


async def test_failover_does_not_double_count_capacity():
    fake_b = FakeBackend("b")
    fake_server = await start_server(fake_b.app())
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fake_server.url),
            ],
            strategy="round_robin",
            admission=AdmissionConfig(max_in_flight=1, max_queue_size=2),
            retry=RetryConfig(max_retries=1),
        ) as (app, server):
            await fakes.servers[0].stop()
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                first = asyncio.create_task(
                    client.post("/completion", json=body(hold_before=True))
                )
                await wait_until(lambda: fake_b.started.is_set())
                stats = app.state.admission.stats()
                assert stats.active_total == 1
                assert stats.active_by_backend["a"] == 0
                assert stats.active_by_backend["b"] == 1
                second = asyncio.create_task(
                    client.post("/completion", json=body(hold_before=True))
                )
                await wait_until(
                    lambda: app.state.admission.stats().queued_total == 1
                )
                assert app.state.admission.stats().active_total == 1
                fake_b.release.set()
                first_response, second_response = await asyncio.wait_for(
                    asyncio.gather(first, second), timeout=5
                )
                assert first_response.status_code == 200
                assert second_response.status_code == 200
                assert app.state.admission.stats().active_total == 0
    finally:
        fake_b.release.set()
        await fake_server.stop()
        await fakes.stop()
