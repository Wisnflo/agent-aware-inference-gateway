"""Deterministic admission tests over real HTTP with controllable fake backends."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx
from prometheus_client.parser import text_string_to_metric_families

from gateway.config import AdmissionConfig, BackendConfig, GatewayConfig
from gateway.server import create_app
from tests.support import FakeBackends, start_server


@asynccontextmanager
async def gateway_env(admission: AdmissionConfig, strategy="session_affinity"):
    fakes = await FakeBackends().start()
    config = GatewayConfig(
        backends=(
            BackendConfig("a", fakes.urls[0]),
            BackendConfig("b", fakes.urls[1]),
        ),
        strategy=strategy,
        health_interval_s=3600.0,
        upstream_timeout_s=10.0,
        upstream_connect_timeout_s=2.0,
        admission=admission,
    )
    app = create_app(config)
    server = await start_server(app)
    try:
        yield fakes, app, server
    finally:
        await server.stop()
        await fakes.stop()


async def wait_until(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition was not reached before timeout")
        await asyncio.sleep(0.005)


def samples(text: str) -> dict:
    result = {}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            result[(sample.name, tuple(sorted(sample.labels.items())))] = sample.value
    return result


def value(samples_map: dict, name: str, **labels) -> float:
    return samples_map.get((name, tuple(sorted(labels.items()))), 0.0)


def backend_for(app, session: str):
    backend_id = app.state.router.select(session).id
    return backend_id


def fake_for(fakes: FakeBackends, backend_id: str):
    return fakes.a if backend_id == "a" else fakes.b


def request_body(prompt="x", stream=False, **extra):
    body = {
        "prompt": prompt,
        "n_predict": 1,
        "temperature": 0,
        "stream": stream,
    }
    body.update(extra)
    return body


async def test_capacity_fifo_queue_full_and_gauges():
    admission = AdmissionConfig(
        max_in_flight=1, max_queue_size=3, queue_timeout_ms=5000
    )
    async with gateway_env(admission) as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            session = "queue-session"
            backend_id = backend_for(app, session)
            backend = fake_for(fakes, backend_id)

            blocker = asyncio.create_task(
                client.post(
                    "/completion",
                    json=request_body("blocker", stream=True, events=1, hold_before=True),
                    headers={"X-Session-ID": session},
                )
            )
            await wait_until(lambda: app.state.admission.stats().active_total == 1)
            await wait_until(lambda: backend.started.is_set())

            queued = []
            for index in range(3):
                task = asyncio.create_task(
                    client.post(
                        "/completion",
                        json=request_body(f"q{index}"),
                        headers={"X-Session-ID": session},
                    )
                )
                queued.append(task)
                await wait_until(
                    lambda index=index: app.state.admission.stats().queued_total
                    == index + 1
                )

            rejected = await client.post(
                "/completion",
                json=request_body("rejected"),
                headers={"X-Session-ID": session},
            )
            assert rejected.status_code == 429
            assert rejected.json()["error"]["type"] == "queue_full"
            assert rejected.headers["retry-after"] == "1"

            metrics = samples((await client.get("/metrics")).text)
            assert value(metrics, "gateway_active_requests") == 1
            assert value(metrics, "gateway_queue_depth") == 3
            assert value(metrics, "gateway_in_flight", backend=backend_id) == 1
            assert (
                value(metrics, "gateway_queue_depth_backend", backend=backend_id) == 3
            )
            assert value(metrics, "gateway_admission_peak_in_flight") == 1
            assert value(metrics, "gateway_admission_peak_queue_depth") == 3

            backend.release.set()
            blocker_response = await asyncio.wait_for(blocker, timeout=5)
            assert blocker_response.status_code == 200
            responses = await asyncio.wait_for(asyncio.gather(*queued), timeout=5)
            assert all(response.status_code == 200 for response in responses)
            assert all(
                response.headers["x-gateway-backend"] == backend_id
                for response in responses
            )

            # FIFO: the blocker reached the backend first, then q0, q1, q2.
            prompts = [item["body"]["prompt"] for item in backend.requests]
            assert prompts == ["blocker", "q0", "q1", "q2"]

            metrics = samples((await client.get("/metrics")).text)
            assert value(metrics, "gateway_active_requests") == 0
            assert value(metrics, "gateway_queue_depth") == 0
            assert value(metrics, "gateway_admission_rejected_total", backend=backend_id) == 1
            assert value(metrics, "gateway_admission_admitted_total", backend=backend_id) == 4


async def test_streaming_holds_capacity_after_first_event():
    admission = AdmissionConfig(
        max_in_flight=1, max_queue_size=2, queue_timeout_ms=5000
    )
    async with gateway_env(admission) as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            session = "stream-session"
            backend_id = backend_for(app, session)
            backend = fake_for(fakes, backend_id)
            first_event = asyncio.Event()
            allow_read = asyncio.Event()
            consumed = asyncio.Event()

            async def consume():
                async with client.stream(
                    "POST",
                    "/completion",
                    json=request_body(
                        "stream", stream=True, events=3, hold_after=0
                    ),
                    headers={"X-Session-ID": session},
                ) as response:
                    assert response.status_code == 200
                    buffer = b""
                    async for chunk in response.aiter_raw():
                        buffer += chunk
                        if b"\n\n" in buffer and not first_event.is_set():
                            first_event.set()
                            await allow_read.wait()
                consumed.set()

            consumer = asyncio.create_task(consume())
            await asyncio.wait_for(first_event.wait(), timeout=3)
            # Headers and the first SSE event have been sent, but the upstream
            # is still holding the rest of the stream.
            assert app.state.admission.stats().active_total == 1

            second = asyncio.create_task(
                client.post(
                    "/completion",
                    json=request_body("second"),
                    headers={"X-Session-ID": session},
                )
            )
            await wait_until(lambda: app.state.admission.stats().queued_total == 1)
            assert app.state.admission.stats().active_total == 1
            assert not second.done()

            backend.release.set()
            allow_read.set()
            await asyncio.wait_for(consumer, timeout=5)
            assert consumed.is_set()
            response = await asyncio.wait_for(second, timeout=5)
            assert response.status_code == 200
            assert app.state.admission.stats().active_total == 0


async def test_queue_timeout_returns_429_and_frees_queue_slot():
    admission = AdmissionConfig(
        max_in_flight=1, max_queue_size=1, queue_timeout_ms=100
    )
    async with gateway_env(admission) as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            session = "timeout-session"
            backend_id = backend_for(app, session)
            backend = fake_for(fakes, backend_id)

            blocker = asyncio.create_task(
                client.post(
                    "/completion",
                    json=request_body("blocker", hold_before=True),
                    headers={"X-Session-ID": session},
                )
            )
            await wait_until(lambda: app.state.admission.stats().active_total == 1)

            timed_out = await client.post(
                "/completion",
                json=request_body("waiter"),
                headers={"X-Session-ID": session},
            )
            assert timed_out.status_code == 429
            assert timed_out.json()["error"]["type"] == "queue_timeout"
            assert app.state.admission.stats().queued_total == 0
            assert app.state.admission.stats().active_total == 1

            backend.release.set()
            assert (await asyncio.wait_for(blocker, timeout=5)).status_code == 200
            metrics = samples((await client.get("/metrics")).text)
            assert value(metrics, "gateway_admission_timed_out_total", backend=backend_id) == 1
            assert value(metrics, "gateway_active_requests") == 0


async def test_upstream_error_releases_capacity():
    admission = AdmissionConfig(max_in_flight=1, max_queue_size=1)
    async with gateway_env(admission) as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            failed = await client.post(
                "/completion",
                json=request_body("fail", fake_status=500),
                headers={"X-Session-ID": "error-session"},
            )
            assert failed.status_code == 500
            assert app.state.admission.stats().active_total == 0
            backend_id = app.state.router.decide(
                "error-session", app.state.pool.healthy_ids()
            ).backend.id
            # A single business 5xx is not a health failure.
            assert app.state.pool.get(backend_id).healthy is True

            ok = await client.post(
                "/completion",
                json=request_body("ok"),
                headers={"X-Session-ID": "error-session"},
            )
            assert ok.status_code == 200
            assert app.state.admission.stats().active_total == 0


async def test_burst_bounds_and_no_double_count():
    admission = AdmissionConfig(
        max_in_flight=2, max_queue_size=3, queue_timeout_ms=5000
    )
    async with gateway_env(admission) as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            session = "burst-session"
            backend_id = backend_for(app, session)
            backend = fake_for(fakes, backend_id)

            tasks = [
                asyncio.create_task(
                    client.post(
                        "/completion",
                        json=request_body(f"burst-{index}", hold_before=True),
                        headers={"X-Session-ID": session},
                    )
                )
                for index in range(10)
            ]
            await wait_until(
                lambda: app.state.admission.stats().active_total
                + app.state.admission.stats().queued_total
                == 5
            )
            stats = app.state.admission.stats()
            assert stats.active_total <= 2
            assert stats.queued_total <= 3

            backend.release.set()
            responses = await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
            statuses = [response.status_code for response in responses]
            assert statuses.count(200) == 5
            assert statuses.count(429) == 5
            assert all(
                response.json()["error"]["type"] == "queue_full"
                for response in responses
                if response.status_code == 429
            )

            metrics = samples((await client.get("/metrics")).text)
            assert value(metrics, "gateway_active_requests") == 0
            assert value(metrics, "gateway_queue_depth") == 0
            assert value(metrics, "gateway_admission_admitted_total", backend=backend_id) == 5
            assert value(metrics, "gateway_admission_rejected_total", backend=backend_id) == 5
            assert value(metrics, "gateway_admission_peak_in_flight") <= 2
            assert value(metrics, "gateway_admission_peak_queue_depth") <= 3


async def test_metrics_distinguish_success_timeout_and_rejection():
    admission = AdmissionConfig(
        max_in_flight=1, max_queue_size=1, queue_timeout_ms=200
    )
    async with gateway_env(admission) as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            session = "metric-session"
            backend_id = backend_for(app, session)
            backend = fake_for(fakes, backend_id)

            blocker = asyncio.create_task(
                client.post(
                    "/completion",
                    json=request_body("blocker", hold_before=True),
                    headers={"X-Session-ID": session},
                )
            )
            await wait_until(lambda: app.state.admission.stats().active_total == 1)

            waiter = asyncio.create_task(
                client.post(
                    "/completion",
                    json=request_body("waiter"),
                    headers={"X-Session-ID": session},
                )
            )
            await wait_until(lambda: app.state.admission.stats().queued_total == 1)

            rejected = await client.post(
                "/completion",
                json=request_body("rejected"),
                headers={"X-Session-ID": session},
            )
            assert rejected.status_code == 429
            timed_out = await asyncio.wait_for(waiter, timeout=3)
            assert timed_out.status_code == 429

            backend.release.set()
            assert (await asyncio.wait_for(blocker, timeout=5)).status_code == 200

            metrics = samples((await client.get("/metrics")).text)
            assert value(metrics, "gateway_admission_admitted_total", backend=backend_id) == 1
            assert value(metrics, "gateway_admission_rejected_total", backend=backend_id) == 1
            assert value(metrics, "gateway_admission_timed_out_total", backend=backend_id) == 1
            assert (
                value(
                    metrics,
                    "gateway_requests_total",
                    route="completion",
                    status="200",
                    backend=backend_id,
                )
                == 1
            )
            assert (
                value(
                    metrics,
                    "gateway_requests_total",
                    route="completion",
                    status="429",
                    backend=backend_id,
                )
                == 2
            )
            assert value(metrics, "gateway_queue_wait_seconds_count", backend=backend_id) == 1
            assert (
                value(metrics, "gateway_backend_execution_seconds_count",
                      route="completion", backend=backend_id)
                == 1
            )


async def test_session_affinity_survives_queueing():
    admission = AdmissionConfig(
        max_in_flight=1, max_queue_size=4, queue_timeout_ms=5000
    )
    async with gateway_env(admission) as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            session = "affinity-queue-session"
            backend_id = backend_for(app, session)
            backend = fake_for(fakes, backend_id)

            tasks = [
                asyncio.create_task(
                    client.post(
                        "/completion",
                        json=request_body(f"affinity-{index}", hold_before=True),
                        headers={"X-Session-ID": session},
                    )
                )
                for index in range(4)
            ]
            await wait_until(
                lambda: app.state.admission.stats().active_total
                + app.state.admission.stats().queued_total
                == 4
            )
            assert app.state.admission.stats().active_total == 1
            assert app.state.admission.stats().queued_total == 3
            backend.release.set()
            responses = await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
            assert all(response.status_code == 200 for response in responses)
            assert {
                response.headers["x-gateway-backend"] for response in responses
            } == {backend_id}
            assert app.state.admission.stats().active_total == 0


async def test_client_disconnect_during_stream_admits_queued_request():
    admission = AdmissionConfig(
        max_in_flight=1, max_queue_size=1, queue_timeout_ms=5000
    )
    async with gateway_env(admission) as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            session = "disconnect-session"
            backend_id = backend_for(app, session)
            backend = fake_for(fakes, backend_id)
            first_event = asyncio.Event()
            close_allowed = asyncio.Event()

            async def consume_then_close():
                async with client.stream(
                    "POST",
                    "/completion",
                    json=request_body("stream", stream=True, events=5, hold_after=0),
                    headers={"X-Session-ID": session},
                ) as response:
                    async for chunk in response.aiter_raw():
                        if b"\n\n" in chunk:
                            first_event.set()
                            await close_allowed.wait()
                            break
                # Leaving the context closes the client connection.

            consumer = asyncio.create_task(consume_then_close())
            await asyncio.wait_for(first_event.wait(), timeout=3)

            queued = asyncio.create_task(
                client.post(
                    "/completion",
                    json=request_body("after-disconnect"),
                    headers={"X-Session-ID": session},
                )
            )
            await wait_until(lambda: app.state.admission.stats().queued_total == 1)
            close_allowed.set()
            await asyncio.wait_for(consumer, timeout=3)
            await wait_until(lambda: app.state.admission.stats().active_total == 0)
            response = await asyncio.wait_for(queued, timeout=5)
            assert response.status_code == 200
            assert response.headers["x-gateway-backend"] == backend_id
