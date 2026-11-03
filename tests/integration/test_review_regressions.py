"""Regression coverage for independent review findings F1–F7."""

import asyncio
import contextlib
import time

import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

from gateway.config import AdmissionConfig, BackendConfig, DeadlineConfig, GatewayConfig
from gateway.admission import AdmissionState
from gateway.proxy import _ClientGone, _AttemptTimeout, _StreamState, _DeadlineResponse
from gateway.server import create_app
from tests.support import FakeBackend, FakeBackends, start_server
from tests.integration.test_reliability_http import (
    gateway_env,
    wait_until,
    samples,
    value,
)


@pytest.mark.parametrize("path", ["/completion", "/v1/chat/completions"])
async def test_host_header_cannot_change_upstream_route(path):
    seen = []

    async def endpoint(request):
        seen.append(request.scope["path"])
        return JSONResponse(
            {
                "path": request.scope["path"],
                "query": request.scope["query_string"].decode(),
            }
        )

    backend = await start_server(
        Starlette(
            routes=[
                Route("/health", endpoint),
                Route("/{path:path}", endpoint, methods=["POST"]),
            ]
        )
    )
    try:
        async with gateway_env([BackendConfig("a", backend.url)]) as (app, gateway):
            async with httpx.AsyncClient(base_url=gateway.url) as client:
                r = await client.post(
                    path + "?keep=%2Fvalue&x=1",
                    json={},
                    headers={"host": "gateway.invalid/admin?"},
                )
                assert r.status_code == 200
                assert r.json() == {"path": path, "query": "keep=%2Fvalue&x=1"}
    finally:
        await backend.stop()


@pytest.mark.parametrize("field", ["fake_cache_n", "fake_prompt_n"])
@pytest.mark.parametrize("stream", [False, True])
async def test_negative_metrics_do_not_leak_ticket(field, stream):
    fake = FakeBackend("a")
    backend = await start_server(fake.app())
    try:
        async with gateway_env(
            [BackendConfig("a", backend.url)],
            admission=AdmissionConfig(max_in_flight=1, max_queue_size=0),
        ) as (app, gateway):
            async with httpx.AsyncClient(base_url=gateway.url) as client:
                r = await client.post("/completion", json={field: -1, "stream": stream})
                active = app.state.admission.stats().active_total
                r2 = await client.post("/completion", json={})
                assert (r.status_code, active, r2.status_code) == (200, 0, 200)
                metrics = samples(app.state.metrics.render().decode())
                assert (
                    value(metrics, "gateway_prompt_tokens_cached_total", backend="a")
                    == 0
                )
                assert (
                    value(metrics, "gateway_prompt_tokens_fresh_total", backend="a")
                    == 1
                )
    finally:
        await backend.stop()


@pytest.mark.parametrize("kind", ["execution_timeout", "request_deadline"])
async def test_retry_queue_honors_execution_deadline(kind):
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [BackendConfig("a", fakes.urls[0]), BackendConfig("b", fakes.urls[1])],
            strategy="round_robin",
            admission=AdmissionConfig(
                max_in_flight=2,
                max_queue_size=2,
                queue_timeout_ms=350,
                per_backend=(("b", 1),),
            ),
            deadline=DeadlineConfig(
                request_timeout_ms=80 if kind == "request_deadline" else 1500,
                execution_timeout_ms=80 if kind == "execution_timeout" else 1500,
            ),
        ) as (app, gateway):
            held = app.state.admission.try_acquire("b")
            await fakes.servers[0].stop()
            try:
                async with httpx.AsyncClient(base_url=gateway.url) as client:
                    started = time.perf_counter()
                    r = await client.post("/completion", json={})
                    elapsed = time.perf_counter() - started
                    assert elapsed < 0.25
                    assert r.status_code == 504
                    assert r.json()["error"]["type"] == kind
                    assert app.state.admission.stats().queued_total == 0
                    assert app.state.admission.stats().active_total == 1
            finally:
                app.state.admission.release(held)
    finally:
        await fakes.stop()


class TrackingStream(httpx.AsyncByteStream):
    def __init__(self):
        self.closed = False

    async def __aiter__(self):
        yield b"{}"

    async def aclose(self):
        self.closed = True


async def test_disconnect_race_closes_completed_upstream_response():
    app = create_app(
        GatewayConfig(backends=(BackendConfig("a", "http://backend.invalid"),))
    )
    gate = asyncio.Event()
    produced = asyncio.Event()
    stream = TrackingStream()
    response = httpx.Response(200, stream=stream)

    async def transport(request):
        await gate.wait()
        produced.set()
        return response

    class DisconnectAtCompletion:
        async def is_disconnected(self):
            gate.set()
            await produced.wait()
            await asyncio.sleep(0)
            return True

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        backend = app.state.pool.get("a")
        backend.client = client
        with pytest.raises(_ClientGone):
            await app.state.proxy._send_with_disconnect(
                DisconnectAtCompletion(),
                backend,
                client.build_request("POST", "http://backend.invalid/completion"),
                1,
            )
        assert response.is_closed and stream.closed
        await response.aclose()


async def test_external_cancel_race_closes_completed_upstream_response():
    app = create_app(
        GatewayConfig(backends=(BackendConfig("a", "http://backend.invalid"),))
    )
    stream = TrackingStream()
    response = httpx.Response(200, stream=stream)
    parent = None

    async def transport(request):
        # Cancellation becomes pending as response headers finish arriving.
        # The send task completes before its cancelled owner resumes.
        parent.cancel()
        return response

    class Connected:
        async def is_disconnected(self):
            return False

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        backend = app.state.pool.get("a")
        backend.client = client
        parent = asyncio.create_task(
            app.state.proxy._send_with_disconnect(
                Connected(),
                backend,
                client.build_request("POST", "http://backend.invalid/completion"),
                1,
            )
        )
        with pytest.raises(asyncio.CancelledError):
            await parent
        assert response.is_closed and stream.closed
        await response.aclose()


async def test_json_body_disconnect_closes_upstream_and_releases_capacity():
    released = asyncio.Event()
    started = asyncio.Event()
    closed = asyncio.Event()

    async def health(request):
        return JSONResponse({})

    async def chunks():
        try:
            yield b'{"content":"'
            started.set()
            await released.wait()
            yield b'hello"}'
        finally:
            closed.set()

    async def endpoint(request):
        return StreamingResponse(chunks(), media_type="application/json")

    backend = await start_server(
        Starlette(
            routes=[
                Route("/health", health),
                Route("/completion", endpoint, methods=["POST"]),
            ]
        )
    )
    try:
        async with gateway_env(
            [BackendConfig("a", backend.url)],
            deadline=DeadlineConfig(request_timeout_ms=1500, execution_timeout_ms=1000),
        ) as (app, gateway):
            async with httpx.AsyncClient(base_url=gateway.url) as client:
                task = asyncio.create_task(client.post("/completion", json={}))
                await asyncio.wait_for(started.wait(), 1)
                await asyncio.sleep(0.08)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                await asyncio.sleep(0.20)
                active = app.state.admission.stats().active_total
                assert active == 0 and closed.is_set()
                released.set()
                await wait_until(lambda: app.state.admission.stats().active_total == 0)
    finally:
        released.set()
        await backend.stop()


async def test_json_response_send_honors_execution_deadline():
    app = create_app(
        GatewayConfig(
            backends=(BackendConfig("a", "http://backend.invalid"),),
            deadline=DeadlineConfig(request_timeout_ms=60, execution_timeout_ms=50),
        )
    )
    async with httpx.AsyncClient(
        base_url="http://backend.invalid",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"content": "ok"})
        ),
    ) as client:
        app.state.pool.get("a").client = client
        received = False

        async def receive():
            nonlocal received
            if not received:
                received = True
                return {"type": "http.request", "body": b"{}", "more_body": False}
            await asyncio.Event().wait()

        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/completion",
            "raw_path": b"/completion",
            "query_string": b"",
            "root_path": "",
            "headers": [],
            "server": ("127.0.0.1", 1),
            "client": ("127.0.0.1", 2),
        }
        body_send = asyncio.Event()

        async def send(message):
            if message["type"] == "http.response.body":
                body_send.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(app(scope, receive, send))
        await asyncio.wait_for(body_send.wait(), 1)
        before = samples(app.state.metrics.render().decode())
        assert (
            value(
                before,
                "gateway_requests_total",
                route="completion",
                status="200",
                backend="a",
            )
            == 0
        )
        await asyncio.sleep(0.15)
        assert task.done()
        await task
        assert app.state.admission.stats().active_total == 0
        after = samples(app.state.metrics.render().decode())
        assert (
            value(
                after,
                "gateway_requests_total",
                route="completion",
                status="200",
                backend="a",
            )
            == 1
        )
        assert (
            value(
                after,
                "gateway_requests_total",
                route="completion",
                status="504",
                backend="a",
            )
            == 0
        )
        assert (
            value(
                after,
                "gateway_execution_timeouts_total",
                backend="a",
                kind="execution_timeout",
            )
            == 1
        )


async def test_shared_upstream_cookie_does_not_cross_sessions():
    seen = []

    async def health(request):
        response = JSONResponse({})
        response.set_cookie("health_session", "probe")
        return response

    async def endpoint(request):
        seen.append(request.headers.get("cookie"))
        response = JSONResponse({})
        if len(seen) == 1:
            response.set_cookie("upstream_session", "session-A")
        return response

    backend = await start_server(
        Starlette(
            routes=[
                Route("/health", health),
                Route("/completion", endpoint, methods=["POST"]),
            ]
        )
    )
    try:
        async with gateway_env([BackendConfig("a", backend.url)]) as (app, gateway):
            for sid in ("A", "B"):
                async with httpx.AsyncClient(base_url=gateway.url) as client:
                    headers = {"X-Session-ID": sid}
                    if sid == "A":
                        headers["Cookie"] = "client=A"
                    r = await client.post("/completion", json={}, headers=headers)
                    assert r.status_code == 200
                    assert "set-cookie" not in r.headers
            assert seen == ["client=A", None]
            assert list(app.state.pool.get("a").client.cookies.jar) == []
    finally:
        await backend.stop()


async def test_timeout_race_closes_completed_upstream_response():
    app = create_app(
        GatewayConfig(backends=(BackendConfig("a", "http://backend.invalid"),))
    )
    gate, produced = asyncio.Event(), asyncio.Event()
    stream = TrackingStream()
    response = httpx.Response(200, stream=stream)

    async def transport(request):
        await gate.wait()
        produced.set()
        return response

    class TimeoutAtCompletion:
        async def is_disconnected(self):
            gate.set()
            await produced.wait()
            await asyncio.sleep(1)
            return False

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        backend = app.state.pool.get("a")
        backend.client = client
        with pytest.raises(_AttemptTimeout):
            await app.state.proxy._send_with_disconnect(
                TimeoutAtCompletion(),
                backend,
                client.build_request("POST", "http://backend.invalid/completion"),
                0.08,
            )
        assert response.is_closed and stream.closed


async def test_stream_finalizer_survives_repeated_cancellation():
    app = create_app(
        GatewayConfig(backends=(BackendConfig("a", "http://backend.invalid"),))
    )
    entered, release = asyncio.Event(), asyncio.Event()

    class BlockingClose(TrackingStream):
        async def aclose(self):
            entered.set()
            await release.wait()
            self.closed = True

    backend = app.state.pool.get("a")
    ticket = app.state.admission.try_acquire("a")
    stream = BlockingClose()
    response = httpx.Response(200, stream=stream)
    state = _StreamState(disconnected=True)
    task = asyncio.create_task(
        app.state.proxy._finalize_stream(
            state,
            response,
            ticket,
            backend,
            "completion",
            time.perf_counter(),
            None,
        )
    )
    await asyncio.wait_for(entered.wait(), 1)
    assert app.state.admission.stats().active_total == 0
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert stream.closed and response.is_closed
    assert ticket.state is AdmissionState.CANCELLED
    await app.state.proxy._finalize_stream(
        state,
        response,
        ticket,
        backend,
        "completion",
        time.perf_counter(),
        None,
    )
    assert app.state.admission.stats().active_total == 0


@pytest.mark.parametrize("streaming", [False, True])
async def test_usage_observer_failure_does_not_change_response_or_leak_ticket(
    monkeypatch, streaming
):
    fake = FakeBackend("a")
    backend = await start_server(fake.app())

    def broken_observer(*args):
        raise RuntimeError("injected telemetry failure")

    try:
        async with gateway_env([BackendConfig("a", backend.url)]) as (app, gateway):
            monkeypatch.setattr(app.state.metrics, "observe_usage", broken_observer)
            async with httpx.AsyncClient(base_url=gateway.url) as client:
                response = await client.post("/completion", json={"stream": streaming})
                assert response.status_code == 200
                if streaming:
                    assert b"[DONE]" in response.content
                assert app.state.admission.stats().active_total == 0
    finally:
        await backend.stop()


async def test_unexpected_body_processing_exception_still_releases_and_closes(
    monkeypatch,
):
    app = create_app(
        GatewayConfig(backends=(BackendConfig("a", "http://backend.invalid"),))
    )
    stream = TrackingStream()
    upstream = httpx.Response(
        200, headers={"content-type": "application/json"}, stream=stream
    )

    def broken_processing(*args):
        raise RuntimeError("unexpected processing failure")

    monkeypatch.setattr(app.state.proxy, "_observe_json_usage", broken_processing)
    async with httpx.AsyncClient(
        base_url="http://backend.invalid",
        transport=httpx.MockTransport(lambda request: upstream),
    ) as client:
        app.state.pool.get("a").client = client
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://gateway",
        ) as downstream:
            response = await downstream.post("/completion", json={})
        assert response.status_code == 500
        assert app.state.admission.stats().active_total == 0
        assert upstream.is_closed and stream.closed


async def test_expired_json_response_sends_real_504():
    completed = []
    messages = []
    response = _DeadlineResponse(
        content=b"original",
        deadline_at=time.perf_counter() - 1,
        timeout_kind="execution_timeout",
        on_finish=lambda status, reason: completed.append((status, reason)),
    )

    async def receive():
        await asyncio.Event().wait()

    async def send(message):
        messages.append(message)

    await asyncio.wait_for(response({"type": "http"}, receive, send), 0.2)
    assert messages[0]["status"] == 504
    assert b"execution_timeout" in messages[1]["body"]
    assert b"original" not in messages[1]["body"]
    assert completed == [(504, "deadline")]


@pytest.mark.parametrize("expired", [False, True])
async def test_json_header_backpressure_is_bounded(expired):
    completed = []
    response = _DeadlineResponse(
        content=b"body",
        deadline_at=time.perf_counter() + (-1 if expired else 0.03),
        timeout_kind="request_deadline",
        on_finish=lambda status, reason: completed.append((status, reason)),
    )

    async def receive():
        await asyncio.Event().wait()

    async def send(message):
        await asyncio.Event().wait()

    await asyncio.wait_for(response({"type": "http"}, receive, send), 0.2)
    assert completed == [(504 if expired else 200, "deadline")]


async def test_json_cancellation_cleanup_survives_second_cancel():
    app = create_app(
        GatewayConfig(backends=(BackendConfig("a", "http://backend.invalid"),))
    )
    reading, closing, release_close = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class BlockingJSON(TrackingStream):
        async def __aiter__(self):
            yield b"{"
            reading.set()
            await asyncio.Event().wait()

        async def aclose(self):
            closing.set()
            await release_close.wait()
            self.closed = True

    stream = BlockingJSON()
    upstream = httpx.Response(
        200, headers={"content-type": "application/json"}, stream=stream
    )
    async with httpx.AsyncClient(
        base_url="http://backend.invalid",
        transport=httpx.MockTransport(lambda request: upstream),
    ) as client:
        app.state.pool.get("a").client = client
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gateway"
        ) as downstream:
            task = asyncio.create_task(downstream.post("/completion", json={}))
            await asyncio.wait_for(reading.wait(), 1)
            task.cancel()
            await asyncio.wait_for(closing.wait(), 1)
            assert app.state.admission.stats().active_total == 0
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            release_close.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
    assert stream.closed and upstream.is_closed


async def test_delayed_ticket_delivery_does_not_restart_execution_budget(monkeypatch):
    app = create_app(
        GatewayConfig(
            backends=(BackendConfig("a", "http://backend.invalid"),),
            deadline=DeadlineConfig(request_timeout_ms=1000, execution_timeout_ms=50),
        )
    )
    proxy = app.state.proxy
    acquire = proxy._acquire
    tickets, dispatched = [], []

    async def delayed_acquire(*args):
        ticket = await acquire(*args)
        tickets.append(ticket)
        # Admission has happened, but delivery to the handler is delayed beyond
        # the original execution budget (as with a delayed queue wakeup).
        await asyncio.sleep(0.08)
        return ticket

    def transport(request):
        dispatched.append(request)
        return httpx.Response(200, json={"content": "should not execute"})

    monkeypatch.setattr(proxy, "_acquire", delayed_acquire)
    async with httpx.AsyncClient(
        base_url="http://backend.invalid", transport=httpx.MockTransport(transport)
    ) as client:
        app.state.pool.get("a").client = client
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gateway"
        ) as downstream:
            response = await downstream.post("/completion", json={})

    assert response.status_code == 504
    assert response.json()["error"]["type"] == "execution_timeout"
    assert dispatched == []
    assert tickets[0].state is AdmissionState.TIMED_OUT
    assert app.state.admission.stats().active_total == 0
