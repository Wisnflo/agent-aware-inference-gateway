"""Integration tests: real HTTP between the gateway and fake backends."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx
from prometheus_client.parser import text_string_to_metric_families

from gateway.config import BackendConfig, GatewayConfig, RetryConfig
from gateway.server import create_app
from tests.support import FakeBackends, free_port, parse_sse, start_server


@asynccontextmanager
async def gateway_env(strategy="round_robin", urls=None, retry=None):
    fakes = await FakeBackends().start()
    backend_urls = urls if urls is not None else fakes.urls
    backends = tuple(
        BackendConfig(backend_id, url)
        for backend_id, url in zip(("a", "b"), backend_urls)
    )
    config = GatewayConfig(
        backends=backends,
        strategy=strategy,
        health_interval_s=3600.0,
        upstream_timeout_s=10.0,
        upstream_connect_timeout_s=2.0,
        retry=retry or RetryConfig(),
    )
    app = create_app(config)
    server = await start_server(app)
    try:
        yield fakes, app, server
    finally:
        await server.stop()
        await fakes.stop()


def samples(text: str) -> dict:
    result = {}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            result[(sample.name, tuple(sorted(sample.labels.items())))] = sample.value
    return result


def value(samples_map: dict, name: str, **labels) -> float:
    return samples_map.get((name, tuple(sorted(labels.items()))), 0.0)


async def first_event(stream) -> bytes:
    buffer = b""
    async for chunk in stream.aiter_raw():
        buffer += chunk
        if b"\n\n" in buffer:
            return buffer
    raise AssertionError("stream ended before the first event")


def backend_object(fakes: FakeBackends, name: str):
    return fakes.a if name == "a" else fakes.b


async def test_non_stream_forwarding_and_attribution():
    async with gateway_env() as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            response = await client.post("/completion", json={"prompt": "hello"})
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("application/json")
            assert response.headers["x-gateway-backend"] == "a"
            assert response.json()["backend"] == "a"
            assert response.json()["timings"]["prompt_n"] == 1

            second = await client.post("/completion", json={"prompt": "hello"})
            assert second.headers["x-gateway-backend"] == "b"


async def test_round_robin_distribution_through_gateway():
    async with gateway_env("round_robin") as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            distribution = {"a": 0, "b": 0}
            for _ in range(6):
                response = await client.post("/completion", json={"prompt": "x"})
                distribution[response.headers["x-gateway-backend"]] += 1
            assert distribution == {"a": 3, "b": 3}


async def test_session_affinity_routes_same_session_to_one_backend():
    async with gateway_env("session_affinity") as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            selected = set()
            for _ in range(10):
                response = await client.post(
                    "/completion",
                    json={"prompt": "x"},
                    headers={"X-Session-ID": "session-alpha"},
                )
                selected.add(response.headers["x-gateway-backend"])
            assert len(selected) == 1

            # The internal header must not be forwarded upstream.
            backend = backend_object(fakes, selected.pop())
            assert backend.requests
            assert all("x-session-id" not in item["headers"] for item in backend.requests)


async def test_session_affinity_falls_back_to_round_robin_without_header():
    async with gateway_env("session_affinity") as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            seen = []
            for _ in range(4):
                response = await client.post("/completion", json={"prompt": "x"})
                seen.append(response.headers["x-gateway-backend"])
            assert seen == ["a", "b", "a", "b"]


async def test_sse_event_order_and_termination():
    async with gateway_env() as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            async with client.stream(
                "POST", "/completion", json={"prompt": "x", "stream": True, "events": 4}
            ) as response:
                assert response.status_code == 200
                assert response.headers["content-type"].startswith("text/event-stream")
                raw = b""
                async for chunk in response.aiter_raw():
                    raw += chunk
                assert response.headers["x-gateway-backend"] == "a"
            events = parse_sse(raw.decode())
            contents = [event.get("content") for event in events if "content" in event]
            assert contents == ["a-0", "a-1", "a-2", "a-3", ""]
            assert events[-1] == {"done": True}
            assert sum(1 for event in events if event.get("stop")) == 1


async def test_streaming_is_not_buffered_behind_the_gateway():
    async with gateway_env() as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            async with client.stream(
                "POST",
                "/completion",
                json={"prompt": "x", "stream": True, "events": 3, "hold_after": 0},
            ) as response:
                backend_name = response.headers["x-gateway-backend"]
                backend = backend_object(fakes, backend_name)
                raw = b""
                released = False
                async with asyncio.timeout(5):
                    async for chunk in response.aiter_raw():
                        raw += chunk
                        if not released and b"\n\n" in raw:
                            # The fake backend is still holding the rest of
                            # the stream, so this proves the first event was
                            # forwarded before the response completed.
                            assert not backend.release.is_set()
                            assert f"{backend_name}-0".encode() in raw
                            released = True
                            backend.release.set()
                        if released and b"[DONE]" in raw:
                            break
                assert released
            events = parse_sse(raw.decode())
            assert [event.get("content") for event in events if "content" in event] == [
                f"{backend_name}-0",
                f"{backend_name}-1",
                f"{backend_name}-2",
                "",
            ]


async def test_client_disconnect_closes_upstream_and_clears_in_flight():
    async with gateway_env() as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            async with client.stream(
                "POST",
                "/completion",
                json={"prompt": "x", "stream": True, "events": 5, "hold_after": 0},
            ) as response:
                backend_name = response.headers["x-gateway-backend"]
                backend = backend_object(fakes, backend_name)
                await asyncio.wait_for(first_event(response), timeout=3)
                # Leave the context without draining the rest of the stream.
            await asyncio.wait_for(backend.stream_closed.wait(), timeout=5)

            for _ in range(100):
                text = (await client.get("/metrics")).text
                metrics = samples(text)
                in_flight = value(metrics, "gateway_in_flight", backend="a") + value(
                    metrics, "gateway_in_flight", backend="b"
                )
                if in_flight == 0:
                    break
                await asyncio.sleep(0.05)
            assert in_flight == 0
            disconnects = value(
                metrics, "gateway_client_disconnects_total", route="completion"
            )
            assert disconnects >= 1


async def test_upstream_http_error_passthrough_and_metric():
    async with gateway_env() as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            response = await client.post(
                "/completion", json={"prompt": "x", "fake_status": 500}
            )
            assert response.status_code == 500
            assert response.json() == {"error": "fake 500"}
            assert response.headers["x-gateway-backend"] == "a"

            text = (await client.get("/metrics")).text
            metrics = samples(text)
            assert (
                value(
                    metrics,
                    "gateway_upstream_errors_total",
                    backend="a",
                    kind="http_status",
                )
                == 1
            )
            assert value(metrics, "gateway_requests_total", route="completion", status="500", backend="a") == 1


async def test_connect_error_maps_to_502():
    async with gateway_env(retry=RetryConfig(max_retries=0)) as (fakes, app, server):
        # Both backends were healthy at startup; stop A after startup so the
        # request selects it and fails while connecting.
        await fakes.servers[0].stop()
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            response = await client.post("/completion", json={"prompt": "x"})
            assert response.status_code == 502
            assert response.json()["error"]["type"] == "upstream_connect_error"
            text = (await client.get("/metrics")).text
            metrics = samples(text)
            assert (
                value(metrics, "gateway_upstream_errors_total", backend="a", kind="connect")
                == 1
            )


async def test_metrics_count_and_low_cardinality():
    async with gateway_env("session_affinity") as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            secret = "super-secret-session-value"
            for _ in range(2):
                await client.post(
                    "/completion",
                    json={"prompt": "x", "fake_cache_n": 10, "fake_prompt_n": 2},
                    headers={"X-Session-ID": secret},
                )
            text = (await client.get("/metrics")).text
            metrics = samples(text)
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
                    status="200",
                    backend="b",
                )
                == 2
            )
            assert (
                value(metrics, "gateway_prompt_tokens_cached_total", backend="a")
                + value(metrics, "gateway_prompt_tokens_cached_total", backend="b")
                == 20
            )
            assert (
                value(metrics, "gateway_prompt_tokens_total", backend="a")
                + value(metrics, "gateway_prompt_tokens_total", backend="b")
                == 24
            )
            assert secret not in text


async def test_streaming_cache_timings_are_observed():
    async with gateway_env() as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            async with client.stream(
                "POST",
                "/completion",
                json={"prompt": "x", "stream": True, "events": 2, "fake_cache_n": 7, "fake_prompt_n": 1},
            ) as response:
                async for _ in response.aiter_raw():
                    pass
            text = (await client.get("/metrics")).text
            metrics = samples(text)
            assert value(metrics, "gateway_prompt_tokens_cached_total", backend="a") == 7
            assert value(metrics, "gateway_prompt_tokens_total", backend="a") == 8
            assert value(metrics, "gateway_sse_events_total", backend="a") == 4


async def test_upstream_stream_failure_does_not_kill_gateway():
    async with gateway_env() as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            raw = b""
            async with client.stream(
                "POST",
                "/completion",
                json={"prompt": "x", "stream": True, "events": 4, "fail_after": 1},
            ) as response:
                assert response.status_code == 200
                async for chunk in response.aiter_raw():
                    raw += chunk
            # The upstream broke mid-stream; the gateway must not fabricate a
            # completion marker and must keep serving other requests.
            assert b"a-0" in raw
            assert b"[DONE]" not in raw
            metrics = samples((await client.get("/metrics")).text)
            assert (
                value(metrics, "gateway_upstream_errors_total", backend="a", kind="stream")
                == 1
            )
            follow_up = await client.post("/completion", json={"prompt": "again"})
            assert follow_up.status_code == 200


async def test_health_reports_degraded_and_unavailable():
    async with gateway_env() as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            healthy = await client.get("/health")
            assert healthy.status_code == 200
            assert healthy.json()["status"] == "ok"
            assert healthy.json()["strategy"] == "round_robin"

            await fakes.servers[1].stop()
            await app.state.pool.check_health()
            await app.state.pool.check_health()
            degraded = await client.get("/health")
            assert degraded.status_code == 200
            assert degraded.json()["status"] == "degraded"
            assert degraded.json()["backends"]["b"]["healthy"] is False

            await fakes.servers[0].stop()
            await app.state.pool.check_health()
            await app.state.pool.check_health()
            unavailable = await client.get("/health")
            assert unavailable.status_code == 503
            assert unavailable.json()["status"] == "unavailable"

            metrics = samples((await client.get("/metrics")).text)
            assert value(metrics, "gateway_backend_health", backend="a") == 0
            assert value(metrics, "gateway_backend_health", backend="b") == 0


async def test_openai_route_is_proxied():
    async with gateway_env() as (fakes, app, server):
        async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
            response = await client.post(
                "/v1/chat/completions",
                json={"model": "qwen", "messages": [{"role": "user", "content": "hi"}]},
            )
            assert response.status_code == 200
            assert response.headers["x-gateway-backend"] == "a"
            assert fakes.a.requests[0]["path"] == "/v1/chat/completions"
