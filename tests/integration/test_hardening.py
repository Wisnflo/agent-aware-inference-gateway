"""Hardening features: body limits, request ids, admin auth, drain, retry."""

from __future__ import annotations

import asyncio

import httpx

from gateway.config import AdminConfig, BackendConfig, PrefixConfig
from tests.integration.test_reliability_http import (
    gateway_env,
    samples,
    value,
    wait_until,
)
from tests.support import FakeBackends


async def test_request_body_limit_rejects_declared_and_streamed_oversize():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [BackendConfig("a", fakes.urls[0])],
            strategy="round_robin",
            max_request_body_bytes=64,
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                response = await client.post(
                    "/completion",
                    content=b'{"prompt":"' + b"x" * 100 + b'"}',
                    headers={"content-type": "application/json"},
                )
                assert response.status_code == 413
                assert response.json()["error"]["type"] == "request_too_large"
                assert response.headers["x-request-id"]

                small = await client.post("/completion", json={"prompt": "x"})
                assert small.status_code == 200

                async def oversize_body():
                    yield b'{"prompt":"' + b"y" * 100 + b'"}'

                chunked = await client.post(
                    "/completion",
                    content=oversize_body(),
                    headers={"content-type": "application/json"},
                )
                assert chunked.status_code == 413
    finally:
        await fakes.stop()


async def test_request_id_is_echoed_generated_and_forwarded():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [BackendConfig("a", fakes.urls[0])], strategy="round_robin"
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                supplied = await client.get(
                    "/health", headers={"x-request-id": "client-request-1"}
                )
                assert supplied.headers["x-request-id"] == "client-request-1"

                generated = await client.post("/completion", json={"prompt": "x"})
                request_id = generated.headers["x-request-id"]
                assert len(request_id) == 32
                assert fakes.a.requests[-1]["headers"]["x-request-id"] == request_id

                invalid = await client.get(
                    "/health", headers={"x-request-id": "bad id with spaces"}
                )
                assert invalid.headers["x-request-id"] != "bad id with spaces"
    finally:
        await fakes.stop()


async def test_admin_token_is_required_when_configured():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [BackendConfig("a", fakes.urls[0])],
            strategy="round_robin",
            admin=AdminConfig(token="secret-token"),
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                denied = await client.get("/admin/backends")
                assert denied.status_code == 401
                assert denied.headers["www-authenticate"] == "Bearer"

                wrong = await client.get(
                    "/admin/backends", headers={"Authorization": "Bearer nope"}
                )
                assert wrong.status_code == 401

                allowed = await client.get(
                    "/admin/backends",
                    headers={"Authorization": "Bearer secret-token"},
                )
                assert allowed.status_code == 200

                metrics = samples((await client.get("/metrics")).text)
                assert value(metrics, "gateway_admin_auth_failures_total") >= 1
    finally:
        await fakes.stop()


async def test_drain_waits_for_active_work_then_removes_backend():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fakes.urls[1]),
            ],
            strategy="round_robin",
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                first = await client.post("/completion", json={"prompt": "first"})
                assert first.headers["x-gateway-backend"] == "a"

                held = asyncio.create_task(
                    client.post(
                        "/completion", json={"prompt": "held", "hold_before": True}
                    )
                )
                await wait_until(lambda: fakes.b.started.is_set())

                timed_out = await client.post("/admin/backends/b/drain?timeout_s=0.05")
                assert timed_out.status_code == 409
                assert timed_out.json()["draining"] is True
                state = (await client.get("/admin/backends")).json()
                assert state["backends"]["b"]["draining"] is True

                # While draining B receives no new work.
                routed = await client.post("/completion", json={"prompt": "new"})
                assert routed.headers["x-gateway-backend"] == "a"

                fakes.b.release.set()
                assert (await asyncio.wait_for(held, timeout=5)).status_code == 200

                drained = await client.post("/admin/backends/b/drain?timeout_s=2")
                assert drained.status_code == 200
                assert drained.json()["removed"] is True
                after = (await client.get("/admin/backends")).json()
                assert set(after["backends"]) == {"a"}
    finally:
        fakes.b.release.set()
        await fakes.stop()


async def test_undrain_restores_routing():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fakes.urls[1]),
            ],
            strategy="round_robin",
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                # Drain times out only if work exists; with no work it removes.
                # Mark draining directly through an active held request instead.
                held = asyncio.create_task(
                    client.post(
                        "/completion", json={"prompt": "held", "hold_before": True}
                    )
                )
                await wait_until(lambda: fakes.a.started.is_set())
                timed_out = await client.post("/admin/backends/a/drain?timeout_s=0.01")
                assert timed_out.status_code == 409

                resumed = await client.post("/admin/backends/a/undrain")
                assert resumed.status_code == 200
                assert resumed.json()["draining"] is False
                fakes.a.release.set()
                await asyncio.wait_for(held, timeout=5)
                state = (await client.get("/admin/backends")).json()
                assert state["backends"]["a"]["draining"] is False
    finally:
        fakes.a.release.set()
        await fakes.stop()


async def test_retry_prefers_backend_with_matching_prefix():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fakes.urls[1]),
            ],
            strategy="prefix_affinity",
            prefix=PrefixConfig(chunk_chars=8, min_match_chars=8),
        ) as (app, server):
            prompt = "a long shared prefix that both backends have seen"
            app.state.router.record_prompt(prompt, "a")
            app.state.router.record_prompt(prompt, "b")
            await fakes.servers[0].stop()

            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                response = await client.post("/completion", json={"prompt": prompt})
                assert response.status_code == 200
                assert response.headers["x-gateway-backend"] == "b"
                metrics = samples((await client.get("/metrics")).text)
                assert (
                    value(
                        metrics,
                        "gateway_retry_prefix_preserved_total",
                        strategy="prefix_affinity",
                    )
                    == 1
                )
    finally:
        await fakes.stop()


async def test_openai_completions_embeddings_and_models_routes():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [BackendConfig("a", fakes.urls[0])], strategy="round_robin"
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                completions = await client.post(
                    "/v1/completions", json={"prompt": "x"}
                )
                assert completions.status_code == 200
                assert fakes.a.requests[-1]["path"] == "/v1/completions"

                embeddings = await client.post(
                    "/v1/embeddings", json={"input": "embed me"}
                )
                assert embeddings.status_code == 200
                assert fakes.a.requests[-1]["path"] == "/v1/embeddings"

                models = await client.get("/v1/models")
                assert models.status_code == 200
                assert models.json()["data"][0]["id"] == "fake-model"
                assert models.headers["x-gateway-backend"] == "a"
    finally:
        await fakes.stop()


async def test_prefix_trie_metrics_are_exported():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [BackendConfig("a", fakes.urls[0])],
            strategy="prefix_affinity",
            prefix=PrefixConfig(chunk_chars=8, min_match_chars=8),
        ) as (app, server):
            prompt = "metrics-visible long shared prefix"
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                await client.post("/completion", json={"prompt": prompt})
                await client.post("/completion", json={"prompt": prompt})
                metrics = samples((await client.get("/metrics")).text)
                assert value(metrics, "gateway_prefix_trie_nodes") > 0
                assert value(metrics, "gateway_prefix_verified_entries") == 0
                assert (
                    value(
                        metrics,
                        "gateway_prefix_routing_total",
                        strategy="prefix_affinity",
                        result="hit",
                    )
                    >= 1
                )
    finally:
        await fakes.stop()


async def test_tokenizer_prefix_matches_model_tokens():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [BackendConfig("a", fakes.urls[0])],
            strategy="prefix_affinity",
            prefix=PrefixConfig(
                min_match_chars=1,
                tokenizer_url=fakes.urls[0],
                min_match_tokens=4,
            ),
        ) as (app, server):
            prompt = "tokenizer aware prefix"
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                first = await client.post("/completion", json={"prompt": prompt})
                assert first.status_code == 200
                second = await client.post("/completion", json={"prompt": prompt})
                assert second.headers["x-gateway-backend"] == "a"
                metrics = samples((await client.get("/metrics")).text)
                assert (
                    value(metrics, "gateway_prefix_match_tokens_count", backend="a")
                    >= 1
                )
    finally:
        await fakes.stop()


async def test_tokenizer_failure_falls_back_to_character_prefix():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [BackendConfig("a", fakes.urls[0])],
            strategy="prefix_affinity",
            prefix=PrefixConfig(
                chunk_chars=8,
                min_match_chars=8,
                tokenizer_url="http://127.0.0.1:1",
                tokenizer_timeout_s=0.2,
            ),
        ) as (app, server):
            prompt = "fallback still routes this long prefix"
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                assert (
                    await client.post("/completion", json={"prompt": prompt})
                ).status_code == 200
                second = await client.post("/completion", json={"prompt": prompt})
                assert second.headers["x-gateway-backend"] == "a"
                metrics = samples((await client.get("/metrics")).text)
                assert (
                    value(
                        metrics,
                        "gateway_prefix_routing_total",
                        strategy="prefix_affinity",
                        result="hit",
                    )
                    >= 1
                )
    finally:
        await fakes.stop()


async def test_health_probe_timeout_uses_health_timeout():
    from gateway.config import HealthConfig

    fakes = await FakeBackends().start()
    fakes.a.health_delay = 1.0
    try:
        async with gateway_env(
            [BackendConfig("a", fakes.urls[0])],
            strategy="round_robin",
            health=HealthConfig(timeout_s=0.05),
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                response = await client.get("/health")
                assert response.status_code == 503
                assert response.json()["backends"]["a"]["healthy"] is False
                assert (
                    response.json()["backends"]["a"]["last_probe_duration_ms"] < 1000
                )
    finally:
        fakes.a.health_delay = 0.0
        await fakes.stop()


async def test_admin_cache_reports_slot_counters_without_prefix_identity():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fakes.urls[1]),
            ],
            strategy="round_robin",
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                response = await client.get("/admin/cache")
                assert response.status_code == 200
                payload = response.json()
                assert "not a prefix" in payload["note"]
                slot = payload["backends"]["a"]["slots"][0]
                assert slot["n_prompt_tokens_cache"] == 10
                assert slot["n_prompt_tokens"] == 12
                assert "params" not in slot
    finally:
        await fakes.stop()
