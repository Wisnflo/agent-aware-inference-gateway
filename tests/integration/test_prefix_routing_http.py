"""HTTP integration for prefix-affinity and KV-cache-aware routing."""

from __future__ import annotations

import httpx

from gateway.config import BackendConfig, PrefixConfig
from tests.integration.test_reliability_http import (
    gateway_env,
    samples,
    value,
)
from tests.support import FakeBackends


PROMPT = (
    "You are a travel assistant with a long shared system prefix. "
    "Respect the budget and keep the answer short. "
) * 8


def body(prompt: str, **extra) -> dict:
    payload = {
        "prompt": prompt,
        "n_predict": 1,
        "temperature": 0,
        "stream": False,
    }
    payload.update(extra)
    return payload


async def test_prefix_affinity_routes_repeated_prompt_to_same_backend():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fakes.urls[1]),
            ],
            strategy="prefix_affinity",
            prefix=PrefixConfig(chunk_chars=32, min_match_chars=32),
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                first = await client.post("/completion", json=body(PROMPT))
                second = await client.post("/completion", json=body(PROMPT))
                assert first.status_code == second.status_code == 200
                assert (
                    first.headers["x-gateway-backend"]
                    == second.headers["x-gateway-backend"]
                )
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


async def test_prefix_affinity_falls_back_for_short_prompt():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fakes.urls[1]),
            ],
            strategy="prefix_affinity",
            prefix=PrefixConfig(chunk_chars=8, min_match_chars=64),
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                first = await client.post("/completion", json=body("hi"))
                second = await client.post("/completion", json=body("hi"))
                assert first.headers["x-gateway-backend"] != second.headers["x-gateway-backend"]
                metrics = samples((await client.get("/metrics")).text)
                assert (
                    value(
                        metrics,
                        "gateway_prefix_routing_total",
                        strategy="prefix_affinity",
                        result="fallback",
                    )
                    >= 1
                )
    finally:
        await fakes.stop()


async def test_chat_prompt_prefix_is_routed_same_way():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fakes.urls[1]),
            ],
            strategy="prefix_affinity",
            prefix=PrefixConfig(chunk_chars=64, min_match_chars=64),
        ) as (app, server):
            payload = {
                "messages": [
                    {"role": "system", "content": "shared system prefix " * 8},
                    {"role": "user", "content": "first turn"},
                ],
                "max_tokens": 1,
                "stream": False,
            }
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                first = await client.post("/v1/chat/completions", json=payload)
                second = await client.post("/v1/chat/completions", json=payload)
                assert first.status_code == second.status_code == 200
                assert (
                    first.headers["x-gateway-backend"]
                    == second.headers["x-gateway-backend"]
                )
    finally:
        await fakes.stop()


async def test_kv_cache_aware_prefers_verified_backend():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [
                BackendConfig("a", fakes.urls[0]),
                BackendConfig("b", fakes.urls[1]),
            ],
            strategy="kv_cache_aware",
            prefix=PrefixConfig(chunk_chars=32, min_match_chars=32),
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                first = await client.post(
                    "/completion", json=body(PROMPT, fake_cache_n=10, fake_prompt_n=1)
                )
                second = await client.post(
                    "/completion", json=body(PROMPT, fake_cache_n=10, fake_prompt_n=1)
                )
                assert first.status_code == second.status_code == 200
                metrics = samples((await client.get("/metrics")).text)
                assert (
                    value(
                        metrics,
                        "gateway_cache_evidence_total",
                        backend=first.headers["x-gateway-backend"],
                        result="hit",
                    )
                    >= 1
                )
                assert (
                    value(
                        metrics,
                        "gateway_prefix_routing_total",
                        strategy="kv_cache_aware",
                        result="verified",
                    )
                    >= 1
                )
    finally:
        await fakes.stop()
