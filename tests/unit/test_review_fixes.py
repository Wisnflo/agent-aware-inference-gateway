"""Unit tests for tokenizer edge cases, middleware ContextVar reset, and SSE flush token tracking."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from gateway.admission import AdmissionController
from gateway.backends import Backend, BackendPool
from gateway.config import AdmissionConfig, BackendConfig, GatewayConfig, PrefixConfig
from gateway.metrics import Metrics
from gateway.middleware import RequestIDMiddleware, request_id_var
from gateway.proxy import Proxy, _StreamState
from gateway.router import KvCacheAwareRouter
from gateway.tokenizer import MAX_CACHED_TOKENS, LlamaTokenizer


def _mock_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url="http://tokenizer.local",
        transport=httpx.MockTransport(handler),
    )


async def test_tokenizer_rejects_empty_and_negative_tokens():
    responses = [
        httpx.Response(200, json={"tokens": []}),
        httpx.Response(200, json={"tokens": [1, -5, 3]}),
        httpx.Response(500, text="error"),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    client = _mock_client(handler)
    tok = LlamaTokenizer("http://tokenizer.local", client=client)
    try:
        assert await tok.tokenize("empty") is None
        assert await tok.tokenize("negative") is None
        assert await tok.tokenize("server-error") is None
    finally:
        await tok.aclose()
        await client.aclose()


async def test_tokenizer_truncates_and_caches_overlong_token_list():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"tokens": list(range(MAX_CACHED_TOKENS + 50))})

    client = _mock_client(handler)
    tok = LlamaTokenizer("http://tokenizer.local", client=client)
    try:
        first = await tok.tokenize("very long prompt")
        second = await tok.tokenize("very long prompt")
        assert first is not None
        assert len(first) == MAX_CACHED_TOKENS
        assert second == first
        assert calls == 1
    finally:
        await tok.aclose()
        await client.aclose()


async def test_tokenizer_single_flight_deduplicates_concurrent_requests():
    calls = 0
    gate = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        await gate.wait()
        return httpx.Response(200, json={"tokens": [10, 20, 30]})

    client = _mock_client(handler)
    tok = LlamaTokenizer("http://tokenizer.local", client=client)
    try:
        t1 = asyncio.create_task(tok.tokenize("same prompt"))
        t2 = asyncio.create_task(tok.tokenize("same prompt"))
        await asyncio.sleep(0.01)
        gate.set()
        r1, r2 = await asyncio.gather(t1, t2)
        assert r1 == (10, 20, 30)
        assert r2 == (10, 20, 30)
        assert calls == 1
    finally:
        await tok.aclose()
        await client.aclose()


async def test_request_id_middleware_resets_contextvar_on_success_and_error():
    assert request_id_var.get() == "-"

    async def ok_app(scope, receive, send):
        assert request_id_var.get() == "req-ok-1"
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    mw_ok = RequestIDMiddleware(ok_app)
    sent = []

    async def send(msg):
        sent.append(msg)

    async def receive():
        return {"type": "http.request", "body": b""}

    scope = {
        "type": "http",
        "headers": [(b"x-request-id", b"req-ok-1")],
    }
    await mw_ok(scope, receive, send)
    assert request_id_var.get() == "-"

    async def failing_app(scope, receive, send):
        assert request_id_var.get() == "req-fail-2"
        raise RuntimeError("boom")

    mw_fail = RequestIDMiddleware(failing_app)
    scope_fail = {
        "type": "http",
        "headers": [(b"x-request-id", b"req-fail-2")],
    }
    with pytest.raises(RuntimeError, match="boom"):
        await mw_fail(scope_fail, receive, send)
    assert request_id_var.get() == "-"


async def test_proxy_stream_flush_forwards_prefix_tokens_for_cache_verification():
    backend = Backend(id="a", base_url="http://127.0.0.1:18090")
    router = KvCacheAwareRouter(
        [backend], PrefixConfig(chunk_chars=8, min_match_chars=8, min_match_tokens=3)
    )
    tokens = (101, 102, 103, 104)
    router.record_prompt("ignored text", "a", prefix_tokens=tokens)

    config = GatewayConfig(backends=[BackendConfig(id="a", url="http://127.0.0.1:18090")])
    pool = BackendPool(config=config)
    metrics = Metrics()
    admission = AdmissionController(AdmissionConfig(enabled=False), ["a"])
    proxy = Proxy(config=config, pool=pool, router=router, metrics=metrics, admission=admission)

    # Final SSE event without trailing blank line so it is only emitted by decoder.flush().
    final_payload = json.dumps(
        {"content": "hi", "stop": True, "timings": {"cache_n": 4, "prompt_n": 1}}
    )
    raw_sse = f"data: {final_payload}".encode("utf-8")

    class _FakeUpstream:
        async def aiter_raw(self):
            yield raw_sse

    state = _StreamState()
    chunks = [
        chunk
        async for chunk in proxy._stream(
            _FakeUpstream(),
            backend,
            "completion",
            0.0,
            state,
            prompt="ignored text",
            prefix_tokens=tokens,
        )
    ]
    assert chunks == [raw_sse]
    decision = router.decide(None, {"a"}, prompt="ignored text", prefix_tokens=tokens)
    assert decision.cache_verified is True
    assert decision.prefix_match_tokens == 4
