"""Runtime backend registry and health refresh endpoints."""

from __future__ import annotations

import asyncio

import httpx

from gateway.config import BackendConfig
from tests.integration.test_reliability_http import gateway_env, wait_until
from tests.support import FakeBackends


async def test_admin_can_add_and_remove_backend():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env(
            [BackendConfig("a", fakes.urls[0])], strategy="round_robin"
        ) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                initial = await client.get("/admin/backends")
                assert initial.status_code == 200
                assert set(initial.json()["backends"]) == {"a"}

                added = await client.post(
                    "/admin/backends",
                    json={"id": "b", "url": fakes.urls[1]},
                )
                assert added.status_code == 201
                assert added.json()["healthy"] is True

                listed = await client.get("/admin/backends")
                assert set(listed.json()["backends"]) == {"a", "b"}

                backends = []
                for _ in range(4):
                    response = await client.post("/completion", json={"prompt": "x"})
                    backends.append(response.headers["x-gateway-backend"])
                assert set(backends) == {"a", "b"}

                removed = await client.delete("/admin/backends/b")
                assert removed.status_code == 200
                after = await client.get("/admin/backends")
                assert set(after.json()["backends"]) == {"a"}
                response = await client.post("/completion", json={"prompt": "x"})
                assert response.headers["x-gateway-backend"] == "a"
    finally:
        await fakes.stop()


async def test_admin_rejects_duplicate_and_bad_backend():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env([BackendConfig("a", fakes.urls[0])]) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                duplicate = await client.post(
                    "/admin/backends",
                    json={"id": "a", "url": fakes.urls[1]},
                )
                assert duplicate.status_code == 409
                bad = await client.post(
                    "/admin/backends",
                    json={"id": "c", "url": "not-a-url"},
                )
                assert bad.status_code == 400
                missing = await client.delete("/admin/backends/nope")
                assert missing.status_code == 404
    finally:
        await fakes.stop()


async def test_admin_cannot_remove_backend_with_active_request():
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
                # First round-robin request goes to A, second is held on B.
                first = await client.post("/completion", json={"prompt": "first"})
                assert first.headers["x-gateway-backend"] == "a"
                held = asyncio.create_task(
                    client.post("/completion", json={"prompt": "held", "hold_before": True})
                )
                await wait_until(lambda: fakes.b.started.is_set())
                refused = await client.delete("/admin/backends/b")
                assert refused.status_code == 409
                fakes.b.release.set()
                assert (await asyncio.wait_for(held, timeout=5)).status_code == 200
                removed = await client.delete("/admin/backends/b")
                assert removed.status_code == 200
    finally:
        fakes.b.release.set()
        await fakes.stop()


async def test_admin_health_refresh_returns_backend_state():
    fakes = await FakeBackends().start()
    try:
        async with gateway_env([BackendConfig("a", fakes.urls[0])]) as (app, server):
            async with httpx.AsyncClient(base_url=server.url, timeout=10) as client:
                response = await client.post("/admin/health/check")
                assert response.status_code == 200
                assert response.json()["backends"]["a"]["healthy"] is True
    finally:
        await fakes.stop()


async def test_admin_rejects_non_loopback_client():
    from gateway.config import GatewayConfig
    from gateway.server import create_app

    app = create_app(
        GatewayConfig(backends=(BackendConfig("a", "http://127.0.0.1:1"),))
    )
    transport = httpx.ASGITransport(app=app, client=("10.1.2.3", 12345))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/admin/backends")
    assert response.status_code == 403
