"""ASGI application and command-line entrypoint."""
from __future__ import annotations

import argparse
import asyncio
import dataclasses
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager

import httpx
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from .admission import AdmissionController
from .backends import BackendPool
from .config import BackendConfig, ConfigError, GatewayConfig, STRATEGIES, load_config
from .metrics import CONTENT_TYPE_LATEST, Metrics
from .middleware import RequestIDFilter, RequestIDMiddleware
from .proxy import Proxy
from .router import make_router
from .tokenizer import LlamaTokenizer

logger = logging.getLogger("gateway.server")


def create_app(config: GatewayConfig) -> Starlette:
    metrics = Metrics()
    pool = BackendPool(config=config)

    def on_health_change(backend, healthy: bool, reason: str) -> None:
        metrics.backend_health.labels(backend.id).set(1 if healthy else 0)
        if reason != "startup":
            if healthy:
                metrics.backend_restored.labels(backend.id).inc()
            else:
                metrics.backend_removed.labels(backend.id).inc()
        logger.info(
            "health transition backend=%s healthy=%s reason=%s",
            backend.id,
            healthy,
            reason,
        )

    def on_probe(backend_id: str, success: bool) -> None:
        metrics.health_checks.labels(
            backend_id, "success" if success else "failure"
        ).inc()

    pool.set_callbacks(on_health_change=on_health_change, on_probe=on_probe)

    def on_prefix_event(reason: str, count: int) -> None:
        metrics.prefix_evictions.labels(reason).inc(count)

    router = make_router(
        config.strategy, pool.backends, config.prefix, on_prefix_event
    )
    tokenizer = (
        LlamaTokenizer(
            config.prefix.tokenizer_url,
            timeout_s=config.prefix.tokenizer_timeout_s,
            cache_entries=config.prefix.tokenizer_cache_entries,
        )
        if config.prefix.tokenizer_url
        else None
    )
    admission = AdmissionController(
        config.admission, [backend.id for backend in pool.backends]
    )
    proxy = Proxy(
        config=config,
        pool=pool,
        router=router,
        metrics=metrics,
        admission=admission,
        tokenizer=tokenizer,
    )
    admission.set_on_change(proxy.sync_capacity_metrics)
    proxy.sync_capacity_metrics()

    @asynccontextmanager
    async def lifespan(app: Starlette):
        await pool.start()
        _refresh_health_gauge(pool, metrics)
        logger.info(
            "gateway started strategy=%s backends=%s",
            config.strategy,
            [backend.id for backend in pool.backends],
        )
        try:
            yield
        finally:
            proxy.accepting = False
            idle = False
            try:
                idle = await admission.wait_all_idle(config.shutdown_timeout_s)
            except asyncio.CancelledError:
                raise
            finally:
                if not idle:
                    logger.warning(
                        "shutdown timeout with active=%d queued=%d",
                        admission.stats().active_total,
                        admission.stats().queued_total,
                    )
                await pool.close()
                if tokenizer is not None:
                    await tokenizer.aclose()
            logger.info("gateway stopped")

    async def completion(request: Request) -> Response:
        return await proxy.handle(request, route="completion")

    async def chat_completions(request: Request) -> Response:
        return await proxy.handle(request, route="chat_completions")

    async def v1_completions(request: Request) -> Response:
        return await proxy.handle(request, route="v1_completions")

    async def v1_embeddings(request: Request) -> Response:
        return await proxy.handle(request, route="v1_embeddings")

    async def v1_models(request: Request) -> Response:
        return await proxy.get_models(request)

    async def health(request: Request) -> Response:
        snapshot = pool.snapshot()
        _refresh_health_gauge(pool, metrics)
        admission_stats = admission.stats()
        states = [item["healthy"] for item in snapshot.values()]
        if states and all(state is False for state in states):
            status_code, status = 503, "unavailable"
        elif any(state is False for state in states):
            status_code, status = 200, "degraded"
        else:
            status_code, status = 200, "ok"
        return JSONResponse(
            {
                "status": status,
                "strategy": config.strategy,
                "backends": snapshot,
                "admission": {
                    "enabled": admission_stats.enabled,
                    "in_flight": admission_stats.active_total,
                    "queued": admission_stats.queued_total,
                    "max_in_flight": admission_stats.max_in_flight,
                    "max_queue_size": admission_stats.max_queue_size,
                },
            },
            status_code=status_code,
        )

    async def metrics_endpoint(request: Request) -> Response:
        _refresh_health_gauge(pool, metrics)
        proxy.sync_capacity_metrics()
        prefix_stats = router.prefix_stats()
        if prefix_stats is not None:
            metrics.sync_prefix_stats(prefix_stats)
        return Response(metrics.render(), media_type=CONTENT_TYPE_LATEST)

    def admin_loopback(request: Request) -> bool:
        host = request.client.host if request.client else None
        return host in (None, "127.0.0.1", "::1", "localhost")

    def admin_denied(request: Request, action: str) -> JSONResponse | None:
        """Return an error response when an admin request is not authorized."""
        if not admin_loopback(request):
            metrics.admin_requests.labels(action, "forbidden").inc()
            logger.warning(
                "admin denied action=%s reason=non_loopback request_id=%s",
                action,
                getattr(request.state, "request_id", "-"),
            )
            return JSONResponse(
                {"error": "admin endpoints are loopback-only"}, status_code=403
            )
        token = config.admin.token
        if token is not None:
            supplied = request.headers.get("authorization", "")
            expected = f"Bearer {token}"
            if not hmac.compare_digest(
                supplied.encode("utf-8", "replace"), expected.encode("utf-8")
            ):
                metrics.admin_auth_failures.inc()
                metrics.admin_requests.labels(action, "unauthorized").inc()
                logger.warning(
                    "admin denied action=%s reason=bad_token request_id=%s",
                    action,
                    getattr(request.state, "request_id", "-"),
                )
                return JSONResponse(
                    {"error": "invalid or missing admin token"},
                    status_code=401,
                    headers={"WWW-Authenticate": "Bearer"},
                )
        return None

    def admin_log(request: Request, action: str, result: str, detail: str = "") -> None:
        logger.info(
            "admin action=%s result=%s %s request_id=%s",
            action,
            result,
            detail,
            getattr(request.state, "request_id", "-"),
        )

    def detach_registry(backend_id: str):
        """Update routing and admission synchronously, then return clients."""
        router.remove_backend(backend_id)
        admission.remove_backend(backend_id)
        metrics.backend_draining.labels(backend_id).set(0)
        clients = pool.detach_backend(backend_id)
        proxy.sync_capacity_metrics()
        return clients

    async def close_clients(clients) -> None:
        for client in clients:
            if client is not None:
                await client.aclose()

    async def admin_backends(request: Request) -> Response:
        action = "list_backends" if request.method == "GET" else "add_backend"
        denied = admin_denied(request, action)
        if denied is not None:
            return denied
        if request.method == "GET":
            metrics.admin_requests.labels(action, "ok").inc()
            return JSONResponse(
                {
                    "strategy": config.strategy,
                    "backends": pool.snapshot(),
                }
            )
        try:
            payload = await request.json()
            backend_id = payload["id"]
            url = payload["url"]
            if not isinstance(backend_id, str) or not isinstance(url, str):
                raise ValueError("id and url must be strings")
            item = BackendConfig(backend_id, url)
        except (KeyError, TypeError, ValueError, ConfigError, json.JSONDecodeError):
            metrics.admin_requests.labels(action, "bad_request").inc()
            return JSONResponse(
                {"error": "body must contain string id and http(s) url"},
                status_code=400,
            )
        if any(backend.id == item.id for backend in pool.backends):
            metrics.admin_requests.labels(action, "conflict").inc()
            return JSONResponse({"error": "backend already exists"}, status_code=409)
        try:
            backend = await pool.add_backend(item)
        except ValueError as exc:
            metrics.admin_requests.labels(action, "conflict").inc()
            return JSONResponse({"error": str(exc)}, status_code=409)
        except Exception:
            metrics.admin_requests.labels(action, "error").inc()
            raise
        try:
            admission.add_backend(item.id)
            router.add_backend(backend)
        except Exception:
            if item.id in admission.backend_ids:
                admission.remove_backend(item.id)
            await pool.remove_backend(item.id)
            metrics.admin_requests.labels(action, "error").inc()
            raise
        metrics.prefix_registry_updates.labels("add").inc()
        metrics.admin_requests.labels(action, "ok").inc()
        _refresh_health_gauge(pool, metrics)
        proxy.sync_capacity_metrics()
        admin_log(request, action, "ok", f"id={item.id} url={item.url}")
        return JSONResponse(
            {"backend": backend.id, "url": backend.base_url, "healthy": backend.healthy},
            status_code=201,
        )

    async def admin_backend(request: Request) -> Response:
        action = "remove_backend"
        denied = admin_denied(request, action)
        if denied is not None:
            return denied
        backend_id = request.path_params["backend_id"]
        if not any(backend.id == backend_id for backend in pool.backends):
            metrics.admin_requests.labels(action, "not_found").inc()
            return JSONResponse({"error": "backend not found"}, status_code=404)
        if len(pool.backends) <= 1:
            metrics.admin_requests.labels(action, "conflict").inc()
            return JSONResponse({"error": "cannot remove the last backend"}, status_code=409)
        stats = admission.stats()
        if stats.active_by_backend.get(backend_id, 0) or stats.queued_by_backend.get(
            backend_id, 0
        ):
            metrics.admin_requests.labels(action, "conflict").inc()
            return JSONResponse(
                {"error": "backend still has active or queued requests"}, status_code=409
            )
        clients = detach_registry(backend_id)
        await close_clients(clients)
        metrics.prefix_registry_updates.labels("remove").inc()
        metrics.admin_requests.labels(action, "ok").inc()
        admin_log(request, action, "ok", f"id={backend_id}")
        return JSONResponse({"removed": backend_id})

    async def admin_drain(request: Request) -> Response:
        action = "drain_backend"
        denied = admin_denied(request, action)
        if denied is not None:
            return denied
        backend_id = request.path_params["backend_id"]
        if not any(backend.id == backend_id for backend in pool.backends):
            metrics.admin_requests.labels(action, "not_found").inc()
            return JSONResponse({"error": "backend not found"}, status_code=404)
        if len(pool.backends) <= 1:
            metrics.admin_requests.labels(action, "conflict").inc()
            return JSONResponse(
                {"error": "cannot drain the last backend"}, status_code=409
            )
        try:
            timeout_s = float(request.query_params.get("timeout_s", config.drain_timeout_s))
        except ValueError:
            metrics.admin_requests.labels(action, "bad_request").inc()
            return JSONResponse({"error": "timeout_s must be a number"}, status_code=400)
        if timeout_s <= 0 or timeout_s > 3600:
            metrics.admin_requests.labels(action, "bad_request").inc()
            return JSONResponse(
                {"error": "timeout_s must be between 0 and 3600"}, status_code=400
            )
        pool.begin_drain(backend_id)
        admission.mark_draining(backend_id)
        metrics.backend_draining.labels(backend_id).set(1)
        admin_log(request, action, "started", f"id={backend_id} timeout_s={timeout_s}")
        idle = await admission.wait_idle(backend_id, timeout_s)
        if not idle:
            stats = admission.stats()
            metrics.admin_requests.labels(action, "timeout").inc()
            admin_log(request, action, "timeout", f"id={backend_id}")
            return JSONResponse(
                {
                    "error": "drain timeout; backend remains draining",
                    "draining": True,
                    "active": stats.active_by_backend.get(backend_id, 0),
                    "queued": stats.queued_by_backend.get(backend_id, 0),
                },
                status_code=409,
            )
        clients = detach_registry(backend_id)
        await close_clients(clients)
        metrics.backend_draining.labels(backend_id).set(0)
        metrics.prefix_registry_updates.labels("remove").inc()
        metrics.admin_requests.labels(action, "ok").inc()
        admin_log(request, action, "ok", f"id={backend_id}")
        return JSONResponse({"drained": backend_id, "removed": True})

    async def admin_undrain(request: Request) -> Response:
        action = "undrain_backend"
        denied = admin_denied(request, action)
        if denied is not None:
            return denied
        backend_id = request.path_params["backend_id"]
        if not any(backend.id == backend_id for backend in pool.backends):
            metrics.admin_requests.labels(action, "not_found").inc()
            return JSONResponse({"error": "backend not found"}, status_code=404)
        pool.cancel_drain(backend_id)
        admission.cancel_draining(backend_id)
        metrics.backend_draining.labels(backend_id).set(0)
        metrics.admin_requests.labels(action, "ok").inc()
        admin_log(request, action, "ok", f"id={backend_id}")
        return JSONResponse({"draining": False, "backend": backend_id})

    async def admin_health(request: Request) -> Response:
        action = "health_check"
        denied = admin_denied(request, action)
        if denied is not None:
            return denied
        await pool.check_health()
        metrics.admin_requests.labels(action, "ok").inc()
        admin_log(request, action, "ok")
        return JSONResponse({"backends": pool.snapshot()})

    async def admin_cache(request: Request) -> Response:
        """Report llama.cpp slot cache counters, not prefix identities.

        ``/slots`` exposes token counts for the last request on each slot; it
        cannot say whether a particular prompt prefix is cached.  This endpoint
        is observability only and does not feed routing decisions.
        """
        action = "cache_status"
        denied = admin_denied(request, action)
        if denied is not None:
            return denied

        async def probe(backend) -> tuple[str, dict]:
            if backend.probe_client is None:
                return backend.id, {"error": "client not started"}
            try:
                async with asyncio.timeout(config.health.timeout_s):
                    response = await backend.probe_client.get("/slots")
                    payload = response.json()
            except (TimeoutError, httpx.HTTPError, ValueError) as exc:
                return backend.id, {"error": type(exc).__name__}
            if not isinstance(payload, list):
                return backend.id, {"error": "unexpected /slots payload"}
            slots = []
            for slot in payload[:16]:
                if not isinstance(slot, dict):
                    continue
                slots.append(
                    {
                        key: slot.get(key)
                        for key in (
                            "id",
                            "is_processing",
                            "n_ctx",
                            "n_prompt_tokens",
                            "n_prompt_tokens_processed",
                            "n_prompt_tokens_cache",
                        )
                    }
                )
            return backend.id, {
                "status_code": response.status_code,
                "slots": slots,
            }

        results = await asyncio.gather(*(probe(backend) for backend in pool.backends))
        metrics.admin_requests.labels(action, "ok").inc()
        admin_log(request, action, "ok")
        return JSONResponse(
            {
                "backends": dict(results),
                "note": "slot cache token counts only; not a prefix or KV-block lookup",
            }
        )

    app = Starlette(
        routes=[
            Route("/completion", completion, methods=["POST"]),
            Route("/v1/chat/completions", chat_completions, methods=["POST"]),
            Route("/v1/completions", v1_completions, methods=["POST"]),
            Route("/v1/embeddings", v1_embeddings, methods=["POST"]),
            Route("/v1/models", v1_models, methods=["GET"]),
            Route("/health", health, methods=["GET"]),
            Route("/metrics", metrics_endpoint, methods=["GET"]),
            Route("/admin/backends", admin_backends, methods=["GET", "POST"]),
            Route("/admin/backends/{backend_id}", admin_backend, methods=["DELETE"]),
            Route(
                "/admin/backends/{backend_id}/drain",
                admin_drain,
                methods=["POST"],
            ),
            Route(
                "/admin/backends/{backend_id}/undrain",
                admin_undrain,
                methods=["POST"],
            ),
            Route("/admin/health/check", admin_health, methods=["POST"]),
            Route("/admin/cache", admin_cache, methods=["GET"]),
        ],
        middleware=[Middleware(RequestIDMiddleware)],
        lifespan=lifespan,
    )
    app.state.config = config
    app.state.pool = pool
    app.state.router = router
    app.state.metrics = metrics
    app.state.proxy = proxy
    app.state.admission = admission
    return app


def _refresh_health_gauge(pool: BackendPool, metrics: Metrics) -> None:
    for backend in pool.backends:
        if backend.healthy is not None:
            metrics.backend_health.labels(backend.id).set(1 if backend.healthy else 0)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Agent-aware inference gateway")
    parser.add_argument("--config", required=True, help="path to gateway TOML config")
    parser.add_argument("--host", help="override server host")
    parser.add_argument("--port", type=int, help="override server port")
    parser.add_argument(
        "--strategy",
        choices=list(STRATEGIES),
        help="override routing strategy",
    )
    parser.add_argument("--max-in-flight", type=int, help="override admission capacity")
    parser.add_argument("--max-queue-size", type=int, help="override admission queue bound")
    parser.add_argument(
        "--queue-timeout-ms", type=int, help="override admission queue timeout"
    )
    parser.add_argument(
        "--no-admission",
        action="store_true",
        help="disable bounded admission (benchmark baseline only)",
    )
    parser.add_argument(
        "--request-timeout-ms", type=int, help="override total request deadline"
    )
    parser.add_argument(
        "--execution-timeout-ms", type=int, help="override execution deadline"
    )
    parser.add_argument("--max-retries", type=int, help="override safe retry limit")
    parser.add_argument(
        "--health-interval-s", type=float, help="override health probe interval"
    )
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    env_token = os.environ.get("GATEWAY_ADMIN_TOKEN")
    if env_token:
        config = dataclasses.replace(
            config, admin=dataclasses.replace(config.admin, token=env_token)
        )
    overrides = {}
    if args.host:
        overrides["host"] = args.host
    if args.port:
        overrides["port"] = args.port
    if args.strategy:
        overrides["strategy"] = args.strategy
    if args.health_interval_s is not None:
        overrides["health_interval_s"] = args.health_interval_s

    admission_overrides = {}
    if args.max_in_flight is not None:
        admission_overrides["max_in_flight"] = args.max_in_flight
    if args.max_queue_size is not None:
        admission_overrides["max_queue_size"] = args.max_queue_size
    if args.queue_timeout_ms is not None:
        admission_overrides["queue_timeout_ms"] = args.queue_timeout_ms
    if args.no_admission:
        admission_overrides["enabled"] = False
    if admission_overrides:
        overrides["admission"] = dataclasses.replace(
            config.admission, **admission_overrides
        )
    deadline_overrides = {}
    if args.request_timeout_ms is not None:
        deadline_overrides["request_timeout_ms"] = args.request_timeout_ms
    if args.execution_timeout_ms is not None:
        deadline_overrides["execution_timeout_ms"] = args.execution_timeout_ms
    if deadline_overrides:
        overrides["deadline"] = dataclasses.replace(
            config.deadline, **deadline_overrides
        )
    if args.max_retries is not None:
        overrides["retry"] = dataclasses.replace(
            config.retry, max_retries=args.max_retries
        )
    if overrides:
        config = dataclasses.replace(config, **overrides)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format=(
            "%(asctime)s %(levelname)s %(name)s "
            "request_id=%(request_id)s %(message)s"
        ),
    )
    for handler in logging.getLogger().handlers:
        handler.addFilter(RequestIDFilter())
    import uvicorn

    uvicorn.run(
        create_app(config),
        host=config.host,
        port=config.port,
        log_level=args.log_level,
        log_config=None,
    )


if __name__ == "__main__":
    main()
