"""ASGI application and command-line entrypoint."""
from __future__ import annotations

import argparse
import dataclasses
import logging
from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from .admission import AdmissionController
from .backends import BackendPool
from .config import GatewayConfig, load_config
from .metrics import CONTENT_TYPE_LATEST, Metrics
from .proxy import Proxy
from .router import make_router

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

    router = make_router(config.strategy, pool.backends)
    admission = AdmissionController(
        config.admission, [backend.id for backend in pool.backends]
    )
    proxy = Proxy(
        config=config,
        pool=pool,
        router=router,
        metrics=metrics,
        admission=admission,
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
            await pool.close()
            logger.info("gateway stopped")

    async def completion(request: Request) -> Response:
        return await proxy.handle(request, route="completion")

    async def chat_completions(request: Request) -> Response:
        return await proxy.handle(request, route="chat_completions")

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
        return Response(metrics.render(), media_type=CONTENT_TYPE_LATEST)

    app = Starlette(
        routes=[
            Route("/completion", completion, methods=["POST"]),
            Route("/v1/chat/completions", chat_completions, methods=["POST"]),
            Route("/health", health, methods=["GET"]),
            Route("/metrics", metrics_endpoint, methods=["GET"]),
        ],
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
        choices=["round_robin", "session_affinity"],
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
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    import uvicorn

    uvicorn.run(
        create_app(config),
        host=config.host,
        port=config.port,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
