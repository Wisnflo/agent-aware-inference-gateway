"""Backend pool: HTTP clients, health debounce, removal and recovery.

Health model:

* startup probes use a threshold of 1 so a dead backend is never routed to;
* runtime probes and passive transport failures require
  ``health.failure_threshold`` consecutive failures to remove a backend;
* recovery requires ``health.success_threshold`` consecutive successful
  ``/health`` probes;
* a business HTTP 5xx is *not* treated as backend death (it may be
  request-specific).  Only transport failures and failed health probes count.
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Callable

import httpx

from .config import BackendConfig, ConfigError, GatewayConfig

logger = logging.getLogger("gateway.backends")

HEALTH_TIMEOUT_S = 2.0

HealthChangeCallback = Callable[["Backend", bool, str], None]
ProbeCallback = Callable[[str, bool], None]


@dataclass
class Backend:
    id: str
    base_url: str
    client: httpx.AsyncClient | None = None
    healthy: bool | None = None
    last_checked_at: float | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_transition_at: float | None = None
    removed_at: float | None = None
    restored_at: float | None = None

    def path(self, path: str) -> str:
        return f"{self.base_url}{path}"


@dataclass
class BackendPool:
    """Owns upstream HTTP clients, health probes and health state."""

    config: GatewayConfig
    on_health_change: HealthChangeCallback | None = None
    on_probe: ProbeCallback | None = None
    _backends: list[Backend] = field(init=False)
    _health_task: asyncio.Task[None] | None = field(init=False, default=None)
    _stop: asyncio.Event = field(init=False)

    def __post_init__(self) -> None:
        self._backends = [
            Backend(id=item.id, base_url=item.base_url) for item in self.config.backends
        ]
        self._stop = asyncio.Event()

    @property
    def backends(self) -> tuple[Backend, ...]:
        return tuple(self._backends)

    def get(self, backend_id: str) -> Backend:
        for backend in self._backends:
            if backend.id == backend_id:
                return backend
        raise KeyError(backend_id)

    def healthy_ids(self) -> set[str]:
        """Unknown (never probed) backends are eligible; explicit False is not."""
        return {backend.id for backend in self._backends if backend.healthy is not False}

    def has_healthy(self) -> bool:
        return any(backend.healthy is not False for backend in self._backends)

    def set_callbacks(
        self,
        on_health_change: HealthChangeCallback | None = None,
        on_probe: ProbeCallback | None = None,
    ) -> None:
        self.on_health_change = on_health_change
        self.on_probe = on_probe

    async def start(self) -> None:
        limits = httpx.Limits(
            max_connections=self.config.max_connections_per_backend,
            max_keepalive_connections=self.config.max_keepalive_connections,
        )
        timeout = httpx.Timeout(
            connect=self.config.upstream_connect_timeout_s,
            read=self.config.upstream_timeout_s,
            write=self.config.upstream_timeout_s,
            pool=self.config.upstream_connect_timeout_s,
        )
        for backend in self._backends:
            if backend.client is None:
                backend.client = httpx.AsyncClient(
                    base_url=backend.base_url,
                    limits=limits,
                    timeout=timeout,
                    follow_redirects=False,
                )
        await self.check_health(startup=True)
        self._health_task = asyncio.create_task(
            self._health_loop(), name="gateway-health-loop"
        )

    async def close(self) -> None:
        self._stop.set()
        if self._health_task is not None:
            self._health_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._health_task
            self._health_task = None
        for backend in self._backends:
            if backend.client is not None:
                await backend.client.aclose()
                backend.client = None

    async def _health_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.config.health_interval_s
                )
            except asyncio.TimeoutError:
                await self.check_health()

    async def check_health(self, startup: bool = False) -> None:
        await asyncio.gather(
            *(self._probe(backend, startup) for backend in self._backends)
        )

    async def _probe(self, backend: Backend, startup: bool) -> None:
        if backend.client is None:
            return
        started = time.monotonic()
        success = False
        try:
            response = await backend.client.get("/health", timeout=HEALTH_TIMEOUT_S)
            success = response.status_code == 200
            backend.last_error = None if success else f"HTTP {response.status_code}"
        except (httpx.HTTPError, OSError) as exc:
            backend.last_error = f"{type(exc).__name__}: {exc}"
        finally:
            backend.last_checked_at = time.monotonic()
            if backend.last_checked_at - started > HEALTH_TIMEOUT_S:
                logger.warning("health probe for %s exceeded timeout", backend.id)
            if success:
                self._record_success(backend, startup=startup)
            else:
                self._record_failure(
                    backend, backend.last_error or "health probe failed", startup=startup
                )
            if self.on_probe is not None:
                self.on_probe(backend.id, success)

    def _record_success(self, backend: Backend, *, startup: bool = False) -> None:
        backend.consecutive_successes += 1
        backend.consecutive_failures = 0
        threshold = 1 if startup else self.config.health.success_threshold
        if backend.healthy is not True and backend.consecutive_successes >= threshold:
            backend.healthy = True
            backend.restored_at = time.monotonic()
            backend.last_transition_at = backend.restored_at
            logger.info("backend %s marked healthy", backend.id)
            self._notify_health(backend, True, "startup" if startup else "probe")

    def _record_failure(
        self, backend: Backend, reason: str, *, startup: bool = False
    ) -> None:
        backend.consecutive_failures += 1
        backend.consecutive_successes = 0
        threshold = 1 if startup else self.config.health.failure_threshold
        if backend.healthy is not False and backend.consecutive_failures >= threshold:
            backend.healthy = False
            backend.removed_at = time.monotonic()
            backend.last_transition_at = backend.removed_at
            logger.warning("backend %s marked unhealthy: %s", backend.id, reason)
            self._notify_health(backend, False, "startup" if startup else "probe")

    def _notify_health(self, backend: Backend, healthy: bool, reason: str) -> None:
        if self.on_health_change is not None:
            self.on_health_change(backend, healthy, reason)

    def mark_failure(self, backend_id: str, reason: str) -> None:
        """Record a passive transport failure (debounced; called by the proxy)."""
        self._record_failure(self.get(backend_id), reason)

    def mark_success(self, backend_id: str) -> None:
        """Record a successful health observation (used by probes/tests)."""
        self._record_success(self.get(backend_id))

    def snapshot(self) -> dict[str, dict[str, object]]:
        now = time.monotonic()
        result: dict[str, dict[str, object]] = {}
        for backend in self._backends:
            result[backend.id] = {
                "healthy": backend.healthy,
                "last_checked_s_ago": (
                    None
                    if backend.last_checked_at is None
                    else round(now - backend.last_checked_at, 3)
                ),
                "last_error": backend.last_error,
                "consecutive_failures": backend.consecutive_failures,
                "consecutive_successes": backend.consecutive_successes,
                "last_transition_s_ago": (
                    None
                    if backend.last_transition_at is None
                    else round(now - backend.last_transition_at, 3)
                ),
            }
        return result


def pool_from_config(config: GatewayConfig) -> BackendPool:
    try:
        return BackendPool(config=config)
    except ConfigError:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        raise ConfigError(f"cannot build backend pool: {exc}") from exc
