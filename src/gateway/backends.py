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
from http.cookiejar import CookieJar, DefaultCookiePolicy
from typing import Callable

import httpx

from .config import BackendConfig, ConfigError, GatewayConfig

logger = logging.getLogger("gateway.backends")


HealthChangeCallback = Callable[["Backend", bool, str], None]
ProbeCallback = Callable[[str, bool], None]


class _NoCookiePolicy(DefaultCookiePolicy):
    """Inference clients must not retain state from other downstream clients."""

    def set_ok(self, cookie, request) -> bool:
        return False

    def return_ok(self, cookie, request) -> bool:
        return False


@dataclass
class Backend:
    id: str
    base_url: str
    client: httpx.AsyncClient | None = None
    # Health probes use a separate client so a saturated inference connection
    # pool cannot starve liveness checks (and vice versa).
    probe_client: httpx.AsyncClient | None = None
    healthy: bool | None = None
    draining: bool = False
    last_checked_at: float | None = None
    last_probe_duration_s: float | None = None
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
        """Unknown (never probed) backends are eligible; explicit False is not.

        A draining backend stays registered for its in-flight work but is never
        eligible for new routing.
        """
        return {
            backend.id
            for backend in self._backends
            if backend.healthy is not False and not backend.draining
        }

    def has_healthy(self) -> bool:
        return any(
            backend.healthy is not False and not backend.draining
            for backend in self._backends
        )

    def begin_drain(self, backend_id: str) -> None:
        backend = self.get(backend_id)
        backend.draining = True
        logger.info("backend %s is draining", backend_id)

    def cancel_drain(self, backend_id: str) -> None:
        backend = self.get(backend_id)
        backend.draining = False
        logger.info("backend %s drain cancelled", backend_id)

    def set_callbacks(
        self,
        on_health_change: HealthChangeCallback | None = None,
        on_probe: ProbeCallback | None = None,
    ) -> None:
        self.on_health_change = on_health_change
        self.on_probe = on_probe

    def _make_client(self, base_url: str) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=base_url,
            limits=httpx.Limits(
                max_connections=self.config.max_connections_per_backend,
                max_keepalive_connections=self.config.max_keepalive_connections,
            ),
            timeout=httpx.Timeout(
                connect=self.config.upstream_connect_timeout_s,
                read=self.config.upstream_timeout_s,
                write=self.config.upstream_timeout_s,
                pool=self.config.upstream_connect_timeout_s,
            ),
            follow_redirects=False,
            cookies=CookieJar(policy=_NoCookiePolicy()),
        )

    def _make_probe_client(self, base_url: str) -> httpx.AsyncClient:
        timeout = self.config.health.timeout_s
        return httpx.AsyncClient(
            base_url=base_url,
            limits=httpx.Limits(max_connections=2, max_keepalive_connections=1),
            timeout=httpx.Timeout(
                connect=timeout,
                read=timeout,
                write=timeout,
                pool=timeout,
            ),
            follow_redirects=False,
            cookies=CookieJar(policy=_NoCookiePolicy()),
        )

    def _attach_clients(self, backend: Backend) -> None:
        if backend.client is None:
            backend.client = self._make_client(backend.base_url)
        if backend.probe_client is None:
            backend.probe_client = self._make_probe_client(backend.base_url)

    async def start(self) -> None:
        for backend in self._backends:
            self._attach_clients(backend)
        await self.check_health(startup=True)
        self._health_task = asyncio.create_task(
            self._health_loop(), name="gateway-health-loop"
        )

    async def add_backend(self, item: BackendConfig) -> Backend:
        """Register a backend at runtime and probe it before returning."""
        if any(backend.id == item.id for backend in self._backends):
            raise ValueError(f"backend already exists: {item.id!r}")
        backend = Backend(id=item.id, base_url=item.base_url)
        self._attach_clients(backend)
        await self._probe(backend, startup=True)
        self._backends.append(backend)
        return backend

    def detach_backend(
        self, backend_id: str
    ) -> tuple[httpx.AsyncClient | None, httpx.AsyncClient | None]:
        """Remove a backend from the pool immediately and return its clients.

        The caller updates routing before awaiting ``aclose`` so no new request
        can select a backend that is absent from admission.
        """
        backend = self.get(backend_id)
        client = backend.client
        probe_client = backend.probe_client
        backend.client = None
        backend.probe_client = None
        self._backends = [item for item in self._backends if item.id != backend_id]
        return client, probe_client

    async def remove_backend(self, backend_id: str) -> None:
        """Remove a backend and close its clients (non-admin convenience)."""
        client, probe_client = self.detach_backend(backend_id)
        for item in (client, probe_client):
            if item is not None:
                await item.aclose()

    async def close(self) -> None:
        self._stop.set()
        if self._health_task is not None:
            self._health_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._health_task
            self._health_task = None
        for backend in self._backends:
            for item in (backend.client, backend.probe_client):
                if item is not None:
                    await item.aclose()
            backend.client = None
            backend.probe_client = None

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
        if backend.probe_client is None:
            return
        started = time.monotonic()
        success = False
        try:
            async with asyncio.timeout(self.config.health.timeout_s):
                response = await backend.probe_client.get("/health")
            success = response.status_code == 200
            backend.last_error = None if success else f"HTTP {response.status_code}"
        except (TimeoutError, httpx.HTTPError, OSError) as exc:
            backend.last_error = f"{type(exc).__name__}: {exc}"
        finally:
            backend.last_checked_at = time.monotonic()
            backend.last_probe_duration_s = backend.last_checked_at - started
            if backend.last_probe_duration_s > self.config.health.timeout_s:
                logger.warning("health probe for %s exceeded timeout", backend.id)
            if success:
                self._record_success(backend, startup=startup)
            else:
                self._record_failure(
                    backend,
                    backend.last_error or "health probe failed",
                    startup=startup,
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

    def mark_business_success(self, backend_id: str) -> None:
        """A successful request can restore an already-unhealthy backend.

        It deliberately does not reset consecutive probe failures while the
        backend is healthy: health removal remains driven by the probe/transport
        debounce, so a fast business response cannot mask a failing health
        endpoint mid-streak.  Recovery still requires success_threshold
        observations.
        """
        backend = self.get(backend_id)
        if backend.healthy is not False or backend.draining:
            return
        backend.consecutive_successes += 1
        if backend.consecutive_successes >= self.config.health.success_threshold:
            backend.healthy = True
            backend.restored_at = time.monotonic()
            backend.last_transition_at = backend.restored_at
            logger.info("backend %s marked healthy by business traffic", backend.id)
            self._notify_health(backend, True, "business")

    def snapshot(self) -> dict[str, dict[str, object]]:
        now = time.monotonic()
        result: dict[str, dict[str, object]] = {}
        for backend in self._backends:
            result[backend.id] = {
                "healthy": backend.healthy,
                "draining": backend.draining,
                "last_checked_s_ago": (
                    None
                    if backend.last_checked_at is None
                    else round(now - backend.last_checked_at, 3)
                ),
                "last_probe_duration_ms": (
                    None
                    if backend.last_probe_duration_s is None
                    else round(backend.last_probe_duration_s * 1000.0, 3)
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
