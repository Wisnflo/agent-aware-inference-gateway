"""Gateway configuration loading and validation.

Configuration is a TOML file.  Validation happens eagerly at startup so a
misconfigured pool fails before the server accepts traffic.
"""
from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

ROUND_ROBIN = "round_robin"
SESSION_AFFINITY = "session_affinity"
STRATEGIES = (ROUND_ROBIN, SESSION_AFFINITY)


class ConfigError(ValueError):
    """Raised when the configuration file is missing or invalid."""


@dataclass(frozen=True)
class BackendConfig:
    id: str
    url: str

    def __post_init__(self) -> None:
        if not self.id or not self.id.strip():
            raise ConfigError("backend id must be a non-empty string")
        if any(ch.isspace() for ch in self.id):
            raise ConfigError(f"backend id must not contain whitespace: {self.id!r}")
        parsed = urlparse(self.url)
        if parsed.scheme not in ("http", "https"):
            raise ConfigError(
                f"backend {self.id!r} url must start with http:// or https://: {self.url!r}"
            )
        if not parsed.hostname:
            raise ConfigError(f"backend {self.id!r} url has no host: {self.url!r}")
        if parsed.query or parsed.fragment:
            raise ConfigError(
                f"backend {self.id!r} url must not contain query or fragment: {self.url!r}"
            )

    @property
    def base_url(self) -> str:
        return self.url.rstrip("/")


@dataclass(frozen=True)
class AdmissionConfig:
    """Bounded admission settings.

    ``per_backend`` is a tuple of ``(backend_id, limit)`` pairs rather than a
    dict so the frozen dataclass stays hashable and deterministic.
    """

    enabled: bool = True
    max_in_flight: int = 64
    max_queue_size: int = 128
    queue_timeout_ms: int = 2000
    per_backend: tuple[tuple[str, int], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ConfigError("admission.enabled must be a boolean")
        for key in ("max_in_flight", "max_queue_size", "queue_timeout_ms"):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ConfigError(f"admission.{key} must be an integer")
        if self.max_in_flight < 1:
            raise ConfigError("admission.max_in_flight must be at least 1")
        if self.max_queue_size < 0:
            raise ConfigError("admission.max_queue_size must not be negative")
        if self.queue_timeout_ms < 0:
            raise ConfigError("admission.queue_timeout_ms must not be negative")
        seen: set[str] = set()
        for backend_id, limit in self.per_backend:
            if not backend_id:
                raise ConfigError("admission.per_backend keys must be non-empty")
            if backend_id in seen:
                raise ConfigError(
                    f"duplicate admission.per_backend entry: {backend_id!r}"
                )
            seen.add(backend_id)
            if isinstance(limit, bool) or not isinstance(limit, int):
                raise ConfigError(
                    f"admission.per_backend.{backend_id} must be an integer"
                )
            if limit < 1:
                raise ConfigError(
                    f"admission.per_backend.{backend_id} must be at least 1"
                )

    def limit_for(self, backend_id: str) -> int:
        for candidate, limit in self.per_backend:
            if candidate == backend_id:
                return limit
        return self.max_in_flight

    @property
    def per_backend_map(self) -> dict[str, int]:
        return dict(self.per_backend)


@dataclass(frozen=True)
class DeadlineConfig:
    """Request-level budgets, all measured on the monotonic clock.

    ``request_timeout_ms`` covers arrival -> response end, including routing,
    queue wait, retries and streaming.  ``execution_timeout_ms`` bounds the
    time from first admission to request end; it never extends the total.
    """

    request_timeout_ms: int = 60000
    execution_timeout_ms: int = 30000

    def __post_init__(self) -> None:
        for key in ("request_timeout_ms", "execution_timeout_ms"):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ConfigError(f"deadline.{key} must be an integer")
            if value < 1:
                raise ConfigError(f"deadline.{key} must be at least 1")


@dataclass(frozen=True)
class RetryConfig:
    """Only pre-dispatch connection failures are retried."""

    max_retries: int = 1

    def __post_init__(self) -> None:
        if isinstance(self.max_retries, bool) or not isinstance(self.max_retries, int):
            raise ConfigError("retry.max_retries must be an integer")
        if self.max_retries < 0:
            raise ConfigError("retry.max_retries must not be negative")


@dataclass(frozen=True)
class HealthConfig:
    failure_threshold: int = 2
    success_threshold: int = 2

    def __post_init__(self) -> None:
        for key in ("failure_threshold", "success_threshold"):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ConfigError(f"health.{key} must be an integer")
            if value < 1:
                raise ConfigError(f"health.{key} must be at least 1")


@dataclass(frozen=True)
class GatewayConfig:
    backends: tuple[BackendConfig, ...]
    host: str = "127.0.0.1"
    port: int = 18080
    strategy: str = SESSION_AFFINITY
    health_interval_s: float = 5.0
    upstream_timeout_s: float = 300.0
    upstream_connect_timeout_s: float = 5.0
    max_connections_per_backend: int = 64
    max_keepalive_connections: int = 16
    admission: AdmissionConfig = field(default_factory=AdmissionConfig)
    deadline: DeadlineConfig = field(default_factory=DeadlineConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    health: HealthConfig = field(default_factory=HealthConfig)

    def __post_init__(self) -> None:
        if not self.backends:
            raise ConfigError("at least one backend must be configured")
        seen: set[str] = set()
        for backend in self.backends:
            if backend.id in seen:
                raise ConfigError(f"duplicate backend id: {backend.id!r}")
            seen.add(backend.id)
        if self.strategy not in STRATEGIES:
            raise ConfigError(
                f"unknown strategy {self.strategy!r}; expected one of {', '.join(STRATEGIES)}"
            )
        if not 1 <= self.port <= 65535:
            raise ConfigError(f"server port out of range: {self.port}")
        if self.health_interval_s <= 0:
            raise ConfigError("health_interval_s must be positive")
        if self.upstream_timeout_s <= 0:
            raise ConfigError("upstream_timeout_s must be positive")
        if self.upstream_connect_timeout_s <= 0:
            raise ConfigError("upstream_connect_timeout_s must be positive")
        if self.max_connections_per_backend <= 0:
            raise ConfigError("max_connections_per_backend must be positive")
        for backend_id, limit in self.admission.per_backend:
            if backend_id not in seen:
                raise ConfigError(
                    f"admission.per_backend references unknown backend: {backend_id!r}"
                )
            if limit > self.admission.max_in_flight:
                raise ConfigError(
                    f"admission.per_backend.{backend_id}={limit} exceeds "
                    f"admission.max_in_flight={self.admission.max_in_flight}"
                )


def config_from_mapping(data: Mapping[str, Any]) -> GatewayConfig:
    """Build a validated config from a mapping (the parsed TOML document)."""
    if not isinstance(data, Mapping):
        raise ConfigError("configuration root must be a table")

    server = data.get("server", {})
    if not isinstance(server, Mapping):
        raise ConfigError("[server] must be a table")

    raw_backends = data.get("backends")
    if not isinstance(raw_backends, list) or not raw_backends:
        raise ConfigError("at least one [[backends]] entry is required")

    backends: list[BackendConfig] = []
    for index, item in enumerate(raw_backends):
        if not isinstance(item, Mapping):
            raise ConfigError(f"backends[{index}] must be a table")
        try:
            backends.append(BackendConfig(id=item["id"], url=item["url"]))
        except KeyError as exc:
            raise ConfigError(f"backends[{index}] is missing {exc.args[0]!r}") from None

    def number(table: Mapping[str, Any], key: str, default: Any) -> Any:
        value = table.get(key, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigError(f"server.{key} must be a number")
        return value

    admission_data = data.get("admission", {})
    if not isinstance(admission_data, Mapping):
        raise ConfigError("[admission] must be a table")
    allowed_admission = {
        "enabled",
        "max_in_flight",
        "max_queue_size",
        "queue_timeout_ms",
        "per_backend",
    }
    unknown_admission = set(admission_data) - allowed_admission
    if unknown_admission:
        raise ConfigError(
            "unknown admission keys: " + ", ".join(sorted(unknown_admission))
        )

    def integer(table: Mapping[str, Any], key: str, default: int, prefix: str) -> int:
        value = table.get(key, default)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigError(f"{prefix}.{key} must be an integer")
        return value

    enabled = admission_data.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ConfigError("admission.enabled must be a boolean")

    raw_per_backend = admission_data.get("per_backend", {})
    if not isinstance(raw_per_backend, Mapping):
        raise ConfigError("[admission.per_backend] must be a table")
    per_backend: list[tuple[str, int]] = []
    for backend_id, limit in raw_per_backend.items():
        if not isinstance(backend_id, str) or not backend_id:
            raise ConfigError("admission.per_backend keys must be non-empty strings")
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ConfigError(
                f"admission.per_backend.{backend_id} must be an integer"
            )
        per_backend.append((backend_id, limit))

    admission = AdmissionConfig(
        enabled=enabled,
        max_in_flight=integer(admission_data, "max_in_flight", 64, "admission"),
        max_queue_size=integer(admission_data, "max_queue_size", 128, "admission"),
        queue_timeout_ms=integer(
            admission_data, "queue_timeout_ms", 2000, "admission"
        ),
        per_backend=tuple(sorted(per_backend)),
    )

    deadline_data = data.get("deadline", {})
    if not isinstance(deadline_data, Mapping):
        raise ConfigError("[deadline] must be a table")
    unknown_deadline = set(deadline_data) - {
        "request_timeout_ms",
        "execution_timeout_ms",
    }
    if unknown_deadline:
        raise ConfigError(
            "unknown deadline keys: " + ", ".join(sorted(unknown_deadline))
        )
    deadline = DeadlineConfig(
        request_timeout_ms=integer(
            deadline_data, "request_timeout_ms", 60000, "deadline"
        ),
        execution_timeout_ms=integer(
            deadline_data, "execution_timeout_ms", 30000, "deadline"
        ),
    )

    retry_data = data.get("retry", {})
    if not isinstance(retry_data, Mapping):
        raise ConfigError("[retry] must be a table")
    unknown_retry = set(retry_data) - {"max_retries"}
    if unknown_retry:
        raise ConfigError("unknown retry keys: " + ", ".join(sorted(unknown_retry)))
    retry = RetryConfig(
        max_retries=integer(retry_data, "max_retries", 1, "retry")
    )

    health_data = data.get("health", {})
    if not isinstance(health_data, Mapping):
        raise ConfigError("[health] must be a table")
    unknown_health = set(health_data) - {"failure_threshold", "success_threshold"}
    if unknown_health:
        raise ConfigError("unknown health keys: " + ", ".join(sorted(unknown_health)))
    health = HealthConfig(
        failure_threshold=integer(
            health_data, "failure_threshold", 2, "health"
        ),
        success_threshold=integer(health_data, "success_threshold", 2, "health"),
    )

    return GatewayConfig(
        backends=tuple(backends),
        host=str(server.get("host", "127.0.0.1")),
        port=int(number(server, "port", 18080)),
        strategy=str(server.get("strategy", SESSION_AFFINITY)),
        health_interval_s=float(number(server, "health_interval_s", 5.0)),
        upstream_timeout_s=float(number(server, "upstream_timeout_s", 300.0)),
        upstream_connect_timeout_s=float(number(server, "upstream_connect_timeout_s", 5.0)),
        max_connections_per_backend=int(number(server, "max_connections_per_backend", 64)),
        max_keepalive_connections=int(number(server, "max_keepalive_connections", 16)),
        admission=admission,
        deadline=deadline,
        retry=retry,
        health=health,
    )


def load_config(path: str | Path) -> GatewayConfig:
    path = Path(path)
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {path}: {exc}") from exc
    return config_from_mapping(data)
