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
PREFIX_AFFINITY = "prefix_affinity"
KV_CACHE_AWARE = "kv_cache_aware"
STRATEGIES = (
    ROUND_ROBIN,
    SESSION_AFFINITY,
    PREFIX_AFFINITY,
    KV_CACHE_AWARE,
)


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
class PrefixConfig:
    """Prefix hash trie settings.

    Chunks are character slices, not model tokens.  This is an approximation of
    llama.cpp's tokenizer-based prefix cache; cache-hit feedback is recorded
    separately and never inferred from a text match alone.

    ``ttl_seconds`` expires prefix/backend associations that have not been
    observed for that long.  ``0`` disables time-based expiry; the node bound
    and LRU eviction still apply.
    """

    chunk_chars: int = 128
    min_match_chars: int = 128
    max_nodes: int = 100_000
    ttl_seconds: float = 900.0
    tokenizer_url: str | None = None
    min_match_tokens: int = 16
    token_block_size: int = 16
    tokenizer_timeout_s: float = 1.0
    tokenizer_cache_entries: int = 4096
    load_overflow_threshold: int = 0

    def __post_init__(self) -> None:
        for key in ("chunk_chars", "min_match_chars", "max_nodes", "token_block_size"):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ConfigError(f"prefix.{key} must be an integer")
            if value < 1:
                raise ConfigError(f"prefix.{key} must be at least 1")
        if isinstance(self.load_overflow_threshold, bool) or not isinstance(
            self.load_overflow_threshold, int
        ):
            raise ConfigError("prefix.load_overflow_threshold must be an integer")
        if self.load_overflow_threshold < 0:
            raise ConfigError("prefix.load_overflow_threshold must not be negative")
        ttl = self.ttl_seconds
        if isinstance(ttl, bool) or not isinstance(ttl, (int, float)):
            raise ConfigError("prefix.ttl_seconds must be a number")
        if ttl < 0:
            raise ConfigError("prefix.ttl_seconds must not be negative")
        if self.tokenizer_url is not None:
            parsed = urlparse(self.tokenizer_url)
            if parsed.scheme not in ("http", "https") or not parsed.hostname:
                raise ConfigError(
                    "prefix.tokenizer_url must be an http(s) URL with a host"
                )
            if parsed.query or parsed.fragment:
                raise ConfigError(
                    "prefix.tokenizer_url must not contain query or fragment"
                )
            for key in ("min_match_tokens", "tokenizer_cache_entries"):
                value = getattr(self, key)
                if isinstance(value, bool) or not isinstance(value, int):
                    raise ConfigError(f"prefix.{key} must be an integer")
                if value < 1:
                    raise ConfigError(f"prefix.{key} must be at least 1")
            timeout = self.tokenizer_timeout_s
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                raise ConfigError("prefix.tokenizer_timeout_s must be a number")
            if timeout <= 0:
                raise ConfigError("prefix.tokenizer_timeout_s must be positive")


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
    timeout_s: float = 2.0

    def __post_init__(self) -> None:
        for key in ("failure_threshold", "success_threshold"):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ConfigError(f"health.{key} must be an integer")
            if value < 1:
                raise ConfigError(f"health.{key} must be at least 1")
        if isinstance(self.timeout_s, bool) or not isinstance(
            self.timeout_s, (int, float)
        ):
            raise ConfigError("health.timeout_s must be a number")
        if self.timeout_s <= 0:
            raise ConfigError("health.timeout_s must be positive")


@dataclass(frozen=True)
class AdminConfig:
    """Admin endpoint policy.

    The token is optional for localhost-only operation.  When set, every admin
    request must carry ``Authorization: Bearer <token>``.  Prefer the
    ``GATEWAY_ADMIN_TOKEN`` environment variable over storing it in TOML.
    """

    token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.token is not None:
            if not isinstance(self.token, str) or not self.token:
                raise ConfigError("admin.token must be a non-empty string when set")


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
    max_request_body_bytes: int = 1_048_576
    drain_timeout_s: float = 30.0
    shutdown_timeout_s: float = 10.0
    admission: AdmissionConfig = field(default_factory=AdmissionConfig)
    prefix: PrefixConfig = field(default_factory=PrefixConfig)
    deadline: DeadlineConfig = field(default_factory=DeadlineConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    health: HealthConfig = field(default_factory=HealthConfig)
    admin: AdminConfig = field(default_factory=AdminConfig)

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
        if isinstance(self.port, bool) or not isinstance(self.port, int):
            raise ConfigError("server port must be an integer")
        if not 1 <= self.port <= 65535:
            raise ConfigError(f"server port out of range: {self.port}")
        if self.health_interval_s <= 0:
            raise ConfigError("health_interval_s must be positive")
        if self.upstream_timeout_s <= 0:
            raise ConfigError("upstream_timeout_s must be positive")
        if self.upstream_connect_timeout_s <= 0:
            raise ConfigError("upstream_connect_timeout_s must be positive")
        if (
            isinstance(self.max_connections_per_backend, bool)
            or not isinstance(self.max_connections_per_backend, int)
            or self.max_connections_per_backend <= 0
        ):
            raise ConfigError("max_connections_per_backend must be a positive integer")
        if (
            isinstance(self.max_keepalive_connections, bool)
            or not isinstance(self.max_keepalive_connections, int)
            or self.max_keepalive_connections < 0
        ):
            raise ConfigError("max_keepalive_connections must be a non-negative integer")
        if self.max_keepalive_connections > self.max_connections_per_backend:
            raise ConfigError(
                "max_keepalive_connections must not exceed max_connections_per_backend"
            )
        if (
            isinstance(self.max_request_body_bytes, bool)
            or not isinstance(self.max_request_body_bytes, int)
            or self.max_request_body_bytes < 1
        ):
            raise ConfigError("max_request_body_bytes must be a positive integer")
        for key in ("drain_timeout_s", "shutdown_timeout_s"):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ConfigError(f"server.{key} must be a number")
            if value <= 0:
                raise ConfigError(f"server.{key} must be positive")
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

    def float_number(
        table: Mapping[str, Any], key: str, default: float, prefix: str
    ) -> float:
        value = table.get(key, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigError(f"{prefix}.{key} must be a number")
        return float(value)

    def server_integer(key: str, default: int) -> int:
        return integer(server, key, default, "server")

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

    prefix_data = data.get("prefix", {})
    if not isinstance(prefix_data, Mapping):
        raise ConfigError("[prefix] must be a table")
    unknown_prefix = set(prefix_data) - {
        "chunk_chars",
        "min_match_chars",
        "max_nodes",
        "ttl_seconds",
        "tokenizer_url",
        "min_match_tokens",
        "token_block_size",
        "tokenizer_timeout_s",
        "tokenizer_cache_entries",
        "load_overflow_threshold",
    }
    if unknown_prefix:
        raise ConfigError(
            "unknown prefix keys: " + ", ".join(sorted(unknown_prefix))
        )
    tokenizer_url = prefix_data.get("tokenizer_url")
    if tokenizer_url is not None and not isinstance(tokenizer_url, str):
        raise ConfigError("prefix.tokenizer_url must be a string")
    prefix = PrefixConfig(
        chunk_chars=integer(prefix_data, "chunk_chars", 128, "prefix"),
        min_match_chars=integer(
            prefix_data, "min_match_chars", 128, "prefix"
        ),
        max_nodes=integer(prefix_data, "max_nodes", 100000, "prefix"),
        ttl_seconds=float_number(prefix_data, "ttl_seconds", 900.0, "prefix"),
        tokenizer_url=tokenizer_url or None,
        min_match_tokens=integer(
            prefix_data, "min_match_tokens", 16, "prefix"
        ),
        token_block_size=integer(
            prefix_data, "token_block_size", 16, "prefix"
        ),
        tokenizer_timeout_s=float_number(
            prefix_data, "tokenizer_timeout_s", 1.0, "prefix"
        ),
        tokenizer_cache_entries=integer(
            prefix_data, "tokenizer_cache_entries", 4096, "prefix"
        ),
        load_overflow_threshold=integer(
            prefix_data, "load_overflow_threshold", 0, "prefix"
        ),
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
    unknown_health = set(health_data) - {
        "failure_threshold",
        "success_threshold",
        "timeout_s",
    }
    if unknown_health:
        raise ConfigError("unknown health keys: " + ", ".join(sorted(unknown_health)))
    health = HealthConfig(
        failure_threshold=integer(
            health_data, "failure_threshold", 2, "health"
        ),
        success_threshold=integer(health_data, "success_threshold", 2, "health"),
        timeout_s=float_number(health_data, "timeout_s", 2.0, "health"),
    )

    admin_data = data.get("admin", {})
    if not isinstance(admin_data, Mapping):
        raise ConfigError("[admin] must be a table")
    unknown_admin = set(admin_data) - {"token"}
    if unknown_admin:
        raise ConfigError("unknown admin keys: " + ", ".join(sorted(unknown_admin)))
    token = admin_data.get("token")
    if token is not None and not isinstance(token, str):
        raise ConfigError("admin.token must be a string")
    if token == "":
        token = None
    admin = AdminConfig(token=token)

    return GatewayConfig(
        backends=tuple(backends),
        host=str(server.get("host", "127.0.0.1")),
        port=server_integer("port", 18080),
        strategy=str(server.get("strategy", SESSION_AFFINITY)),
        health_interval_s=float(number(server, "health_interval_s", 5.0)),
        upstream_timeout_s=float(number(server, "upstream_timeout_s", 300.0)),
        upstream_connect_timeout_s=float(number(server, "upstream_connect_timeout_s", 5.0)),
        max_connections_per_backend=server_integer("max_connections_per_backend", 64),
        max_keepalive_connections=server_integer("max_keepalive_connections", 16),
        max_request_body_bytes=server_integer("max_request_body_bytes", 1_048_576),
        drain_timeout_s=float_number(server, "drain_timeout_s", 30.0, "server"),
        shutdown_timeout_s=float_number(
            server, "shutdown_timeout_s", 10.0, "server"
        ),
        admission=admission,
        prefix=prefix,
        deadline=deadline,
        retry=retry,
        health=health,
        admin=admin,
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
