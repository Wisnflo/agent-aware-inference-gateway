"""Prometheus metrics with explicit, low-cardinality definitions.

Every label set is bounded:

* ``route`` is one of the two proxy routes, never a raw URL.
* ``backend`` is a configured backend id.
* ``status`` is an HTTP status code.
* No session id, prompt text, header value or client address is ever a label.
"""
from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"

DEFAULT_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
)
QUEUE_BUCKETS = (
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
)


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.requests_total = Counter(
            "gateway_requests_total",
            "Proxy requests that reached a routing decision, by final status.",
            ["route", "status", "backend"],
            registry=self.registry,
        )
        self.request_duration = Histogram(
            "gateway_request_duration_seconds",
            "Wall time from gateway receiving the request to the response ending.",
            ["route", "backend"],
            buckets=DEFAULT_BUCKETS,
            registry=self.registry,
        )
        self.upstream_duration = Histogram(
            "gateway_upstream_duration_seconds",
            "Wall time from sending upstream to the upstream response being fully read.",
            ["route", "backend"],
            buckets=DEFAULT_BUCKETS,
            registry=self.registry,
        )
        self.ttft = Histogram(
            "gateway_ttft_seconds",
            "Time from gateway receiving a streaming request to the first content event.",
            ["backend"],
            buckets=DEFAULT_BUCKETS,
            registry=self.registry,
        )
        self.in_flight = Gauge(
            "gateway_in_flight",
            "Admitted requests currently holding execution capacity, by backend.",
            ["backend"],
            registry=self.registry,
        )
        self.active_requests = Gauge(
            "gateway_active_requests",
            "Admitted requests currently holding execution capacity (global).",
            registry=self.registry,
        )
        self.queue_depth = Gauge(
            "gateway_queue_depth",
            "Requests currently waiting in admission queues (global).",
            registry=self.registry,
        )
        self.queue_depth_backend = Gauge(
            "gateway_queue_depth_backend",
            "Requests currently waiting in the admission queue for each backend.",
            ["backend"],
            registry=self.registry,
        )
        self.peak_in_flight = Gauge(
            "gateway_admission_peak_in_flight",
            "Highest observed admitted-request count since process start.",
            registry=self.registry,
        )
        self.peak_queue_depth = Gauge(
            "gateway_admission_peak_queue_depth",
            "Highest observed admission queue depth since process start.",
            registry=self.registry,
        )
        self.admission_admitted = Counter(
            "gateway_admission_admitted_total",
            "Requests granted execution capacity (immediately or after waiting).",
            ["backend"],
            registry=self.registry,
        )
        self.admission_rejected = Counter(
            "gateway_admission_rejected_total",
            "Requests rejected because the bounded admission queue was full.",
            ["backend"],
            registry=self.registry,
        )
        self.admission_timed_out = Counter(
            "gateway_admission_timed_out_total",
            "Requests that exceeded queue_timeout_ms while waiting for capacity.",
            ["backend"],
            registry=self.registry,
        )
        self.admission_cancelled = Counter(
            "gateway_admission_cancelled_total",
            "Waiting requests cancelled or abandoned by the client before admission.",
            ["backend"],
            registry=self.registry,
        )
        self.queue_wait = Histogram(
            "gateway_queue_wait_seconds",
            "Time from request arrival to admission; 0 for immediate admission.",
            ["backend"],
            buckets=QUEUE_BUCKETS,
            registry=self.registry,
        )
        self.backend_execution = Histogram(
            "gateway_backend_execution_seconds",
            "Time from admission to request end (excludes queue wait).",
            ["route", "backend"],
            buckets=DEFAULT_BUCKETS,
            registry=self.registry,
        )
        self.backend_health = Gauge(
            "gateway_backend_health",
            "1 when the last health probe succeeded, 0 when it failed.",
            ["backend"],
            registry=self.registry,
        )
        self.upstream_errors = Counter(
            "gateway_upstream_errors_total",
            "Upstream failures by class (connect, timeout, http_status, stream).",
            ["backend", "kind"],
            registry=self.registry,
        )
        self.client_disconnects = Counter(
            "gateway_client_disconnects_total",
            "Streaming responses cancelled because the client went away.",
            ["route"],
            registry=self.registry,
        )
        self.client_cancelled = Counter(
            "gateway_client_cancelled_total",
            "Requests cancelled by the client, by lifecycle stage.",
            ["route", "stage"],
            registry=self.registry,
        )
        self.execution_timeouts = Counter(
            "gateway_execution_timeouts_total",
            "Requests that hit an execution or total deadline before completing.",
            ["backend", "kind"],
            registry=self.registry,
        )
        self.upstream_disconnects = Counter(
            "gateway_upstream_disconnects_total",
            "Upstream disconnects or protocol errors, by stage.",
            ["backend", "stage"],
            registry=self.registry,
        )
        self.stream_terminations = Counter(
            "gateway_stream_terminations_total",
            "Why an SSE stream ended (completed, client_disconnect, upstream_error, deadline).",
            ["backend", "reason"],
            registry=self.registry,
        )
        self.health_checks = Counter(
            "gateway_health_checks_total",
            "Health probe outcomes.",
            ["backend", "result"],
            registry=self.registry,
        )
        self.backend_removed = Counter(
            "gateway_backend_removed_total",
            "Backends removed from routing by the health state machine.",
            ["backend"],
            registry=self.registry,
        )
        self.backend_restored = Counter(
            "gateway_backend_restored_total",
            "Backends restored to routing after consecutive successful probes.",
            ["backend"],
            registry=self.registry,
        )
        self.routing_failovers = Counter(
            "gateway_routing_failovers_total",
            "Requests routed away from an unhealthy preferred backend.",
            ["from_backend", "to_backend"],
            registry=self.registry,
        )
        self.retries = Counter(
            "gateway_retries_total",
            "Retries started after a safe pre-dispatch connection failure.",
            ["backend", "reason"],
            registry=self.registry,
        )
        self.retry_results = Counter(
            "gateway_retry_results_total",
            "Outcome of a retry attempt on the replacement backend.",
            ["from_backend", "to_backend", "result"],
            registry=self.registry,
        )
        self.sse_events = Counter(
            "gateway_sse_events_total",
            "Complete SSE events observed while streaming.",
            ["backend"],
            registry=self.registry,
        )
        self.prompt_tokens_cached = Counter(
            "gateway_prompt_tokens_cached_total",
            "Prompt tokens reported as cache hits in llama.cpp timings.",
            ["backend"],
            registry=self.registry,
        )
        self.prompt_tokens_fresh = Counter(
            "gateway_prompt_tokens_fresh_total",
            "Prompt tokens that required fresh prefill in llama.cpp timings.",
            ["backend"],
            registry=self.registry,
        )
        self.prompt_tokens = Counter(
            "gateway_prompt_tokens_total",
            "Prompt tokens seen in llama.cpp timings (cache hits plus fresh prefill).",
            ["backend"],
            registry=self.registry,
        )

    def observe_usage(self, backend_id: str, cached: int, fresh: int) -> None:
        if cached:
            self.prompt_tokens_cached.labels(backend_id).inc(cached)
        if fresh:
            self.prompt_tokens_fresh.labels(backend_id).inc(fresh)
        if cached or fresh:
            self.prompt_tokens.labels(backend_id).inc(cached + fresh)

    def render(self) -> bytes:
        return generate_latest(self.registry)
