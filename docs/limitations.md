# Limitations

This document is deliberately conservative. A limitation listed here is not a
bug report; it is a boundary the project has not proven or does not intend to
cover.

## Routing and caching

- **No prefix-aware or KV-aware routing.** The routing key is `X-Session-ID`,
  never prompt text or KV-cache location. Prefix-aware routing exists in vLLM
  Router and was measured in Phase 2.1 to add nothing over session affinity for
  unique session prefixes.
- **Session affinity can concentrate load.** The 15-session regression maps
  9/6 to two backends, not 7.5/7.5. Per-backend admission caps can trade
  utilisation for isolation.
- **Immediate failback.** After a preferred backend recovers, the next request
  for that session immediately returns to it. This is deterministic and tested,
  but can lose prefix cache again and can create a recovery-time burst.
- **Queued requests are not proactively migrated on health removal.** A request
  queued for backend A before A is removed may still be admitted and attempted
  against A; a connection failure is then handled by the safe retry path. It
  does not bypass capacity, but it can add latency.

## Cancellation and streaming

- **Internal inference cancellation is not proven.** Fake/raw backends prove
  gateway task cancellation and upstream connection closure. Real llama.cpp
  runs show `llamacpp:requests_processing` returning to zero, but this does not
  prove that every internal KV/slot operation stopped.
- **SSE status is already committed after headers.** If an execution deadline
  or upstream error occurs after `200` headers were sent, the gateway can only
  terminate the stream; it cannot change the HTTP status. It never appends a
  synthetic `[DONE]`.
- **Request bodies are fully read before forwarding.** Only responses are
  streamed. Very large request bodies can consume gateway memory.
- **No per-request body-size limit.** The gateway relies on the ASGI server and
  deployment for that boundary.

## Reliability

- **Retry is intentionally narrow.** Only `ConnectError` / `ConnectTimeout`
  before request dispatch is retried. Read timeouts, protocol errors, upstream
  5xx and mid-stream failures are treated as unknown or non-retryable and are
  not retried automatically.
- **No circuit breaker.** Health is a per-backend debounced probe/passive
  failure state, not a full circuit breaker with half-open probes.
- **No Kubernetes/service discovery.** Backends are static configuration.
- **Health probes share the upstream HTTP client pool.** Under extreme
  connection-pool pressure a probe can be delayed; the current admission limits
  make this unlikely but it is not isolated by design.
- **No execution-level retry after headers.** A non-stream body read failure
  after headers returns 502; it is not retried because the request may already
  have executed.

## Metrics

- **Per-process metrics.** No multi-worker aggregation. Running uvicorn with
  multiple workers would require a Prometheus multiprocess setup.
- **Attempt-level vs request-level counters.** `gateway_retries_total`,
  `gateway_upstream_errors_total` and `gateway_admission_admitted_total` are
  attempt-level. `gateway_requests_total` is the final request result.
- **Stream deadline status.** A stream that times out after headers is counted
  in `gateway_requests_total{status="200"}` plus
  `gateway_stream_terminations_total{reason="deadline"}`; the status label is
  the HTTP status actually sent, not a synthetic failure code.
- **No per-session labels.** Session-level analysis must use benchmark raw
  records; sessions are not metric labels.

## Performance evidence

- **CPU 0.5B model.** All latency/throughput numbers are from a single Apple
  Silicon machine running Qwen2.5-0.5B-Instruct q4_0 on CPU. They must not be
  generalized to GPU serving or larger models.
- **Single-run real fault scenarios.** Deterministic fake/raw tests carry the
  precision claims; real fault experiments are repeated in Week 4 but still
  have a small sample and machine-specific health timing.
- **Completed-only latency.** Overload benchmark p50/p95 exclude rejected and
  timed-out requests by design; they are not overall user-perceived latency.
- **No claim of p95 improvement from admission.** Bounded admission shapes
  latency and exposes overload; it did not demonstrate throughput gains.

## Security and operations

- No authentication, multi-tenancy, TLS termination, quota or billing.
- No UI; operations are CLI and Prometheus.
- Logs contain a session fingerprint, not raw session ids or prompt text.
  Request bodies are never logged by the gateway.
