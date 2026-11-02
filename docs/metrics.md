# Metrics

Endpoint: `GET /metrics`, Prometheus text format.

All metric names are prefixed `gateway_`. Labels are deliberately bounded:

| Label | Values |
|---|---|
| `route` | `completion`, `chat_completions` |
| `backend` | configured backend id (`a`, `b`, …) |
| `status` | HTTP status code as a string |
| `kind` | `connect`, `timeout`, `http_status`, `stream`, `other` |

No session id, prompt text, request id, client address or header value is used
as a label. Session ids appear in logs only as an 8-hex SHA-256 fingerprint.

## Counters

| Metric | Definition | Increment point |
|---|---|---|
| `gateway_requests_total{route,status,backend}` | Proxy requests that reached a routing decision, labelled by the final status returned to the client. | Exactly once per request in `Proxy._finish`. |
| `gateway_upstream_errors_total{backend,kind}` | Failed upstream exchanges. | Once per failed exchange: connect error/timeout, read error, mid-stream error, or an upstream 5xx response. |
| `gateway_client_disconnects_total{route}` | Streaming responses whose client went away and whose response task was cancelled. | In the streaming generator's cleanup path. |
| `gateway_sse_events_total{backend}` | Complete SSE events observed on forwarded streams. | At stream end, by the number of events decoded for metrics. |
| `gateway_prompt_tokens_cached_total{backend}` | Sum of llama.cpp `timings.cache_n` (prompt tokens served from prefix cache). | When a timing object is observed (final SSE event or JSON body). |
| `gateway_prompt_tokens_fresh_total{backend}` | Sum of llama.cpp `timings.prompt_n` (tokens that required prefill). | Same. |
| `gateway_prompt_tokens_total{backend}` | Sum of `cache_n + prompt_n`. | Same. |
| `gateway_admission_admitted_total{backend}` | Requests granted execution capacity, immediately or after waiting. | Once per successful admission, before the upstream request is sent. |
| `gateway_admission_rejected_total{backend}` | Requests rejected because the bounded queue was full. | Once per `queue_full` rejection. |
| `gateway_admission_timed_out_total{backend}` | Requests that exceeded `queue_timeout_ms` while waiting. | Once per `queue_timeout` outcome. |
| `gateway_admission_cancelled_total{backend}` | Waiting requests cancelled or abandoned by the client before admission. | Once per waiting cancellation/disconnect. |

## Gauges

| Metric | Definition |
|---|---|
| `gateway_in_flight{backend}` | Admitted requests currently holding execution capacity for that backend. |
| `gateway_active_requests` | Admitted requests currently holding execution capacity (global). |
| `gateway_queue_depth` | Requests currently waiting in admission queues (global). |
| `gateway_queue_depth_backend{backend}` | Requests waiting in that backend's FIFO queue. |
| `gateway_admission_peak_in_flight` | Highest observed admitted count since process start. |
| `gateway_admission_peak_queue_depth` | Highest observed queue depth since process start. |
| `gateway_backend_health{backend}` | 1 if the last `/health` probe returned 200, 0 otherwise. Unset until the first probe completes. |

All capacity gauges are derived from `AdmissionController.stats()` by
`sync_capacity_metrics`, so they cannot drift from the authoritative counters.

## Histograms

| Metric | Definition | Buckets |
|---|---|---|
| `gateway_request_duration_seconds{route,backend}` | From the gateway receiving the request to the response ending (last streamed byte sent, or error returned). Includes queue wait. | 5 ms … 60 s |
| `gateway_upstream_duration_seconds{route,backend}` | From just before sending upstream to the upstream response being fully read. | 5 ms … 60 s |
| `gateway_ttft_seconds{backend}` | From receiving a streaming request to the first event containing non-empty generated content. Includes queue wait. | 5 ms … 60 s |
| `gateway_queue_wait_seconds{backend}` | From request arrival to admission. Observed for every admitted request; 0 for immediate admission. | 1 ms … 30 s |
| `gateway_backend_execution_seconds{route,backend}` | From admission to request end. Excludes queue wait. | 5 ms … 60 s |

### Queue, execution and total latency

- **Queue wait** = arrival → admitted.
- **Backend execution** = admitted → request end (upstream + forwarding).
- **Total request latency** = arrival → request end.

Total is approximately queue + execution + pre-admission routing/body-read
overhead. Rejected and timed-out requests have only `gateway_request_duration`
and `gateway_requests_total{status="429"}`; they intentionally do not appear in
queue-wait or execution histograms.

For non-streaming requests, upstream duration ends after `aread()`; for
streaming requests it ends after the last chunk is read. Request duration ends
after the response generator finishes, so it includes forwarding time to the
client.

### Gateway overhead estimate

`gateway_request_duration_seconds_sum - gateway_upstream_duration_seconds_sum`
divided by the request count over the same window is the benchmark's
"gateway overhead" estimate. It covers routing, header filtering, reading the
request body, the extra loopback hop, and forwarding the last chunk. It is an
estimate, not a pure function-level profile. `bench/run_bench.py` also runs a
direct-backend vs gateway probe for a second, client-side measurement.

## Cache hit ratio

```
cache_hit_pct = 100 * Σ cache_n / Σ (cache_n + prompt_n)
```

This matches the Phase 2.1 direct-backend definition, so values are comparable.
The denominator counts prompt tokens actually presented to the backend in that
run, not model context length.

Do not compare this ratio across different prompt lengths or different numbers
of cold sessions without also reporting the backend request distribution.

## Relationship to llama.cpp counters

llama.cpp exposes `llamacpp:prompt_tokens_total`,
`llamacpp:prompt_tokens_cached_total`, `llamacpp:tokens_predicted_total` and
`llamacpp:requests_processing` per process. The benchmark snapshots these
before/after each run.

Measured semantics in the pinned llama.cpp version (Week 1 benchmark):

- `gateway_prompt_tokens_cached_total` delta matched
  `llamacpp:prompt_tokens_cached_total` delta exactly (12,940 and 19,410 for
  the sequential B0/B1 runs).
- `llamacpp:prompt_tokens_total` counts **fresh prefill only**, so it matches
  `gateway_prompt_tokens_fresh_total` (13,480 / 7,010), not
  `gateway_prompt_tokens_total` (which adds cache hits: 26,420 in both).
- `llamacpp:tokens_predicted_total` matched the 60 generated tokens per run.

Therefore cache hit ratio must always be computed as
`cache_n / (cache_n + prompt_n)` and never as
`cached_total / llamacpp:prompt_tokens_total`; the latter would exceed 100%.
The benchmark stores both vantage points so this reconciliation is auditable.
`gateway_prompt_tokens_total` is a gateway-computed convenience counter with no
single llama.cpp counterpart.

## Admission status codes

| Condition | HTTP | `error.type` | Counted as |
|---|---|---|---|
| Queue full | 429 | `queue_full` | `gateway_admission_rejected_total`, `gateway_requests_total{status="429"}` |
| Queue wait timeout | 429 | `queue_timeout` | `gateway_admission_timed_out_total`, `gateway_requests_total{status="429"}` |
| Client gone while queued | 499 (not sent) | n/a | `gateway_admission_cancelled_total`, `gateway_requests_total{status="499"}` |

Both 429 responses carry `Retry-After: 1` and `error.retry_after_ms`. They never
increment `gateway_upstream_errors_total`, because no upstream exchange
occurred.

Admitted responses include `X-Gateway-Queue-Ms` for per-request queue-wait
inspection; it is not a metric label.

## Reliability metrics (Week 3)

| Metric | Type | Definition |
|---|---|---|
| `gateway_execution_timeouts_total{backend,kind}` | counter | Requests that hit a total/execution deadline. `kind` is `request_deadline` or `execution_timeout` (including connect/read phases). Queue-specific timeouts remain in `gateway_admission_timed_out_total`. |
| `gateway_client_cancelled_total{route,stage}` | counter | Requests cancelled by the client; `stage` is `queued`, `connecting`, `body` or `streaming`. |
| `gateway_upstream_disconnects_total{backend,stage}` | counter | Upstream transport/protocol failures; `stage` is `connect`, `body` or `stream`. |
| `gateway_stream_terminations_total{backend,reason}` | counter | Why an SSE stream ended: `completed`, `client_disconnect`, `upstream_error`, `deadline`. Exactly one reason per stream. |
| `gateway_health_checks_total{backend,result}` | counter | Health probe outcomes (`success`/`failure`). |
| `gateway_backend_removed_total{backend}` | counter | Health-transition removals (startup transitions excluded). |
| `gateway_backend_restored_total{backend}` | counter | Health-transition restorations (startup transitions excluded). |
| `gateway_routing_failovers_total{from_backend,to_backend}` | counter | Requests routed away from an unhealthy preferred backend before dispatch. |
| `gateway_retries_total{backend,reason}` | counter | Retry attempts started after a safe pre-dispatch connect failure. |
| `gateway_retry_results_total{from_backend,to_backend,result}` | counter | Replacement attempt outcome: `success`, `http_error`, `failed`, `rejected`, `timed_out`, `deadline`, `cancelled`, `no_backend`. |

### Deadline vs cancellation vs overload

| Outcome | HTTP | Primary counter |
|---|---|---|
| Queue full | 429 `queue_full` | `gateway_admission_rejected_total` |
| Queue wait timeout | 429 `queue_timeout` | `gateway_admission_timed_out_total` |
| Total request deadline while queued | 504 `request_deadline` | `gateway_execution_timeouts_total{kind="request_deadline"}` |
| Execution/connect deadline | 504 `execution_timeout` | `gateway_execution_timeouts_total{kind="execution_timeout"}` |
| Client cancellation | 499 / stream end | `gateway_client_cancelled_total` |
| No healthy backend | 503 `no_healthy_backend` | `gateway_requests_total{status="503",backend="none"}` |

`gateway_requests_total` counts exactly one final result per client request.
`gateway_retries_total` and `gateway_upstream_errors_total` are attempt-level:
one request can have a failed first attempt and a successful second attempt,
so those counters are not request counts. `gateway_admission_admitted_total`
is also attempt-level (one per acquired ticket, including a retry ticket).

### Cancellation evidence levels

- Fake-backend tests prove gateway task termination and upstream connection
  closure deterministically.
- Real llama.cpp runs additionally show `llamacpp:requests_processing`
  returning to zero after a client disconnect; this is supporting evidence, not
  proof of internal KV/slot cancellation.

## Intentional omissions

- No per-session latency or cache metric: that would create unbounded label
  cardinality. Session-level analysis belongs in benchmark raw data.
- No token-per-second gauge: throughput is computed from counters over a
  benchmark window, not from a scrape-time derivative.
- No prompt/body logging: request bodies may contain user data. The gateway
  logs only route, backend, status, duration and a session fingerprint.
