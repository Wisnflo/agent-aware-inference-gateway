# Architecture

Week 1 scope. Last updated 2026-11-02.

## 1. Request path

```
client
  |  POST /completion or /v1/chat/completions
  v
Starlette app (uvicorn, one event loop)
  |  read body, X-Session-ID
  v
Router (round_robin | session_affinity)
  |  Backend
  v
Admission controller (per-backend FIFO queue, global + per-backend caps)
  |  AdmittedTicket
  v
httpx.AsyncClient (one per backend, keep-alive pool)
  |  upstream request with body bytes and filtered headers
  v
llama-server A / B (identical model and context)
  |
  v
JSON response  OR  SSE stream forwarded chunk by chunk
```

Modules:

| Module | Responsibility |
|---|---|
| `config.py` | TOML parsing, eager validation of backends, ports, strategy, timeouts, admission |
| `admission.py` | bounded per-backend FIFO admission, lifecycle states, queue timeout |
| `router.py` | `RoundRobinRouter`, `SessionAffinityRouter` (rendezvous SHA-256) |
| `backends.py` | backend objects, HTTP client lifecycle, periodic health probes |
| `proxy.py` | request forwarding, admission integration, SSE streaming, error mapping, cleanup, metrics |
| `sse.py` | incremental event decoder that is safe across arbitrary chunk splits |
| `metrics.py` | bounded-label Prometheus metrics |
| `server.py` | ASGI app, lifespan, `/health`, `/metrics`, CLI |

## 2. Why this async stack

- **asyncio + Starlette**: the response generator is an async generator, so
  backpressure is expressed as `await send(...)` rather than a thread per
  connection. Starlette is a thin ASGI layer; there is no hidden buffering of
  streaming bodies.
- **uvicorn**: production-grade ASGI server that supports in-process test
  startup with a pre-bound socket, which lets the integration tests exercise
  real HTTP and SSE rather than an in-memory transport.
- **httpx.AsyncClient**: one client per backend gives connection pooling and
  keep-alive reuse; `send(..., stream=True)` + `aiter_raw()` exposes the
  upstream bytes without decompression or line aggregation.
- **No framework-specific proxy plugin**: the forwarding behavior is explicit
  and auditable, which is the point of the project.

Alternatives rejected: Flask/FastAPI sync workers (one thread per stream,
harder cancellation story), aiohttp (viable, but httpx is already used in the
test harness), raw `asyncio.start_server` (week 1 would become an HTTP parser
project).

## 3. Session routing stability

`session_affinity` maps `X-Session-ID` to a backend with rendezvous
(highest-random-weight) hashing:

```
score(session, backend) = int.from_bytes(sha256(backend_id + "\x00" + session)[:8], "big")
winner = argmax(score)
```

Properties:

- **Deterministic across processes**: SHA-256 instead of Python's `hash()`,
  which is randomised per process for strings.
- **Minimal migration**: adding or removing a backend only moves sessions that
  the affected backend used to win. Tests assert this over 500 sessions.
- **Explicit fallback**: no `X-Session-ID` → round-robin. The gateway never
  guesses a session from client address or prompt text.
- **Bounded label space**: the session id is never a metric label and never
  logged raw; logs carry an 8-hex fingerprint.

This is deliberately **not** prefix-aware routing. It does not inspect the
prompt or compute KV-cache locations. Prefix-aware routing exists in vLLM
Router and was measured in Phase 2.1 to add nothing over session affinity for
unique session prefixes.

## 3.1 Admission control (Week 2)

`admission.py` owns all execution capacity. It is a synchronous state machine
on the event loop: no lock is taken and no I/O is performed while mutating
state.

Lifecycle:

```
ARRIVED -> ADMITTED -> FINISHED
    |          +-> FAILED      (upstream error)
    |          +-> CANCELLED   (client disconnect during the stream)
    +-> QUEUED -> ADMITTED
          +-> REJECTED         (queue full)
          +-> TIMED_OUT        (queue_timeout_ms exceeded)
          +-> CANCELLED        (waiter cancelled / client gone)
```

Semantics:

- **Route first, then admit.** A request is assigned to a backend before it can
  queue, so waiting never changes session affinity. B0/B1 routing semantics are
  unchanged by admission. One consequence: B0's round-robin counter advances
  for rejected requests too; rejection is a completed routing decision, not a
  retry.
- **Global cap**: `active_total <= max_in_flight`.
- **Queue cap**: `queued_total <= max_queue_size`; the check happens before
  enqueue, so a full queue rejects immediately (`429 queue_full`).
- **Per-backend caps** are optional; each must be `<= max_in_flight`. The
  global cap is always authoritative. Without per-backend caps, one backend may
  use the whole global capacity; with caps, isolation is bought at the cost of
  leaving global slots idle when affinity concentrates load.
- **FIFO** is per target backend. When capacity frees, the oldest eligible
  waiter is admitted; if the oldest waiter's backend is still saturated, the
  next eligible waiter is considered. This avoids cross-backend head-of-line
  blocking while preserving FIFO within a backend.
- **Queue timeout** applies only while waiting; it does not consume execution
  capacity. `queue_timeout_ms = 0` means any request that cannot be admitted
  immediately times out.
- **Cancellation** removes a queued ticket synchronously. While a request is
  waiting, the proxy polls `Request.is_disconnected()` every 50 ms so a client
  that goes away does not hold a queue slot until timeout.
- **Execution capacity** is held from admission until the request truly ends:
  upstream body fully read for JSON, or streaming generator completion for
  SSE. Returning HTTP 200 or SSE headers does **not** release it.
- **Exactly-once release**: tickets are idempotent; release on an already
  finished ticket is a no-op. Streaming cleanup runs from both the generator's
  `finally` and a response-level finalizer, so a body iterator that never
  starts cannot leak capacity.
- **No retry after streaming starts**: the gateway never switches backends
  mid-response.

Status codes:

| Condition | Status | `error.type` |
|---|---|---|
| Queue full | `429` | `queue_full` |
| Queue wait timeout | `429` | `queue_timeout` |
| Client left while queued | `499` (not sent) | n/a |
| Upstream connect failure | `502` | `upstream_connect_error` |
| Upstream read timeout | `504` | `upstream_read_timeout` |

`--no-admission` disables the caps for benchmark baselines only; it is not the
default and is documented as such.

## 3.2 Deadlines and cancellation (Week 3)

Two monotonic absolute timestamps are computed once:

- `request_deadline = arrival + request_timeout_ms`;
- `execution_deadline = min(request_deadline, first_admission + execution_timeout_ms)`.

Neither is reset by queueing, retry or failover. Queue wait is bounded by
`min(queue_timeout_ms, request_deadline - now)`. The remaining budget is passed
to every connect attempt, body read and SSE response.

- Before upstream headers: a timeout returns `504` with
  `error.type=request_deadline|execution_timeout`, closes the upstream attempt,
  releases the ticket and records the timeout.
- Non-stream after headers: `aread()` is wrapped in the remaining budget and
  returns `504` on expiry.
- SSE: the response's entire send loop is wrapped in the remaining budget. If
  the deadline expires before headers are committed, a real `504` is sent. If
  headers were already committed, the stream ends without appending `[DONE]`
  or any synthetic success event; the termination reason is recorded.
- Queue timeout, total deadline and execution timeout are separate metrics.

Cancellation paths:

| Stage | Detection | Result |
|---|---|---|
| queued | `Request.is_disconnected` poll + admission cancellation | remove from queue, `499`, cancelled metric |
| awaiting headers | send task + disconnect poll | cancel send task, release ticket, `499` |
| SSE streaming | Starlette response cancellation | close upstream, release ticket, no `[DONE]` |
| non-stream body | task cancellation | close upstream, release ticket |
| slow client | `await send` backpressure; deadline wraps send loop | bounded upstream read-ahead, stream terminates at deadline |

Three levels are kept distinct in reporting: gateway task ended, HTTP connection
closed, and inference backend stopped. The fake-backend tests prove the first
two deterministically; real llama.cpp `requests_processing` is used as
supporting evidence for the third, not as proof of internal KV cancellation.

## 3.3 Health removal and recovery (Week 3)

- Startup probes use threshold 1 so a dead backend is never routed to.
- Runtime probes and passive transport failures use
  `health.failure_threshold` consecutive failures; recovery uses
  `health.success_threshold` consecutive successful `/health` probes.
- A business HTTP 5xx is request-specific and does not change health. Connect
  errors, read/protocol errors and failed probes do.
- The router receives the current healthy id set. B0 skips an unhealthy
  preferred backend; B1 keeps the stable preferred backend while healthy, falls
  back via rendezvous over the healthy subset, and fails back immediately when
  the preferred backend recovers. Both migrations can lose prefix cache.
- If no backend is healthy, the gateway returns `503 no_healthy_backend` rather
  than routing to a known-dead backend.

## 3.4 Retry boundaries (Week 3)

Only failures that occur before a connection is established are retried:

- `ConnectError` / `ConnectTimeout` -> safe to try another healthy backend,
  bounded by `max_retries` and the remaining execution deadline.
- Any error after request bytes may have been sent (read timeout, protocol
  error, upstream 5xx, stream failure) -> no automatic retry; the outcome is
  unknown and could duplicate work.
- Once response headers are received or any SSE content has been sent -> no
  retry or backend switch.

On retry the old admission ticket is released before acquiring the new
backend's ticket, so per-backend capacity is never bypassed and two tickets are
never held. The total/execution deadlines are not reset.

## 4. SSE forwarding and disconnect cleanup

- The proxy detects `text/event-stream` on the upstream response and returns a
  `StreamingResponse` whose generator iterates `upstream.aiter_raw()`.
- Each raw chunk is yielded immediately. The gateway never accumulates a full
  SSE response. A fake backend holds the second event until the test releases
  it; the test proves the first event reaches the client while the backend is
  still blocked.
- Event order is upstream order because chunks are forwarded in order. The
  gateway does not re-serialize events.
- The incremental decoder buffers only the current partial event for metrics.
  It handles LF, CRLF and CR line endings and event splits at any byte.
- **Termination**: upstream close ends the generator; `[DONE]` is forwarded as
  data, never generated by the gateway.
- **Client disconnect**: Starlette cancels the response task. The generator's
  `finally` and the response-level finalizer both call an idempotent cleanup
  that closes the upstream response and releases the admission ticket. The
  integration tests drop the client mid-stream and assert the upstream
  generator is closed and in-flight returns to zero.
- **No mid-stream failover**: after any byte is sent, switching backends would
  duplicate output. The gateway does not retry streams at all.
- **Slow consumer**: the generator yields each chunk and Starlette awaits
  `send`; no full-response buffering occurs. A raw TCP test with a small send
  buffer confirms the upstream cannot finish writing while the client is
  paused.

## 5. Upstream connection management

- One `httpx.AsyncClient` per backend, created in the lifespan and closed on
  shutdown.
- Pool limits: `max_connections_per_backend` (default 64) and
  `max_keepalive_connections` (default 16).
- Timeouts: connect 5 s, read/write 300 s (Week 1 only; there is no per-request
  deadline policy yet).
- `Accept-Encoding: identity` is forced upstream so SSE bytes are never
  compressed or transparently decoded.
- Hop-by-hop headers and `X-Session-ID` are stripped; the body is forwarded
  unchanged.
- Health probes use a 2 s timeout on the same client, every 5 s by default.

## 6. Metrics without double counting

- One `gateway_requests_total` increment per proxy request, in `_finish`, which
  is called exactly once: by the non-stream path, by the streaming finalizer,
  or by the pre-upstream failure/rejection path.
- Admission gauges are derived from the controller's authoritative counters via
  `sync_capacity_metrics`, not incremented ad hoc in the proxy.
- `gateway_admission_admitted_total` increments once per successful acquire;
  `rejected` / `timed_out` / `cancelled` increment on their exclusive error
  paths. No path increments more than one of them.
- `gateway_upstream_errors_total` increments once per failed exchange: transport
  errors at send time, read errors, mid-stream errors, or an upstream 5xx status
  observed on the response. Admission rejections do not use this counter.
- Token counters come from llama.cpp `timings` (`cache_n`, `prompt_n`) seen in
  the final SSE event or the JSON body; each timing object is counted once.
- `/metrics` scrapes and `/health` probes are not counted as proxy requests.

See `metrics.md` for definitions and denominators.

## 7. Benchmark fairness

- Same model file, same backend arguments, same ports, same prompt bytes, same
  request order per workload, concurrency 1, `n_predict=1`, temperature 0,
  seed 42.
- Backends are restarted before **every** strategy/repeat run, so prompt cache
  starts cold for both B0 and B1.
- The gateway is also restarted per run, so the round-robin counter starts at
  the same position and metrics do not carry over.
- Only `--strategy` changes between B0 and B1.
- B0 still receives `X-Session-ID`; the round-robin router ignores it. This
  keeps the request bytes identical.
- Raw per-request data and backend counter snapshots are stored; summaries are
  derived from raw records, not hand-entered.

## 8. Known limitations (Week 3)

- No circuit breaker, K8s service discovery, multi-worker metric aggregation,
  prefix-aware routing or KV-cache introspection.
- Retry is intentionally limited to pre-dispatch connect failures; read
  timeouts and unknown outcomes are never retried.
- A streaming timeout after headers were committed can only terminate the
  stream; the HTTP status remains 200.
- Real-backend cancellation evidence is limited to connection closure and
  `llamacpp:requests_processing` returning to zero. Internal KV/slot
  cancellation is not proven.
- Health recovery is immediate failback for session affinity; a recovered
  backend can attract sessions and lose prefix cache again.
- The request body is fully read before forwarding; only responses stream.
- Absolute latency/throughput numbers are from a 0.5B CPU model and must not be
  extrapolated to GPU production serving.
