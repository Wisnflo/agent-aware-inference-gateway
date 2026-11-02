# Engineering Review Guide

This file is a study guide, not evidence that the project author has mastered
the code. Work through each question, run the verification command, and be able
to modify the code without assistance.

Source paths are relative to `gateway/`.

## 1. asyncio cancellation semantics

**Question:** What happens when a client disconnects while the gateway is
waiting for upstream headers or streaming SSE? Which coroutine sees
`CancelledError`, and where is capacity released?

**Where:** `src/gateway/proxy.py` (`_send_with_disconnect`, `_stream`,
`_FinalizingStreamingResponse`, `_finalize_stream`), `src/gateway/admission.py`
(`acquire`, `release`).

**Correctness basis:** `send_task.cancel()` stops the httpx send; the response
finalizer runs exactly once; `AdmissionTicket.release` is idempotent. Tests
assert `active=0` after disconnect.

**Verify:** `uv run pytest -q tests/integration/test_reliability_http.py -k "disconnect or streaming_deadline"`.

**Small task:** Add a cancellation test where the client disconnects before the
fake backend calls `started.set()`. Explain why the outcome is still `499`.

## 2. SSE framing across arbitrary HTTP chunks

**Question:** Why can the gateway not treat one HTTP chunk as one SSE event?
How does the decoder handle CRLF split across chunks?

**Where:** `src/gateway/sse.py`, `src/gateway/proxy.py::_stream`.

**Correctness basis:** `SSEEventDecoder` buffers until a blank-line boundary;
tests feed one byte at a time and split CRLF between chunks.

**Verify:** `uv run pytest -q tests/unit/test_sse.py`.

**Small task:** Add a test for a mixed `\r\n`/`\n` stream and decide whether the
decoder should normalize it; write down the tradeoff.

## 3. Request and upstream lifecycle

**Question:** When is an upstream `httpx.Response` closed on each path:
non-stream success, non-stream error, SSE completion, SSE timeout, client
disconnect, and retry?

**Where:** `src/gateway/proxy.py::handle`, `_execute`, `_fail` equivalents,
`_finalize_stream`, `_aclose`.

**Correctness basis:** every exit path either calls `_aclose` or the streaming
finalizer; tests assert no in-flight capacity remains.

**Verify:** `uv run pytest -q tests/integration/test_fake_backend.py tests/integration/test_reliability_http.py`.

**Small task:** Add a logging counter for `aclose` calls and prove exactly one
close per request in the tests.

## 4. Admission controller invariants

**Question:** Why is `AdmissionTicket.release` idempotent? What prevents
`active_total > max_in_flight` or `queued_total > max_queue_size`?

**Where:** `src/gateway/admission.py` (`try_acquire`, `acquire`, `release`,
`_drain`, `stats`).

**Correctness basis:** all transitions are synchronous on the event loop; the
queue bound is checked before enqueue; `_drain` checks both global and
per-backend capacity; tests assert peaks and final zero.

**Verify:** `uv run pytest -q tests/unit/test_admission.py tests/integration/test_admission_http.py`.

**Small task:** Change `max_in_flight` at runtime (without restart) in a test
harness; explain why the current controller does not support it.

## 5. FIFO and cross-backend scheduling

**Question:** Why is FIFO per backend rather than globally strict? What
happens if the oldest waiter targets a saturated backend while another
backend has capacity?

**Where:** `src/gateway/admission.py::_drain`.

**Correctness basis:** `_drain` chooses the oldest eligible ticket; a saturated
backend's head is skipped so another backend can use global capacity. Tests
assert per-backend FIFO.

**Verify:** `uv run pytest -q tests/unit/test_admission.py -k fifo`.

**Small task:** Implement and test a strict global FIFO variant; compare
head-of-line blocking in the over-load benchmark.

## 6. Session affinity and rendezvous hashing

**Question:** Why use SHA-256 rendezvous instead of `hash(session) % n`? What
happens to session mappings when a third backend is added?

**Where:** `src/gateway/router.py`.

**Correctness basis:** golden mapping test; 500-session migration property
test; no Python hash randomization.

**Verify:** `uv run pytest -q tests/unit/test_router.py`.

**Small task:** Add a fourth backend to the test and verify that only sessions
won by the new backend move.

## 7. Cache benefit versus load imbalance

**Question:** Why can session affinity improve cache hits yet still be unfair?
What does the 108/72 backend distribution mean?

**Where:** `src/gateway/router.py`, `bench/run_bench.py`,
`research/phase-2/implementation/week-4-benchmark.md`.

**Correctness basis:** repeated B0/B1 runs with cold backends; per-backend
distribution recorded in raw JSON.

**Verify:** run the Week 1 sequential workload with `--repeat 5` and inspect
`summary.json` and `backend_counts`.

**Small task:** Add per-backend limits to `config.toml`, rerun the same workload,
and explain why the distribution changes but the cache benefit may not.

## 8. Deadline and retry safety boundaries

**Question:** Which failures are safe to retry, and which are not? Why does
the execution deadline not reset on retry?

**Where:** `src/gateway/proxy.py::_execute`, `_send_with_disconnect`;
`src/gateway/config.py` (`DeadlineConfig`, `RetryConfig`).

**Correctness basis:** raw TCP backend tests for connect-before-headers and
mid-stream close; metadata records retry counts; deadline is an absolute
timestamp.

**Verify:** `uv run pytest -q tests/integration/test_reliability_http.py -k "retry or close_before or close_mid"`.

**Small task:** Add a test where the replacement backend returns HTTP 500 and
confirm no further retry occurs.

## 9. Backend health recovery and failback

**Question:** Why do startup probes use threshold 1 while runtime uses
`failure_threshold`? What is the cost of immediate failback?

**Where:** `src/gateway/backends.py::_probe`, `_record_success`,
`_record_failure`; `src/gateway/router.py::decide`.

**Correctness basis:** debounce tests with a controllable fake `/health`; real
A stop/restart experiment in Week 4.

**Verify:** `uv run pytest -q tests/integration/test_reliability_http.py -k health`.

**Small task:** Change recovery to require three successful probes and measure
the recovery distribution again.

## 10. Prometheus metric semantics

**Question:** Which counters are request-level and which are attempt-level?
Why is a stream deadline counted as HTTP 200 plus a termination reason?

**Where:** `src/gateway/metrics.py`, `docs/metrics.md`,
`src/gateway/proxy.py::_finish`, `_finalize_stream`.

**Correctness basis:** `gateway_requests_total` increments once per request in
`_finish`; retry/admission counters are attempt-level; termination reason is
exclusive.

**Verify:** run a retry scenario and compare `gateway_requests_total` with
`gateway_retries_total` and `gateway_admission_admitted_total`.

**Small task:** Add a test that a retried request produces one `status=200`
request count but two admission counts.

## 11. Benchmark fairness

**Question:** What is held constant between B0 and B1, and why are completed-
only p50/p95 not overall performance?

**Where:** `bench/run_bench.py`, `bench/workload.py`,
`bench/run_admission_bench.py`.

**Correctness basis:** same prompts/order/backend args/concurrency; backend and
gateway restarted per run; rejected/timed-out requests reported separately.

**Verify:** inspect `raw.json` for one B0 and one B1 run and recompute cache hit
as `Σcache_n / Σ(cache_n+prompt_n)`.

**Small task:** Add a new workload that shares a system prefix across sessions;
explain why B1 and B2-like routing may differ.

## 12. Fault troubleshooting path

**Question:** A request returns 503 vs 429 vs 504 vs 499: which subsystem owns
each outcome, and what log/metric would you inspect first?

**Where:** `docs/metrics.md`, `docs/runbook.md`, `src/gateway/proxy.py`.

**Correctness basis:** status mapping table and tests for each path.

**Verify:** `uv run pytest -q tests/integration/test_reliability_http.py`.

**Small task:** Reproduce each status with the fake backend and write down the
exact metric delta.

## 13. Configuration boundaries

**Question:** Which settings are validated at startup, and what happens with a
negative queue, an unknown backend in `per_backend`, or a deadline of zero?

**Where:** `src/gateway/config.py`, `tests/unit/test_config.py`.

**Correctness basis:** eager dataclass validation and TOML parsing errors.

**Verify:** `uv run pytest -q tests/unit/test_config.py`.

**Small task:** Add a config case where `execution_timeout_ms` exceeds
`request_timeout_ms`; explain why total still wins.

## 14. Logging and privacy

**Question:** What is logged per request, and what is deliberately not logged?
How is a session referenced in logs?

**Where:** `src/gateway/proxy.py` logging calls, `src/gateway/router.py::session_fingerprint`.

**Correctness basis:** logs include route/backend/status/duration/fingerprint;
no body, prompt, session id or credentials.

**Verify:** run a request and inspect gateway stderr; grep the raw benchmark JSON
to confirm prompt text is not stored.

**Small task:** Add a log redaction test that asserts a known session id does
not appear in captured logs.
