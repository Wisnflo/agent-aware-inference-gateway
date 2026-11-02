# Agent-Aware Inference Gateway

A small, auditable Python asyncio gateway in front of two local
[llama.cpp](https://github.com/ggml-org/llama.cpp) `llama-server` processes.
It exposes llama.cpp's native `/completion` and an OpenAI-compatible
`/v1/chat/completions` passthrough, supports JSON and SSE, and implements
bounded admission, request/execution deadlines, health-aware routing and a
deliberately conservative retry policy.

This is **not** a new routing algorithm. The routing design follows mature
routers such as [vLLM Router](https://github.com/vllm-project/production-stack).
The gateway is an independent local implementation of the request lifecycle,
SSE forwarding, admission, health and observability parts that are relevant to
this project.

## Scope

Implemented and tested:

- HTTP and SSE proxying with incremental forwarding.
- Round-robin and session-affinity routing (SHA-256 rendezvous hash of
  `X-Session-ID`).
- Two static llama-server backends with connection reuse.
- Bounded admission: global/per-backend in-flight caps, bounded per-backend
  FIFO wait queue, queue timeout, cancellation removal.
- Whole-request and execution deadlines on a monotonic clock.
- Client disconnect cleanup, including mid-SSE.
- Safe retry only for pre-dispatch connect failures.
- Health debounce, removal, recovery and health-aware routing.
- Prometheus metrics and deterministic fault-injection tests.

Not implemented: prefix/KV-aware routing, circuit breaker, Kubernetes service
discovery, C++ core, multi-tenant auth/billing, frontend, production GPU
serving. See [docs/limitations.md](docs/limitations.md).

## Architecture

```
client
  |
  v
Starlette ASGI app
  |  request body + X-Session-ID
  v
health-aware router
  |  preferred backend, healthy fallback
  v
admission controller
  |  global/per-backend caps, bounded FIFO queue, deadline
  v
httpx.AsyncClient (one keep-alive pool per backend)
  |
  +--> llama-server A (127.0.0.1:18090)
  +--> llama-server B (127.0.0.1:18091)
  |
  v
JSON response or SSE stream forwarded chunk by chunk
```

Detailed design: [docs/architecture.md](docs/architecture.md). Metrics:
[docs/metrics.md](docs/metrics.md). Operations: [docs/runbook.md](docs/runbook.md).
Study guide: [docs/engineering-review.md](docs/engineering-review.md).

## Quick start

Requirements: Python ≥ 3.11 (reference: 3.13), [uv](https://docs.astral.sh/uv/),
CMake, a llama.cpp build with `llama-server`, and a GGUF model.

### 1. Build llama.cpp

```bash
git clone --filter=blob:none https://github.com/ggml-org/llama.cpp.git
cd llama.cpp
git checkout 785322d17f2d51e5185cf996a1f4e46d87640841
cmake -B build -DLLAMA_BUILD_SERVER=ON -DGGML_METAL=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build --target llama-server -j 8
```

### 2. Get the reference model

Reference model: `Qwen/Qwen2.5-0.5B-Instruct-GGUF`, `q4_0`, 428,730,208 bytes,
SHA-256 `7671c0c304e6ce5a7fc577bcb12aba01e2c155cc2efd29b2213c95b18edaf6ed`.

```bash
mkdir -p "$HOME/gateway-assets/models"
curl -L --fail \
  -o "$HOME/gateway-assets/models/qwen2.5-0.5b-instruct-q4_0.gguf" \
  'https://modelscope.cn/api/v1/models/Qwen/Qwen2.5-0.5B-Instruct-GGUF/repo?Revision=master&FilePath=qwen2.5-0.5b-instruct-q4_0.gguf'
shasum -a 256 "$HOME/gateway-assets/models/qwen2.5-0.5b-instruct-q4_0.gguf"
```

### 3. Install the gateway

```bash
cd gateway
uv sync
cp config.example.toml config.toml
```

### 4. Start backends and gateway

```bash
export GATEWAY_WORKSPACE="$HOME/gateway-assets"
export LLAMA_SERVER="$GATEWAY_WORKSPACE/llama.cpp/build/bin/llama-server"
export GATEWAY_MODEL="$GATEWAY_WORKSPACE/models/qwen2.5-0.5b-instruct-q4_0.gguf"

./scripts/start_backends.sh
# expected:
# started backend 18090 pid=...
# started backend 18091 pid=...
# backend 18090 healthy
# backend 18091 healthy

uv run python -m gateway --config config.toml --strategy session_affinity
# expected: gateway started strategy=session_affinity backends=['a', 'b']
```

The gateway runs in the foreground; stop it with Ctrl-C.

### 5. Send requests

Non-streaming:

```bash
curl -s http://127.0.0.1:18080/completion \
  -H 'Content-Type: application/json' \
  -H 'X-Session-ID: demo-session' \
  -d '{"prompt":"Say hello in one sentence.","n_predict":8,"stream":false,"cache_prompt":true}'
```

Streaming SSE:

```bash
curl -N http://127.0.0.1:18080/completion \
  -H 'Content-Type: application/json' \
  -H 'X-Session-ID: demo-session' \
  -d '{"prompt":"Say hello in one sentence.","n_predict":8,"stream":true,"cache_prompt":true}'
```

OpenAI-compatible:

```bash
curl -s http://127.0.0.1:18080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Say hello."}],"max_tokens":8}'
```

Responses include `X-Gateway-Backend: a|b`; admitted responses also include
`X-Gateway-Queue-Ms`. Same `X-Session-ID` maps to the same backend while it is
healthy.

### 6. Health and metrics

```bash
curl -s http://127.0.0.1:18080/health | python3 -m json.tool
curl -s http://127.0.0.1:18080/metrics | grep '^gateway_'
```

## Configuration

`[admission]`:

```toml
[admission]
enabled = true
max_in_flight = 8
max_queue_size = 16
queue_timeout_ms = 2000

[admission.per_backend]   # optional; each must be <= max_in_flight
a = 4
b = 4
```

`[deadline]`, `[retry]`, `[health]`:

```toml
[deadline]
request_timeout_ms = 60000
execution_timeout_ms = 30000

[retry]
max_retries = 1

[health]
failure_threshold = 2
success_threshold = 2
```

Requests are routed first, then admitted against the target backend. Without
`per_backend` entries, one backend may use the whole global capacity. With
caps, a saturated backend cannot occupy every slot, at the cost of lower
utilisation when session affinity concentrates load.

## Behaviour and statuses

| Condition | Status | `error.type` |
|---|---|---|
| Queue full | 429 | `queue_full` |
| Queue wait timeout | 429 | `queue_timeout` |
| Total deadline while queued | 504 | `request_deadline` |
| Execution/connect deadline | 504 | `execution_timeout` |
| No healthy backend | 503 | `no_healthy_backend` |
| Upstream connect failure (after retries) | 502 | `upstream_connect_error` |
| Client gone while queued/connecting | 499 (not sent) | n/a |

Reliability rules:

- Total and execution deadlines are fixed monotonic timestamps, never reset by
  retry or failover.
- Only `ConnectError` / `ConnectTimeout` before request dispatch is retried.
- Once response headers are received or SSE content is sent, no retry or
  backend switch occurs.
- Health removal requires consecutive failed probes; recovery requires
  consecutive successful probes. A single business 5xx does not remove a
  backend.
- Session affinity fails back immediately when the preferred backend recovers;
  this can lose prefix cache.

## Testing

```bash
uv run pytest -q
GATEWAY_REAL_BACKENDS=1 uv run pytest -q -m real_backend
```

The real-backend suite starts two llama-server processes on ports `18190/18191`
and uses `LLAMA_SERVER` / `GATEWAY_MODEL`. No paid API is used.

Test groups:

- `tests/unit`: configuration, routing, SSE decoder, admission controller.
- `tests/integration/test_fake_backend.py`: HTTP/SSE forwarding and Week 1
  behaviour with controllable fake backends.
- `tests/integration/test_admission_http.py`: capacity, FIFO, queue timeout,
  cancellation, metrics.
- `tests/integration/test_reliability_http.py`: raw TCP failures, deadlines,
  retry boundaries, health debounce, slow consumer, race tests.
- `tests/integration/test_real_backend.py`: real llama-server E2E.

## Reproducible benchmarks

All scripts restart backends and the gateway per run and write raw JSON plus a
summary. Run them sequentially; they share ports 18090/18091.

```bash
uv run python -m bench.run_bench --repeat 5 \
  --workloads sequential,shuffled,negative --strategies b0,b1 \
  --outdir bench/results/week4-route

uv run python -m bench.run_admission_bench --repeat 5 \
  --outdir bench/results/week4-admission

uv run python -m bench.run_reliability_bench --repeat 5 \
  --outdir bench/results/week4-reliability
```

The routing benchmark covers 15-session sequential, fixed-seed shuffled and
no-shared-prefix negatives. The admission benchmark covers baseline, below,
near, over, far-over and queue-timeout scenarios. The reliability benchmark
covers normal SSE, short execution deadline, backend stop/restart/failback and
streaming client cancellation.

Reference results and their hardware/model caveats are in
`research/phase-2/implementation/week-4-benchmark.md` and
`week-4-reliability.md`. The absolute numbers are CPU Qwen2.5-0.5B and must not
be generalized to GPU serving.

## Safe fault drill

Start both backends and the gateway. Stop backend A:

```bash
./scripts/stop_backend.sh 18090
curl -s http://127.0.0.1:18080/health | python3 -m json.tool
```

After two health intervals, `backends.a.healthy` becomes false. A request for a
session normally mapped to A should return 200 with `X-Gateway-Backend: b`.
Restore A:

```bash
./scripts/start_backend.sh 18090 a
```

After two successful probes, A is healthy again and the next same-session
request returns to A.

Short deadline drill (restart gateway):

```bash
uv run python -m gateway --config config.toml \
  --execution-timeout-ms 50 --request-timeout-ms 2000
```

A long request should return 504 `execution_timeout`, with
`gateway_execution_timeouts_total` increasing and active/queue returning to 0.

## Cleanup

```bash
# stop the gateway with Ctrl-C, then:
./scripts/stop_backends.sh
rm -rf .run
pgrep -fl llama-server || true
```

The scripts only operate on PIDs recorded under `.run/`.

## Project layout

```
gateway/
  src/gateway/      # config, admission, backends, router, proxy, sse, metrics, server
  tests/            # unit, fake/raw backend, real llama-server integration
  bench/            # workloads, routing/admission/reliability runners
  scripts/          # two-backend and single-backend start/stop
  docs/             # architecture, metrics, runbook, limitations, engineering review
  config.example.toml
  config.reliability.toml
  pyproject.toml
```

## Design decisions

- **Starlette + uvicorn + httpx**: minimal ASGI stack, real streaming,
  connection pooling and explicit cancellation.
- **Rendezvous SHA-256 session hashing**: stable across processes and minimal
  migration when backends change.
- **Admission owns capacity**: route first, then admit; streaming holds the slot
  until the body ends; release is idempotent.
- **Deadlines never reset**: total and execution budgets are absolute.
- **Health is debounced**: consecutive failures remove, consecutive successes
  restore; business 5xx is not a health signal.
- **Retry only before dispatch**: unknown execution outcomes are never retried.
- **Raw chunk forwarding**: SSE bytes are forwarded as received; a separate
  incremental decoder only observes metrics.
- **Low-cardinality metrics**: bounded route/backend/status/kind labels; no
  session ids or prompt text.
