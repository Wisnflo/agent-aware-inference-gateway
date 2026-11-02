# Runbook

Operational commands for the Agent-Aware Inference Gateway. Run from the
`gateway/` directory unless stated otherwise. Paths use environment variables;
do not hard-code machine-specific paths in deployment notes.

## 1. Prerequisites and assets

- Python ≥ 3.11; the reference environment is Python 3.13 with `uv`.
- A llama.cpp build containing `llama-server`.
- A GGUF model. Reference: Qwen2.5-0.5B-Instruct q4_0, 428,730,208 bytes,
  SHA-256 `7671c0c304e6ce5a7fc577bcb12aba01e2c155cc2efd29b2213c95b18edaf6ed`.

Reference llama.cpp build:

```bash
git clone --filter=blob:none https://github.com/ggml-org/llama.cpp.git
cd llama.cpp
git checkout 785322d17f2d51e5185cf996a1f4e46d87640841
cmake -B build -DLLAMA_BUILD_SERVER=ON -DGGML_METAL=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build --target llama-server -j 8
```

Reference model download (public ModelScope repository):

```bash
mkdir -p "$HOME/gateway-assets/models"
curl -L --fail \
  -o "$HOME/gateway-assets/models/qwen2.5-0.5b-instruct-q4_0.gguf" \
  'https://modelscope.cn/api/v1/models/Qwen/Qwen2.5-0.5B-Instruct-GGUF/repo?Revision=master&FilePath=qwen2.5-0.5b-instruct-q4_0.gguf'
shasum -a 256 "$HOME/gateway-assets/models/qwen2.5-0.5b-instruct-q4_0.gguf"
```

## 2. Install and configure

```bash
cd gateway
uv sync
cp config.example.toml config.toml
```

Export the asset locations used by scripts and tests:

```bash
export GATEWAY_WORKSPACE="$HOME/gateway-assets"
export LLAMA_SERVER="$GATEWAY_WORKSPACE/llama.cpp/build/bin/llama-server"
export GATEWAY_MODEL="$GATEWAY_WORKSPACE/models/qwen2.5-0.5b-instruct-q4_0.gguf"
```

`start_backends.sh` defaults to `$GATEWAY_WORKSPACE/tools/llama.cpp/...` and
`$GATEWAY_WORKSPACE/models/...`; either match those paths or pass the two
environment variables explicitly.

## 3. Start and stop

```bash
./scripts/start_backends.sh
```

Expected:

```
started backend 18090 pid=...
started backend 18091 pid=...
backend 18090 healthy
backend 18091 healthy
```

Start the gateway (foreground; stop with Ctrl-C):

```bash
uv run python -m gateway --config config.toml --strategy session_affinity
```

Health and metrics:

```bash
curl -s http://127.0.0.1:18080/health | python3 -m json.tool
curl -s http://127.0.0.1:18080/metrics | grep '^gateway_'
```

Stop backends (only PIDs recorded by the script):

```bash
./scripts/stop_backends.sh
rm -rf .run
```

## 4. Requests

Non-streaming:

```bash
curl -s http://127.0.0.1:18080/completion \
  -H 'Content-Type: application/json' \
  -H 'X-Session-ID: demo-session' \
  -d '{"prompt":"Say hello in one sentence.","n_predict":8,"stream":false,"cache_prompt":true}'
```

Streaming SSE (press Ctrl-C to stop):

```bash
curl -N http://127.0.0.1:18080/completion \
  -H 'Content-Type: application/json' \
  -H 'X-Session-ID: demo-session' \
  -d '{"prompt":"Say hello in one sentence.","n_predict":8,"stream":true,"cache_prompt":true}'
```

OpenAI-compatible route:

```bash
curl -s http://127.0.0.1:18080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Say hello."}],"max_tokens":8}'
```

The response carries `X-Gateway-Backend: a|b`. Admitted responses also carry
`X-Gateway-Queue-Ms`.

## 5. Tests and benchmarks

```bash
uv run pytest -q
GATEWAY_REAL_BACKENDS=1 uv run pytest -q -m real_backend
uv run python -m bench.run_bench --repeat 5 \
  --workloads sequential,shuffled,negative --strategies b0,b1 \
  --outdir bench/results/week4-route
uv run python -m bench.run_admission_bench --repeat 5 \
  --outdir bench/results/week4-admission
uv run python -m bench.run_reliability_bench --repeat 5 \
  --outdir bench/results/week4-reliability
```

The benchmark scripts restart backends and the gateway per run. Do not run two
benchmark scripts concurrently: both use ports 18090/18091.

## 6. Safe fault drill

Start both backends and the gateway first. To demonstrate health removal and
failover for backend A:

```bash
./scripts/stop_backend.sh 18090
curl -s http://127.0.0.1:18080/health | python3 -m json.tool
curl -s http://127.0.0.1:18080/completion \
  -H 'Content-Type: application/json' \
  -H 'X-Session-ID: demo-session' \
  -d '{"prompt":"hello","n_predict":1,"stream":false}'
```

Wait at least two health intervals (`health_interval_s` in the config) and
observe `backends.a.healthy=false`; the request should return `200` with
`X-Gateway-Backend: b`. Restore A:

```bash
./scripts/start_backend.sh 18090 a
```

Wait for two successful probes; `/health` should report A healthy and the next
same-session request should return to A. If the request is sent before health
completion, it may still use B or hit a connect failure and be safely retried.

Short-deadline drill (gateway restart needed):

```bash
uv run python -m gateway --config config.toml \
  --execution-timeout-ms 50 --request-timeout-ms 2000
```

Send a long request; expect `504` with `error.type=execution_timeout` and
`gateway_execution_timeouts_total` increasing.

## 7. Troubleshooting

| Symptom | Check |
|---|---|
| `port 18090 is already in use` | `lsof -iTCP:18090 -sTCP:LISTEN`; stop only the project PID in `.run/` |
| `/health` 503 `unavailable` | Both backends unhealthy; check llama-server logs and model path |
| `429 queue_full` under load | Bounded admission working; inspect `gateway_queue_depth`, `max_queue_size`, client concurrency |
| `429 queue_timeout` | Queue wait exceeded; inspect `queue_timeout_ms` and backend latency |
| `503 no_healthy_backend` | All backends failed probes; inspect `/health` and backend processes |
| `504 request_deadline`/`execution_timeout` | Deadline reached; inspect `gateway_execution_timeouts_total` and logs |
| SSE ends without `[DONE]` | Upstream error, client disconnect or deadline; inspect `gateway_stream_terminations_total` |
| No cache hit | Same session must route to same backend; check `X-Gateway-Backend` and `cache_n` |
| High p95 | Cold first turns dominate; inspect per-request raw benchmark data |

Logs:

- gateway stdout/stderr (foreground terminal);
- backend logs from `scripts/start_backends.sh`: `bench/logs/backend-<port>.log`;
- benchmark logs under `bench/results/<run>/logs/`.

## 8. Cleanup

```bash
./scripts/stop_backends.sh
rm -rf .run
```

Stop the gateway with Ctrl-C. Verify no project processes remain:

```bash
pgrep -fl llama-server || true
lsof -iTCP:18080 -sTCP:LISTEN || true
```

Do not kill unrelated `llama-server` processes; the scripts only operate on
PIDs they recorded.
