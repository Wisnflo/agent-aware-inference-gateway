#!/usr/bin/env bash
# Start two llama-server backends for the gateway.
# Override any of: LLAMA_SERVER, GATEWAY_MODEL, PORT_A, PORT_B, CTX, N_PREDICT,
# THREADS, LOG_DIR.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE="${GATEWAY_WORKSPACE:-$HOME/PersonalHub/workspace}"
LLAMA_SERVER="${LLAMA_SERVER:-$WORKSPACE/tools/llama.cpp/build/bin/llama-server}"
MODEL="${GATEWAY_MODEL:-$WORKSPACE/models/qwen2.5-0.5b-instruct-q4_0.gguf}"
PORT_A="${PORT_A:-18090}"
PORT_B="${PORT_B:-18091}"
CTX="${CTX:-4096}"
N_PREDICT="${N_PREDICT:-16}"
THREADS="${THREADS:-4}"
LOG_DIR="${LOG_DIR:-$ROOT/bench/logs}"
RUN_DIR="$ROOT/.run"

if [[ ! -x "$LLAMA_SERVER" ]]; then
  echo "llama-server not executable: $LLAMA_SERVER" >&2
  exit 1
fi
if [[ ! -f "$MODEL" ]]; then
  echo "model not found: $MODEL" >&2
  exit 1
fi

mkdir -p "$LOG_DIR" "$RUN_DIR"

for port in "$PORT_A" "$PORT_B"; do
  if lsof -ti "tcp:$port" >/dev/null 2>&1; then
    echo "port $port is already in use; refusing to start" >&2
    exit 1
  fi
done

start_one() {
  local port="$1"
  local log="$LOG_DIR/backend-$port.log"
  "$LLAMA_SERVER" -m "$MODEL" --host 127.0.0.1 --port "$port" \
    -c "$CTX" -n "$N_PREDICT" -t "$THREADS" --metrics --no-webui \
    >"$log" 2>&1 &
  local pid=$!
  echo "$pid" >"$RUN_DIR/backend-$port.pid"
  echo "started backend $port pid=$pid log=$log"
}

wait_healthy() {
  local port="$1"
  for _ in $(seq 1 240); do
    if curl -fsS "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "backend $port did not become healthy" >&2
  return 1
}

start_one "$PORT_A"
start_one "$PORT_B"

for port in "$PORT_A" "$PORT_B"; do
  wait_healthy "$port" || {
    "$ROOT/scripts/stop_backends.sh"
    exit 1
  }
  echo "backend $port healthy"
done
