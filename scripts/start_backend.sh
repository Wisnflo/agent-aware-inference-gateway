#!/usr/bin/env bash
# Start one llama-server backend (used by the safe fault drill).
# Usage: ./scripts/start_backend.sh PORT [BACKEND_ID]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE="${GATEWAY_WORKSPACE:-$HOME/PersonalHub/workspace}"
LLAMA_SERVER="${LLAMA_SERVER:-$WORKSPACE/tools/llama.cpp/build/bin/llama-server}"
MODEL="${GATEWAY_MODEL:-$WORKSPACE/models/qwen2.5-0.5b-instruct-q4_0.gguf}"
PORT="${1:?usage: start_backend.sh PORT [BACKEND_ID]}"
BACKEND_ID="${2:-$PORT}"
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
if lsof -ti "tcp:$PORT" >/dev/null 2>&1; then
  echo "port $PORT is already in use; refusing to start" >&2
  exit 1
fi

mkdir -p "$LOG_DIR" "$RUN_DIR"
"$LLAMA_SERVER" -m "$MODEL" --host 127.0.0.1 --port "$PORT" \
  -c "$CTX" -n "$N_PREDICT" -t "$THREADS" --metrics --no-webui \
  >"$LOG_DIR/backend-$PORT.log" 2>&1 &
pid=$!
echo "$pid" >"$RUN_DIR/backend-$PORT.pid"
echo "started backend $BACKEND_ID port=$PORT pid=$pid"

for _ in $(seq 1 240); do
  if curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
    echo "backend $PORT healthy"
    exit 0
  fi
  sleep 0.25
done
echo "backend $PORT did not become healthy" >&2
exit 1
