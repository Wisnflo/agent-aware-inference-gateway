#!/usr/bin/env bash
# Stop the backends started by start_backends.sh (only recorded PIDs).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT/.run"
PORT_A="${PORT_A:-18090}"
PORT_B="${PORT_B:-18091}"

for port in "$PORT_A" "$PORT_B"; do
  pidfile="$RUN_DIR/backend-$port.pid"
  if [[ ! -f "$pidfile" ]]; then
    continue
  fi
  pid="$(cat "$pidfile")"
  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 0.2
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
    echo "stopped backend $port pid=$pid"
  fi
  rm -f "$pidfile"
done
