#!/usr/bin/env bash
# Stop one backend started by start_backend.sh/start_backends.sh.
# Usage: ./scripts/stop_backend.sh PORT
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${1:?usage: stop_backend.sh PORT}"
PIDFILE="$ROOT/.run/backend-$PORT.pid"

if [[ ! -f "$PIDFILE" ]]; then
  echo "no recorded PID for port $PORT" >&2
  exit 1
fi

pid="$(cat "$PIDFILE")"
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
  echo "stopped backend port=$PORT pid=$pid"
fi
rm -f "$PIDFILE"
