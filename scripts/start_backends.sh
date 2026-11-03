#!/usr/bin/env bash
# Local CPU backends; process ownership is verified by manage_backends.py.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$ROOT/scripts/manage_backends.py" start-pair "$@"
