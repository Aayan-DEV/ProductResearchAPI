#!/usr/bin/env sh
set -eu

# Default workers to 1 for more predictable SSE behavior
WORKERS="${WORKERS:-1}"
PORT="${PORT:-8000}"

exec python -m uvicorn main:app --host 0.0.0.0 --port "${PORT}" --workers "${WORKERS}"