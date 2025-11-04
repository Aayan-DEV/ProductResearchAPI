#!/bin/sh
set -eu

MODEL_DIR="${MODEL_DIR:-/data/models}"
MODEL_PATH="${MODEL_PATH:-$MODEL_DIR/gemma-3n-E4B-it-Q4_K_S.gguf}"
MODEL_URL="${MODEL_URL:-}"
MODEL_SHA256="${MODEL_SHA256:-}"

mkdir -p "$MODEL_DIR"

if [ ! -f "$MODEL_PATH" ]; then
  if [ -z "$MODEL_URL" ]; then
    echo "MODEL_URL not set and model not found at $MODEL_PATH"
    echo "Set MODEL_URL to a public or pre-signed URL to the .gguf file."
    exit 1
  fi

  echo "Model not found. Downloading from $MODEL_URL ..."
  TMP="${MODEL_PATH}.tmp"
  curl -L "$MODEL_URL" -o "$TMP"

  if [ -n "$MODEL_SHA256" ]; then
    echo "Verifying model checksum..."
    echo "$MODEL_SHA256  $TMP" | sha256sum -c -
  fi

  mv "$TMP" "$MODEL_PATH"
  echo "Model stored at $MODEL_PATH"
else
  echo "Model already present at $MODEL_PATH"
fi

exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}" --workers "${WORKERS:-2}"