# Python base; 3.13 matches your current environment
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000 \
    WORKERS=1 \
    MODEL_DIR=/data/models \
    MODEL_PATH=/data/models/gemma-3n-E4B-it-Q4_K_S.gguf

# Curl for model download + TLS certs
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*

# App directory
WORKDIR /app

# Install Python dependencies (layer-cached)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# Copy source and make bootstrap executable
COPY . /app
RUN chmod +x /app/bootstrap.sh

# Expose service port
EXPOSE 8000

# Healthcheck using existing /health endpoint in main.py
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD ["sh", "-c", "curl -fsS http://localhost:${PORT:-8000}/health || exit 1"]

# Entrypoint: downloads model if missing, then runs uvicorn
CMD ["python","-m","uvicorn","main:app","--host","0.0.0.0","--port","${PORT}","--workers","${WORKERS}"]
