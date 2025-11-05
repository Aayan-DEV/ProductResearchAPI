FROM python:3.13-slim

# Runtime configuration
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000 \
    WORKERS=1 \
    MODEL_DIR=/data/models \
    MODEL_PATH=/data/models/gemma-3n-E4B-it-Q4_K_S.gguf \
    PIP_NO_CACHE_DIR=1 \
    CMAKE_ARGS="-DGGML_BLAS=ON -DGGML_BLAS_VENDOR=OpenBLAS" \
    LLAMA_CPP_USE_OPENBLAS=1

# System deps (curl for healthcheck; build-essential/cmake/openblas for llama-cpp-python)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    build-essential \
    cmake \
    git \
    libopenblas-dev \
 && rm -rf /var/lib/apt/lists/*

# App setup
WORKDIR /app

# Install Python deps first to leverage layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && \
    pip install -r /app/requirements.txt

# Copy source
COPY . /app

# Healthcheck for Railway
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD curl -fsS http://localhost:${PORT}/health || exit 1

# Port hint (Railway injects PORT env, we honor it)
EXPOSE 8000

# Start FastAPI via uvicorn; honors $PORT and $WORKERS
CMD ["python","-m","uvicorn","main:app","--host","0.0.0.0","--port","${PORT}","--workers","${WORKERS}"]