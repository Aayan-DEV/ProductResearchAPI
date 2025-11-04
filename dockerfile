# Python runtime (match local dev: 3.13)
FROM python:3.13-slim

# Prevent .pyc and ensure stdout/stderr are unbuffered
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000 \
    WORKERS=2

# Install curl for model download
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install deps first (leverages Docker layer caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# Defaults for model location inside Railway volume
ENV MODEL_DIR=/data/models \
    MODEL_PATH=/data/models/gemma-3n-E4B-it-Q4_K_S.gguf

# Install deps first (leverages Docker layer caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# Copy the rest of the project
COPY . /app
RUN chmod +x /app/bootstrap.sh

# Expose default port (can override by setting PORT)
EXPOSE 8000

# Start FastAPI via uvicorn; honors $PORT and $WORKERS
CMD ["sh", "/app/bootstrap.sh"]