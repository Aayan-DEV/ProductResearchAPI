# Lightweight Python base pinned by digest
FROM docker.io/library/python:3.11-slim@sha256:1738c75ae61595d2a9a5301d60a9a2f61abe7017005b3ccb660103d2476c6946

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Working directory
WORKDIR /app

# Copy the entire project into the image
COPY . /app

# Install runtime dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    if [ -f requirements.txt ]; then \
      pip install --no-cache-dir -r requirements.txt; \
    fi && \
    pip install --no-cache-dir gunicorn uvicorn

# Expose the service port
EXPOSE 8080

# Start the API with gunicorn; respects $PORT when provided (e.g., Railway)
# Use UvicornWorker to run FastAPI (ASGI)
CMD ["sh", "-c", "gunicorn -b 0.0.0.0:${PORT:-8080} -w 2 -k uvicorn.workers.UvicornWorker main:app"]