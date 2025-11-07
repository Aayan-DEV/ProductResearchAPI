# Lightweight Python base pinned by digest
FROM docker.io/library/python:3.11-slim@sha256:1738c75ae61595d2a9a5301d60a9a2f61abe7017005b3ccb660103d2476c6946

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Install curl and CA certificates
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir --upgrade pip && \
    if [ -f requirements.txt ]; then \
      pip install --no-cache-dir -r requirements.txt; \
    fi && \
    pip install --no-cache-dir gunicorn uvicorn

EXPOSE 8080
CMD ["sh", "-c", "gunicorn -b 0.0.0.0:${PORT:-8080} -w 2 -k uvicorn.workers.UvicornWorker main:app"]