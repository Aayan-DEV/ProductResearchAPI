# Python slim; install curl and build tooling for llama-cpp-python
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl ca-certificates build-essential cmake && \
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