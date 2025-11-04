# Python runtime (match local dev: 3.13)
FROM python:3.13-slim

# Prevent .pyc and ensure stdout/stderr are unbuffered
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000 \
    WORKERS=2

# Set working directory
WORKDIR /app

# Install deps first (leverages Docker layer caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# Copy the rest of the project
COPY . /app

# Expose default port (can override by setting PORT)
EXPOSE 8000

# Start FastAPI via uvicorn; honors $PORT and $WORKERS
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --workers ${WORKERS:-2}"]