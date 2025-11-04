#!/usr/bin/env bash
set -euo pipefail

# ====== Settings (edit as needed) ======
APP_NAME="ai-keywords-api"
APP_DIR="/srv/AI_KEYWORDS_API"
IMAGE_NAME="ai-keywords-api:latest"

# Container listens on 8000 (uvicorn). Host maps 80 → 8000.
CONTAINER_PORT="${CONTAINER_PORT:-8000}"
HOST_PORT="${HOST_PORT:-80}"
WORKERS="${WORKERS:-2}"

# Optionally auto-fetch your code (choose ONE and fill the URL):
GIT_REPO=""           # e.g., https://github.com/youruser/AI_KEYWORDS_API.git
ARCHIVE_URL=""        # e.g., https://example.com/AI_KEYWORDS_API.tar.gz

# ====== Install Docker & tools ======
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y docker.io git curl ca-certificates
systemctl enable --now docker

# ====== Prepare app directory ======
mkdir -p "${APP_DIR}"
mkdir -p "${APP_DIR}/outputs" "${APP_DIR}/users"

# ====== Fetch code (optional) ======
if [ -n "${GIT_REPO}" ]; then
  if [ ! -d "${APP_DIR}/.git" ]; then
    git clone --depth=1 "${GIT_REPO}" "${APP_DIR}"
  fi
elif [ -n "${ARCHIVE_URL}" ]; then
  tmp="/tmp/ai_keywords_api.tar.gz"
  curl -fsSL "${ARCHIVE_URL}" -o "${tmp}"
  # Extract; if archive has a top folder, strip it
  tar -xzf "${tmp}" -C "${APP_DIR}" --strip-components=1 || tar -xzf "${tmp}" -C "${APP_DIR}"
fi

# ====== Build image if dockerfile exists ======
if [ -f "${APP_DIR}/dockerfile" ]; then
  docker build -t "${IMAGE_NAME}" -f "${APP_DIR}/dockerfile" "${APP_DIR}"
else
  echo "WARN: ${APP_DIR}/dockerfile not found. Upload code and build manually later."
fi

# ====== Systemd service for the container ======
cat >/etc/systemd/system/${APP_NAME}.service <<EOF
[Unit]
Description=AI Keywords API (Docker)
After=network.target docker.service
Requires=docker.service

[Service]
Type=simple
Restart=always
RestartSec=5
ExecStartPre=-/usr/bin/docker rm -f ${APP_NAME}
ExecStart=/usr/bin/docker run --name ${APP_NAME} \
  -p ${HOST_PORT}:${CONTAINER_PORT} \
  --env-file ${APP_DIR}/.env \
  -e PORT=${CONTAINER_PORT} \
  -e WORKERS=${WORKERS} \
  -v ${APP_DIR}/outputs:/app/outputs \
  -v ${APP_DIR}/users:/app/users \
  ${IMAGE_NAME}
ExecStop=/usr/bin/docker stop ${APP_NAME}

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
# If image exists, start now; otherwise it will start after you build.
systemctl enable --now ${APP_NAME} || true

# ====== Open firewall for HTTP (port 80) ======
if command -v ufw >/dev/null 2>&1; then
  ufw allow 80/tcp || true
  ufw --force enable || true
fi

echo "Init complete. Upload .env to ${APP_DIR}/.env, then build/restart if needed:"
echo "  docker build -t ${IMAGE_NAME} -f ${APP_DIR}/dockerfile ${APP_DIR}"
echo "  systemctl restart ${APP_NAME}"