#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/mop-agent}"
BRANCH="${BRANCH:-main}"
REMOTE="${REMOTE:-origin}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8000/health}"

cd "$APP_DIR"

if [[ ! -f .env ]]; then
  cp deploy/.env.production.example .env
  if command -v openssl >/dev/null 2>&1; then
    secret="$(openssl rand -hex 32)"
    sed -i "s/^JWT_SECRET=.*/JWT_SECRET=${secret}/" .env
  fi
  chmod 600 .env
  echo "Created .env from deploy/.env.production.example. Review ALLOWED_ORIGINS and TRUSTED_HOSTS for the public domain."
fi

git fetch "$REMOTE" "$BRANCH"
git pull --ff-only "$REMOTE" "$BRANCH"

docker compose build --pull
docker compose up -d --remove-orphans

for attempt in $(seq 1 30); do
  if curl -fsS "$HEALTH_URL" >/dev/null; then
    docker compose ps
    echo "Deployment healthy: $HEALTH_URL"
    exit 0
  fi
  sleep 2
done

docker compose logs --tail=120 mop-agent
echo "Deployment failed health check: $HEALTH_URL" >&2
exit 1
