#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/mop-agent}"
BRANCH="${BRANCH:-main}"
REMOTE="${REMOTE:-origin}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8000/health}"
DEEP_HEALTH_URL="${DEEP_HEALTH_URL:-http://127.0.0.1:8000/health/deep}"

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

JWT_VALUE="$(sed -n 's/^JWT_SECRET=//p' .env | tail -n 1)"
DEEPSEEK_VALUE="$(sed -n 's/^DEEPSEEK_API_KEY=//p' .env | tail -n 1)"
OPENAI_VALUE="$(sed -n 's/^OPENAI_API_KEY=//p' .env | tail -n 1)"
LLM_INVALID=false
if [[ -n "$DEEPSEEK_VALUE" ]]; then
  [[ "$DEEPSEEK_VALUE" == replace-* ]] && LLM_INVALID=true
elif [[ -z "$OPENAI_VALUE" || "$OPENAI_VALUE" == replace-* ]]; then
  LLM_INVALID=true
fi
if [[ ${#JWT_VALUE} -lt 32 ]] \
  || [[ "$JWT_VALUE" == replace-* || "$JWT_VALUE" == change-me* ]] \
  || [[ "$LLM_INVALID" == true ]] \
  || grep -Eq 'your-domain\.example' .env; then
  echo "Deployment blocked: .env still contains placeholder production values." >&2
  exit 1
fi

BACKUP_PATH="$(APP_DIR="$APP_DIR" bash deploy/backup.sh)"
echo "Pre-deployment backup: $BACKUP_PATH"

# Do not change a healthy installation while a required dependency is already
# unavailable. A failed preflight cannot be repaired by rebuilding the image.
if docker compose ps -q mop-agent | grep -q .; then
  curl -fsS "$HEALTH_URL" >/dev/null
  curl -fsS "$DEEP_HEALTH_URL" >/dev/null
fi

ROLLBACK_TAG="mop-agent:rollback-$(date -u +%Y%m%dT%H%M%SZ)"
if docker image inspect mop-agent:latest >/dev/null 2>&1; then
  docker tag mop-agent:latest "$ROLLBACK_TAG"
else
  ROLLBACK_TAG=""
fi

git fetch "$REMOTE" "$BRANCH"
git pull --ff-only "$REMOTE" "$BRANCH"
export APP_VERSION="$(git rev-parse --short=12 HEAD)"

docker compose build --pull
docker compose up -d --remove-orphans

for attempt in $(seq 1 45); do
  if curl -fsS "$HEALTH_URL" >/dev/null \
    && curl -fsS "$DEEP_HEALTH_URL" >/dev/null; then
    docker compose ps
    echo "Deployment healthy: $HEALTH_URL and $DEEP_HEALTH_URL"
    exit 0
  fi
  sleep 2
done

docker compose logs --tail=120 mop-agent
if [[ -n "$ROLLBACK_TAG" ]]; then
  echo "Health check failed; restoring the pre-deployment image." >&2
  docker tag "$ROLLBACK_TAG" mop-agent:latest
  docker compose up -d --no-build --force-recreate --remove-orphans
fi
echo "Deployment failed health checks: $HEALTH_URL or $DEEP_HEALTH_URL" >&2
exit 1
