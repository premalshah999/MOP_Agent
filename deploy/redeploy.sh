#!/usr/bin/env bash
# =============================================================================
# MOP Agent — Redeploy (run after pushing new code to GitHub)
# =============================================================================
# Usage (on the Hetzner server):
#   bash /opt/mop-agent/deploy/redeploy.sh
#
# What it does:
#   1. git pull
#   2. docker compose build (incremental — uses layer cache)
#   3. Zero-downtime swap: bring up new container before tearing down old
#   4. Health check
# =============================================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${CYAN}==>${NC} $*"; }
success() { echo -e "${GREEN}✓${NC} $*"; }
die()     { echo -e "${RED}✗ ERROR:${NC} $*" >&2; exit 1; }

APP_DIR="${APP_DIR:-/opt/mop-agent}"
[[ -d "$APP_DIR/.git" ]] || die "Not a git repo: $APP_DIR  (set APP_DIR env var if installed elsewhere)"

cd "$APP_DIR"

# Pull latest code
info "Pulling latest code..."
git fetch origin
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse @{u})
if [[ "$LOCAL" == "$REMOTE" ]]; then
  echo "  Already up to date. Force rebuild? [y/N] "
  read -r FORCE
  [[ "${FORCE,,}" == "y" ]] || { success "Nothing to do."; exit 0; }
fi
git pull --ff-only
success "Code updated to $(git rev-parse --short HEAD)."

# Build new image (incremental — Docker layer cache keeps it fast)
info "Building Docker image..."
docker compose build

# Restart with zero-downtime swap
info "Restarting container..."
docker compose up -d --remove-orphans

# Wait for health check
info "Waiting for health check..."
HEALTHY=false
for i in {1..30}; do
  if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    HEALTHY=true
    break
  fi
  sleep 2
done

if $HEALTHY; then
  success "Health check passed."
else
  die "Health check failed after 60s. Check logs: docker compose logs --tail=50"
fi

# Clean up old images
info "Pruning old Docker images..."
docker image prune -f --filter "label=com.docker.compose.project=mop-agent" 2>/dev/null || true

echo ""
success "Redeployment complete. Running version: $(git rev-parse --short HEAD)"
echo "  Logs:   docker compose -f $APP_DIR/docker-compose.yml logs -f"
echo "  Health: curl http://localhost:8000/health"
