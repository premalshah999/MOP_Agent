#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/mop-agent}"
BACKUP_ROOT="${BACKUP_ROOT:-/opt/mop-agent-backups}"
BACKUP_RETENTION="${BACKUP_RETENTION:-14}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SNAPSHOT_DIR="${BACKUP_ROOT}/${STAMP}"
ARCHIVE="${BACKUP_ROOT}/mop-agent-${STAMP}.tar.gz"

cd "$APP_DIR"
install -d -m 700 "$BACKUP_ROOT" "$SNAPSHOT_DIR"

git rev-parse HEAD > "$SNAPSHOT_DIR/git-commit.txt"
git status --short > "$SNAPSHOT_DIR/git-status.txt"
git diff --binary > "$SNAPSHOT_DIR/uncommitted.patch"
git bundle create "$SNAPSHOT_DIR/repository.bundle" --all

if [[ -f .env ]]; then
  install -m 600 .env "$SNAPSHOT_DIR/environment.env"
fi

CONTAINER_ID="$(docker compose ps -a -q mop-agent 2>/dev/null || true)"
if [[ -n "$CONTAINER_ID" ]]; then
  CONTAINER_IMAGE="$(docker inspect --format '{{.Image}}' "$CONTAINER_ID")"
  docker run --rm -i \
    --volumes-from "$CONTAINER_ID" \
    -v "$SNAPSHOT_DIR:/backup" \
    "$CONTAINER_IMAGE" python - <<'PY'
import glob
import os
import sqlite3
import tarfile

runtime = "/app/data/runtime"
source = os.path.join(runtime, "mop.sqlite3")
snapshot = "/backup/mop.sqlite3"
if os.path.exists(source):
    with sqlite3.connect(source) as src, sqlite3.connect(snapshot) as dst:
        src.backup(dst)

log_archive = "/backup/runtime-logs.tar.gz"
with tarfile.open(log_archive, "w:gz") as archive:
    for pattern in ("query_log.jsonl*", "feedback.jsonl*"):
        for path in glob.glob(os.path.join(runtime, pattern)):
            archive.add(path, arcname=os.path.basename(path))
PY
fi

tar -czf "$ARCHIVE" -C "$BACKUP_ROOT" "$STAMP"
chmod 600 "$ARCHIVE"
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$ARCHIVE" > "${ARCHIVE}.sha256"
else
  shasum -a 256 "$ARCHIVE" > "${ARCHIVE}.sha256"
fi
chmod 600 "${ARCHIVE}.sha256"
rm -rf "$SNAPSHOT_DIR"

find "$BACKUP_ROOT" -maxdepth 1 -type f -name 'mop-agent-*.tar.gz' -mtime "+${BACKUP_RETENTION}" -delete
find "$BACKUP_ROOT" -maxdepth 1 -type f -name 'mop-agent-*.tar.gz.sha256' -mtime "+${BACKUP_RETENTION}" -delete

echo "$ARCHIVE"
