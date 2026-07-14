from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.paths import RUNTIME_DIR


LOG_PATH = RUNTIME_DIR / "query_log.jsonl"


def _max_bytes() -> int:
    """Per-file rotation threshold; default 10MB. Override via env."""
    return int(os.getenv("LOG_ROTATE_MAX_BYTES", str(10 * 1024 * 1024)))


def _keep_archives() -> int:
    """How many rotated archives to retain (FIFO); default 5."""
    return max(1, int(os.getenv("LOG_ROTATE_KEEP", "5")))


def _maybe_rotate(path: Path) -> None:
    """Rotate when the live log exceeds the size budget, and prune old
    archives FIFO. Safe on Linux + macOS — uses os.rename for atomicity.

    Archive filename: {stem}-{YYYYmmdd-HHMMSS}.jsonl
    """
    try:
        if not path.exists() or path.stat().st_size < _max_bytes():
            return
    except OSError:
        return
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    archive = path.with_name(f"{path.stem}-{stamp}{path.suffix}")
    try:
        os.replace(path, archive)
    except OSError:
        return
    # Prune oldest archives so disk doesn't grow unbounded.
    pattern = f"{path.stem}-*{path.suffix}"
    archives = sorted(path.parent.glob(pattern))
    excess = len(archives) - _keep_archives()
    for old in archives[:max(0, excess)]:
        try:
            old.unlink()
        except OSError:
            pass


def log_pipeline_event(event: dict[str, Any]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _maybe_rotate(LOG_PATH)
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **event,
    }
    with LOG_PATH.open("a") as f:
        f.write(json.dumps(payload, default=str, sort_keys=True) + "\n")
