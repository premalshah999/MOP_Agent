from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.paths import RUNTIME_DIR

LOG_PATH = RUNTIME_DIR / "query_log.jsonl"
LLM_LOG_PATH = RUNTIME_DIR / "llm_calls.jsonl"


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
    # Include process id and microseconds so simultaneous workers cannot
    # overwrite each other's archive within the same second.
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f") + f"-{os.getpid()}"
    archive = path.with_name(f"{path.stem}-{stamp}{path.suffix}")
    try:
        os.replace(path, archive)
    except OSError:
        return
    # Prune oldest archives so disk doesn't grow unbounded.
    pattern = f"{path.stem}-*{path.suffix}"
    archives = sorted(path.parent.glob(pattern))
    excess = len(archives) - _keep_archives()
    for old in archives[: max(0, excess)]:
        try:
            old.unlink()
        except OSError:
            pass


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    """Append one private JSONL record with a single O_APPEND write."""
    path.parent.mkdir(parents=True, exist_ok=True)
    _maybe_rotate(path)
    encoded = (json.dumps(payload, default=str, sort_keys=True) + "\n").encode("utf-8")
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        written = os.write(descriptor, encoded)
        if written != len(encoded):
            raise OSError(f"Short JSONL append: wrote {written} of {len(encoded)} bytes")
    finally:
        os.close(descriptor)


def log_pipeline_event(event: dict[str, Any]) -> None:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **event,
    }
    try:
        append_jsonl(LOG_PATH, payload)
    except OSError:
        # Telemetry must never turn a valid analytical response into a 500.
        return


def log_llm_event(event: dict[str, Any]) -> None:
    """Record model/prompt drift metadata without storing prompt or response text.

    This telemetry must never become an availability dependency. It is a
    shadow signal for comparing provider versions, prompt fingerprints,
    latency, token use, and output hashes across otherwise identical calls.
    """

    try:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **event,
        }
        append_jsonl(LLM_LOG_PATH, payload)
    except OSError:
        # Observability is deliberately non-blocking: losing one drift sample
        # must not prevent an otherwise valid analytical answer.
        return
