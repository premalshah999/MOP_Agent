from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.paths import RUNTIME_DIR


LOG_PATH = RUNTIME_DIR / "query_log.jsonl"


def log_pipeline_event(event: dict[str, Any]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **event,
    }
    with LOG_PATH.open("a") as f:
        f.write(json.dumps(payload, default=str, sort_keys=True) + "\n")
