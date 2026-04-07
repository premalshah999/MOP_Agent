from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

LOGGER = logging.getLogger("mop_agent.query_logger")
LOG_PATH = Path("data/query_log.jsonl")


def log_query(
    question: str,
    sql: Optional[str],
    success: bool,
    row_count: int,
    error: Optional[str] = None,
    user_id: Optional[int] = None,
    thread_id: Optional[str] = None,
) -> None:
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "question": question,
            "sql": sql,
            "success": success,
            "row_count": row_count,
            "error": error,
            "user_id": user_id,
            "thread_id": thread_id,
        }
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        LOGGER.exception("Failed to write query log")
