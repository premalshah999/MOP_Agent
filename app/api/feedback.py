"""Per-message feedback (thumbs up / down + optional note).

Appends each verdict to data/runtime/feedback.jsonl. Read by the admin
dashboard alongside the existing query_log.jsonl. Kept intentionally small —
this is for capturing demo + early-use signal, not a full feedback platform.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field

from app.observability.logging import _maybe_rotate
from app.paths import RUNTIME_DIR

FEEDBACK_LOG = RUNTIME_DIR / "feedback.jsonl"


class FeedbackRequest(BaseModel):
    message_id: str
    thread_id: str | None = None
    verdict: Literal["up", "down"]
    note: str | None = Field(default=None, max_length=2000)


def record_feedback(payload: FeedbackRequest, user: dict[str, Any]) -> dict[str, Any]:
    if not payload.message_id.strip():
        raise HTTPException(status_code=400, detail="message_id is required")
    FEEDBACK_LOG.parent.mkdir(parents=True, exist_ok=True)
    _maybe_rotate(FEEDBACK_LOG)
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user.get("id"),
        "user_email": user.get("email"),
        "message_id": payload.message_id,
        "thread_id": payload.thread_id,
        "verdict": payload.verdict,
        "note": (payload.note or "").strip() or None,
    }
    with FEEDBACK_LOG.open("a") as f:
        f.write(json.dumps(entry, default=str, sort_keys=True) + "\n")
    return {"ok": True}
