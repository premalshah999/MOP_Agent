"""Per-message feedback (thumbs up / down + optional note).

Appends each verdict to data/runtime/feedback.jsonl. Read by the admin
dashboard alongside the existing query_log.jsonl. Kept intentionally small —
this captures product-quality signals without becoming a full feedback platform.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field

from app.observability.logging import append_jsonl
from app.paths import RUNTIME_DIR

FEEDBACK_LOG = RUNTIME_DIR / "feedback.jsonl"


class FeedbackRequest(BaseModel):
    message_id: str = Field(min_length=1, max_length=128)
    thread_id: str | None = Field(default=None, max_length=128)
    verdict: Literal["up", "down"]
    note: str | None = Field(default=None, max_length=2000)


def record_feedback(payload: FeedbackRequest, user: dict[str, Any]) -> dict[str, Any]:
    if not payload.message_id.strip():
        raise HTTPException(status_code=400, detail="message_id is required")
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user.get("id"),
        "user_email": user.get("email"),
        "message_id": payload.message_id,
        "thread_id": payload.thread_id,
        "verdict": payload.verdict,
        "note": (payload.note or "").strip() or None,
    }
    try:
        append_jsonl(FEEDBACK_LOG, entry)
    except OSError as exc:
        raise HTTPException(status_code=503, detail="Feedback could not be saved") from exc
    return {"ok": True}
