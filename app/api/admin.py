"""Admin endpoints for the demo: usage summary, recent questions, recent
feedback. Gated by ADMIN_EMAILS (comma-separated env var).

Reads two append-only JSONL logs:
  - data/runtime/query_log.jsonl  (written by app.observability.logging)
  - data/runtime/feedback.jsonl   (written by app.api.feedback)

Kept intentionally small — the goal is post-demo visibility, not a BI tool.
"""

from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from typing import Any

from fastapi import HTTPException

from app.paths import RUNTIME_DIR

QUERY_LOG = RUNTIME_DIR / "query_log.jsonl"
FEEDBACK_LOG = RUNTIME_DIR / "feedback.jsonl"


def admin_emails() -> set[str]:
    raw = os.getenv("ADMIN_EMAILS", "")
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


def is_admin(user: dict[str, Any]) -> bool:
    email = str(user.get("email", "")).lower()
    return bool(email) and email in admin_emails()


def assert_admin(user: dict[str, Any]) -> None:
    if not is_admin(user):
        raise HTTPException(status_code=403, detail="Admin access required")


def _read_jsonl(path, limit: int = 5000) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out[-limit:]


def usage_summary(user: dict[str, Any]) -> dict[str, Any]:
    assert_admin(user)
    rows = _read_jsonl(QUERY_LOG)
    fb = _read_jsonl(FEEDBACK_LOG)
    if not rows and not fb:
        return {
            "total_questions": 0,
            "by_resolution": {},
            "by_intent": {},
            "unique_users": 0,
            "top_users": [],
            "recent_failures": [],
            "feedback": {"total": 0, "up": 0, "down": 0, "recent_down": []},
        }
    by_resolution = Counter(str(r.get("resolution", "unknown")) for r in rows)
    by_intent = Counter(str(r.get("intent", "unknown")) for r in rows)
    users = Counter(str(r.get("user_id", "?")) for r in rows if r.get("user_id"))
    failures = [r for r in rows if r.get("resolution") in ("error", "no_data")][-15:]
    fb_up = sum(1 for f in fb if f.get("verdict") == "up")
    fb_down = sum(1 for f in fb if f.get("verdict") == "down")
    recent_down = [
        {
            "timestamp": f.get("timestamp"),
            "user_email": f.get("user_email"),
            "message_id": f.get("message_id"),
            "note": f.get("note"),
        }
        for f in fb if f.get("verdict") == "down"
    ][-10:]
    return {
        "total_questions": len(rows),
        "by_resolution": dict(by_resolution),
        "by_intent": dict(by_intent),
        "unique_users": len(users),
        "top_users": users.most_common(10),
        "recent_failures": [
            {
                "timestamp": r.get("timestamp"),
                "question": r.get("question"),
                "resolution": r.get("resolution"),
                "warnings": r.get("warnings"),
                "user_id": r.get("user_id"),
            }
            for r in failures
        ],
        "feedback": {
            "total": len(fb),
            "up": fb_up,
            "down": fb_down,
            "recent_down": recent_down,
        },
    }


def recent_questions(user: dict[str, Any], limit: int = 50) -> list[dict[str, Any]]:
    assert_admin(user)
    rows = _read_jsonl(QUERY_LOG)
    out: list[dict[str, Any]] = []
    for r in reversed(rows[-limit:]):
        out.append(
            {
                "timestamp": r.get("timestamp"),
                "user_id": r.get("user_id"),
                "question": r.get("question"),
                "intent": r.get("intent"),
                "resolution": r.get("resolution"),
                "row_count": r.get("row_count"),
                "confidence": r.get("confidence"),
                "warnings": r.get("warnings"),
                "datasets": r.get("datasets"),
            }
        )
    return out


def recent_feedback(user: dict[str, Any], limit: int = 50) -> list[dict[str, Any]]:
    assert_admin(user)
    fb = _read_jsonl(FEEDBACK_LOG)
    out = []
    for f in reversed(fb[-limit:]):
        out.append(
            {
                "timestamp": f.get("timestamp"),
                "user_email": f.get("user_email"),
                "verdict": f.get("verdict"),
                "note": f.get("note"),
                "message_id": f.get("message_id"),
            }
        )
    return out
