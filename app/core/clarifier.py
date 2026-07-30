"""Grounded clarification options for incomplete analytical questions.

Suggestions come from the semantic registry through concept discovery. This
keeps clickable options synchronized with the variables the SQL pipeline can
actually query and removes model-invented clarification dead ends.
"""

from __future__ import annotations

from typing import Any

from app.semantic.discovery import build_guidance


def generate_clarification(question: str, clarification: str = "") -> dict[str, Any]:
    return build_guidance(question, intent="CLARIFY", clarification=clarification)


def generate_clarification_chips(question: str) -> list[str]:
    """Backward-compatible helper used by tests and older call sites."""
    return list(generate_clarification(question).get("suggestions") or [])
