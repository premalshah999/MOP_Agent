"""Deterministic last-resort rendering of already-validated query rows.

This is not the primary answer writer.  It is used only when model-written
prose cannot pass verification, so a transient or mistaken judge cannot turn a
correct query into a different answer or a dead end.  It performs no analysis
and makes no derived claims: every displayed value is copied from a row.
"""

from __future__ import annotations

from typing import Any


def _cell(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        text = f"{value:,}"
    elif isinstance(value, int) and not isinstance(value, bool):
        text = f"{value:,}"
    else:
        text = str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def render_verified_rows(
    rows: list[dict[str, Any]],
    *,
    truncated: bool = False,
    max_rows: int = 25,
) -> dict[str, Any]:
    if not rows:
        return {
            "answer": "The verified query returned no rows.",
            "key_numbers": [],
            "caveats": [],
            "confidence": "low",
            "valid": True,
        }
    columns = list(rows[0])
    shown = rows[:max_rows]
    header = "| " + " | ".join(_cell(column) for column in columns) + " |"
    divider = "|" + "|".join("---" for _ in columns) + "|"
    body = [
        "| " + " | ".join(_cell(row.get(column)) for column in columns) + " |"
        for row in shown
    ]
    omitted = len(rows) - len(shown)
    note = ""
    if omitted > 0 or truncated:
        visible = len(shown)
        suffix = " and the executor also capped the full result" if truncated else ""
        note = f"\n\n_Shown: {visible} of {len(rows)} returned rows{suffix}."
    answer = (
        "I could not safely validate the model-written interpretation, so here "
        "are the verified query results directly, without additional claims.\n\n"
        + "\n".join([header, divider, *body])
        + note
    )
    return {
        "answer": answer,
        "key_numbers": [],
        "caveats": ["Evidence-only fallback: values are copied directly from the validated query rows."],
        "confidence": "high",
        "valid": True,
    }
