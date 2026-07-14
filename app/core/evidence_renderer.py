"""Deterministic last-resort rendering of already-validated query rows.

This is not the primary answer writer.  It is used only when model-written
prose cannot pass verification, so a transient or mistaken judge cannot turn a
correct query into a different answer or a dead end.  It performs no analysis
and makes no derived claims: every displayed value is copied from a row.
"""

from __future__ import annotations

import re
from typing import Any

from app.core.formatting import format_key_number


_MONEY_COLUMN_RE = re.compile(
    r"contract|grant|payment|fund|amount|subaward|subcontract|inflow|outflow|spend|revenue|income",
    re.IGNORECASE,
)

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


def _scalar_result(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Render one copied numeric value cleanly without interpreting it."""
    if len(rows) != 1:
        return None
    row = rows[0]
    numeric = [
        (key, value)
        for key, value in row.items()
        if isinstance(value, (int, float))
        and not isinstance(value, bool)
        and key.casefold() not in {"year", "rank"}
    ]
    if len(numeric) != 1:
        return None
    key, value = numeric[0]
    label = key.replace("_", " ").strip().capitalize()
    dimensions = [
        str(item).strip()
        for column, item in row.items()
        if column != key and isinstance(item, str) and item.strip()
    ]
    raw_key_number = {
        "label": label,
        "value": value,
        "unit": "USD" if _MONEY_COLUMN_RE.search(key) else "",
    }
    formatted = format_key_number(raw_key_number)
    display_value = str(formatted["value"])
    if formatted.get("unit"):
        display_value += f" {formatted['unit']}"
    entity = f" for {', '.join(dimensions[:2])}" if dimensions else ""
    return {
        "answer": f"Verified {label.lower()}{entity}: **{display_value}**.",
        "key_numbers": [raw_key_number],
        "caveats": [
            "Evidence-only fallback: the value is copied directly from the validated query result."
        ],
        "confidence": "high",
        "valid": True,
    }


def render_verified_rows(
    rows: list[dict[str, Any]],
    *,
    truncated: bool = False,
    max_rows: int = 25,
    fallback: bool = True,
) -> dict[str, Any]:
    if not rows:
        return {
            "answer": "The verified query returned no rows.",
            "key_numbers": [],
            "caveats": [],
            "confidence": "low",
            "valid": True,
        }
    scalar = _scalar_result(rows)
    if scalar is not None:
        if not fallback:
            scalar["caveats"] = [
                "Analyst-verified query: the headline is copied directly from its validated result."
            ]
        return scalar
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
