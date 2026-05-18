"""Stage 4b — grounded natural-language answer.

The answer is written STRICTLY from the rows the query returned. No numbers may
be invented; caveats from the grounding (year coverage, per-capita meaning,
proxies) must be surfaced, not buried.
"""

from __future__ import annotations

import json
from typing import Any

from app.llm import client

_MAX_ROWS_IN_PROMPT = 60

_SYSTEM = """You explain SQL results to a policy analyst.

You are given the question, the SQL that ran, and the EXACT rows it returned.
Write a clear, concise answer that uses ONLY those rows.

Rules:
- Never invent or extrapolate numbers. Every figure must come from the rows.
- If rows are empty, say plainly that the data returned nothing for that query
  and suggest the most likely reason (filter/year/scope) — do not guess a value.
- Lead with the direct answer. Use a short markdown table for rankings/multiple
  rows. Format large dollar amounts readably (e.g. $1.2B) but keep them faithful.
- Surface relevant caveats from the grounding (year actually used, per-capita vs
  total, single-snapshot data, proxy measures) in a short "Notes" line.
- Be brief. No methodology lecture.

Return ONLY JSON:
{"answer": "<markdown answer>",
 "key_numbers": [{"label": "<str>", "value": <number-or-string>, "unit": "<str>"}],
 "caveats": ["<short caveat>"],
 "confidence": "high|medium|low"}"""


def write_answer(
    question: str,
    sql: str,
    rows: list[dict[str, Any]],
    grounding_text: str,
) -> dict[str, Any]:
    shown = rows[:_MAX_ROWS_IN_PROMPT]
    user = (
        f"QUESTION: {question}\n\n"
        f"SQL THAT RAN:\n{sql}\n\n"
        f"ROWS RETURNED ({len(rows)} total, showing {len(shown)}):\n"
        f"{json.dumps(shown, default=str, indent=2)}\n\n"
        f"GROUNDING (for caveats only):\n{grounding_text[:4000]}\n\n"
        "Write the grounded answer JSON."
    )
    try:
        raw = client.chat_json(
            [
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
            max_tokens=1200,
            purpose="stage4_answer",
        )
    except client.LLMError as exc:
        return {
            "answer": f"I ran the query but could not compose a written answer ({exc}).",
            "key_numbers": [],
            "caveats": [],
            "confidence": "low",
        }

    key_numbers = []
    for item in raw.get("key_numbers") or []:
        if isinstance(item, dict) and "label" in item and "value" in item:
            key_numbers.append(
                {
                    "label": str(item["label"]),
                    "value": item["value"],
                    "unit": str(item.get("unit") or ""),
                }
            )
    return {
        "answer": str(raw.get("answer") or "").strip(),
        "key_numbers": key_numbers,
        "caveats": [str(c) for c in (raw.get("caveats") or [])],
        "confidence": str(raw.get("confidence") or "medium"),
    }
