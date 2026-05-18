"""LLM-judge: is every quantitative claim in the answer supported by the rows?

Used by the strict golden suite and by the orchestrator to downgrade confidence
and attach a caveat when an answer drifts from the data.
"""

from __future__ import annotations

import json
from typing import Any

from app.llm import client

_SYSTEM = """You grade whether an answer is FAITHFUL to its data.

You get the question, the SQL that ran, and the EXACT rows it returned.

FAITHFUL means: numbers, rankings, entities, and comparisons in the answer are
supported by the rows. Apply these allowances generously:
- Rounding and unit formatting are CORRECT and faithful: $30.58B, $30.6B, or
  "about $30.58 billion" all faithfully represent 30579948445.74. Do not nitpick
  decimals or significant figures.
- Restating SCOPE that comes from the SQL/question (the year filtered on, the
  state, "per capita", "top 10") is faithful even if that value is not a column
  in the returned rows — e.g. saying "in 2024" when the SQL has year = '2024'.
- Brief, reasonable context that does not assert a new number is fine.

UNFAITHFUL means: fabricated or contradictory numbers, wrong ordering, wrong
entities, or quantitative claims with no support in the rows.

Only mark faithful=false for a real, material discrepancy.

Return ONLY JSON: {"faithful": <bool>, "reason": "<short>"}"""


def judge_faithfulness(
    question: str,
    answer: str,
    rows: list[dict[str, Any]],
    sql: str = "",
) -> dict[str, Any]:
    user = (
        f"QUESTION: {question}\n\n"
        f"SQL THAT RAN:\n{sql or '(not provided)'}\n\n"
        f"ANSWER:\n{answer}\n\n"
        f"ROWS ({len(rows)} total, showing up to 60):\n"
        f"{json.dumps(rows[:60], default=str, indent=2)}\n\n"
        "Grade faithfulness."
    )
    try:
        raw = client.chat_json(
            [
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
            max_tokens=300,
            purpose="faithfulness_judge",
        )
    except client.LLMError as exc:
        return {"faithful": True, "reason": f"judge unavailable ({exc}); not blocking"}
    return {
        "faithful": bool(raw.get("faithful", False)),
        "reason": str(raw.get("reason") or "").strip(),
    }
