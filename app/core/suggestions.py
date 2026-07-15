"""Suggested follow-up questions, one cheap call after every analytical answer.

Goal: help the user keep exploring without typing. Given the question they just
asked and the answer they got, propose 3 short contextual next-questions
("compare to Virginia", "now per capita", "how did this change since 2020").
Best-effort — any failure returns an empty list and never blocks the response.
"""

from __future__ import annotations

from typing import Any

from app.llm import client
from app.semantic.registry import domain_summary

_SYSTEM = """You suggest 3 short follow-up questions an analyst would naturally
ask next. EVERY suggestion MUST be answerable using ONLY the catalog below —
do NOT propose questions about metrics or entities that are not listed
(no "students", "test scores", "crime", "unemployment", "GDP", forecasts, etc.).

CATALOG (only these measures + geographies are available)
========================================================
{domain}

Each suggestion:
- is 6–16 words, lowercase first letter unless a proper noun
- explores a DIFFERENT angle the catalog supports (peer comparison, per-capita /
  per-1000 variant from the catalog, year trend within the data's year range,
  drill-down by an available sub-region, switch to a related catalog measure)
- is self-contained enough to type as-is
- prefer concrete state/county names over generic phrasings
- preserves the current entity, metric, direction, and period unless it clearly
  states the intended change
- never suggests a trend when the current table has no year dimension

Reply ONLY with JSON: {{"followups": ["...","...","..."]}}"""


def suggest_followups(
    question: str,
    answer: str,
    contract: dict[str, Any] | None = None,
    max_items: int = 3,
) -> list[str]:
    if not question or not answer:
        return []
    ctx_lines: list[str] = []
    if contract:
        memory = contract.get("context_memory")
        if isinstance(memory, dict):
            for k, v in memory.items():
                if v not in (None, "", [], {}):
                    ctx_lines.append(f"  {k} = {v}")
        for k in ("family", "metric", "focus_state", "year", "geography_level"):
            v = contract.get(k)
            if v and not any(line.startswith(f"  {k} =") for line in ctx_lines):
                ctx_lines.append(f"  {k} = {v}")
    ctx = "\nCONTEXT:\n" + "\n".join(ctx_lines) if ctx_lines else ""
    try:
        raw = client.chat_json(
            [
                {"role": "system", "content": _SYSTEM.format(domain=domain_summary())},
                {
                    "role": "user",
                    "content": (
                        f"QUESTION: {question}\n\nANSWER:\n{answer[:1500]}\n{ctx}\n\n"
                        "Write 3 follow-ups, each answerable by the catalog above."
                    ),
                },
            ],
            temperature=0.0,
            max_tokens=250,
            purpose="suggest_followups",
        )
    except client.LLMError:
        return []
    items = raw.get("followups") or []
    if not isinstance(items, list):
        return []
    cleaned = [str(x).strip() for x in items if isinstance(x, str) and x.strip()]
    return cleaned[:max_items]
