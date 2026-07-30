"""Non-analytical responses: META, CLARIFY, UNANSWERABLE, OUT_OF_SCOPE.

None of these touch the database. CLARIFY and OUT_OF_SCOPE are templated (no LLM
needed); META and UNANSWERABLE get one grounded LLM call so the reply is helpful
and specific to this catalog.
"""

from __future__ import annotations

import re
from typing import Any

from app.llm import client
from app.semantic.registry import domain_summary, load_registry
from app.semantic.discovery import build_guidance, discover_metrics

_SCOPE_LINE = (
    "I answer questions about a fixed catalog of US public-policy data: Census "
    "demographics (ACS), state/local government finance, federal "
    "contracts/grants/spending (incl. by agency), FINRA financial-health "
    "indices, and federal subaward flows — at state, county, and congressional-"
    "district level."
)

_CATALOG_SEARCH_RE = re.compile(
    r"(could(?: not|n't) find|can(?: not|'t) find|closest (?:measure|variable)|"
    r"do you have (?:data|a (?:measure|variable))|is there .*?(?:data|measure|variable))",
    re.IGNORECASE,
)


def _facts() -> str:
    reg = load_registry()
    lines = []
    for ds in reg.datasets.values():
        years = ", ".join(str(y) for y in ds.available_years) or "single snapshot"
        lines.append(f"- {ds.id}: {ds.description} (years: {years})")
    return "\n".join(lines)


def _llm_reply(system: str, user: str, purpose: str) -> str:
    try:
        return client.chat(
            [{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.0,
            max_tokens=600,
            purpose=purpose,
        ).strip()
    except client.LLMError:
        return ""


def respond(question: str, intent: str, intent_payload: dict[str, Any]) -> dict[str, Any]:
    # A user searching the data dictionary needs concept navigation regardless
    # of whether the router called the wording META, CLARIFY, or OUT_OF_SCOPE.
    # This also powers the dictionary's no-result CTA.
    if _CATALOG_SEARCH_RE.search(question):
        matches = discover_metrics(question, limit=1)
        intent_for_guidance = "CLARIFY" if matches and matches[0].score >= 0.86 else "UNANSWERABLE"
        return build_guidance(question, intent=intent_for_guidance)

    if intent == "CLARIFY":
        ask = intent_payload.get("clarification_question") or (
            "Could you clarify which measure, geography level, and time period you mean?"
        )
        return build_guidance(question, intent="CLARIFY", clarification=str(ask))

    if intent == "OUT_OF_SCOPE":
        system = (
            "You are a friendly, sharp data assistant for US public-policy "
            "data. The user asked something outside your scope (small talk, "
            "weather, jokes, general knowledge, etc.). Reply in ONE or TWO "
            "warm, natural sentences: acknowledge the ask with a little "
            "personality (never scold, never say 'outside what I can help "
            "with'), then pivot to what you CAN do with a concrete, inviting "
            "example question they could ask. No lists, no headers.\n\n"
            "What you cover: " + _SCOPE_LINE
        )
        reply = _llm_reply(system, question, "out_of_scope") or (
            f"I'll stay in my lane on that one — my expertise is data. {_SCOPE_LINE}"
        )
        return {"answer": reply, "resolution": "unsupported", "confidence": "high"}

    if intent == "UNANSWERABLE":
        return build_guidance(question, intent="UNANSWERABLE")

    # META
    system = (
        "You answer questions about THIS assistant and its catalog: what data "
        "exists, available years, and the meaning of datasets/terms. Use only "
        "the facts below. Be concise and helpful.\n\n"
        + _SCOPE_LINE
        + "\n\nDATASETS:\n"
        + _facts()
    )
    reply = _llm_reply(system, question, "meta") or _SCOPE_LINE
    return {"answer": reply, "resolution": "answered", "confidence": "high"}
