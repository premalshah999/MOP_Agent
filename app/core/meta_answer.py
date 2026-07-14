"""Non-analytical responses: META, CLARIFY, UNANSWERABLE, OUT_OF_SCOPE.

None of these touch the database. CLARIFY and OUT_OF_SCOPE are templated (no LLM
needed); META and UNANSWERABLE get one grounded LLM call so the reply is helpful
and specific to this catalog.
"""

from __future__ import annotations

from typing import Any

from app.llm import client
from app.semantic.registry import domain_summary, load_registry

_SCOPE_LINE = (
    "I answer questions about a fixed catalog of US public-policy data: Census "
    "demographics (ACS), state/local government finance, federal "
    "contracts/grants/spending (incl. by agency), FINRA financial-health "
    "indices, and federal subaward flows — at state, county, and congressional-"
    "district level."
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
    if intent == "CLARIFY":
        ask = intent_payload.get("clarification_question") or (
            "Could you clarify which measure, geography level, and time period you mean?"
        )
        return {"answer": ask, "resolution": "needs_clarification", "confidence": "medium"}

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
        system = (
            "You are a friendly, sharp data analyst. The user asked for a "
            "metric your catalog doesn't have. Reply the way a helpful "
            "colleague would — conversational, 2-3 sentences, no lists or "
            "headers. First be straight that you don't have that specific "
            "measure (name it). Then offer the one or two CLOSEST things you "
            "do have as a concrete follow-up question they could ask, phrased "
            "invitingly ('I could show you X instead — want that?'). Never "
            "say 'not available in the catalog' or 'the closest available "
            "metrics are'.\n\nCATALOG:\n" + domain_summary()
        )
        reply = _llm_reply(system, question, "unanswerable") or (
            f"I don't have that measure in my data. {_SCOPE_LINE}"
        )
        return {"answer": reply, "resolution": "unsupported", "confidence": "high"}

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
