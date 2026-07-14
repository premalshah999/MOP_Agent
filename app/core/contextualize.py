"""Conversation contextualization.

Multi-turn fix: a clarification answer ("federal contracts") or a follow-up
("what about Virginia?") is meaningless on its own. This rewrites the latest
message into a single self-contained question using the conversation so every
downstream stage (routing, entity resolution, SQL, answer) sees the full intent.

Runs ONLY when there is prior history, so single-turn behaviour (and the golden
/ held-out gates) is byte-for-byte unchanged and incurs no extra LLM call.
"""

from __future__ import annotations

from typing import Any

from app.llm import client

_SYSTEM = """You rewrite the user's latest message into ONE standalone analytical
question, using the conversation only to fill in missing context.

Rules:
- If the latest message is a clarification answer or a follow-up, MERGE it with
  the earlier question: carry over the entity/geography (state, county,
  district), the metric/measure, filters, and time period.
  e.g. earlier "federal spending in miami-dade" + answer "federal contracts"
       -> "federal contracts in Miami-Dade county"
  e.g. earlier "top counties in Maryland by grants" + "what about Virginia?"
       -> "top counties in Virginia by grants"
- If the latest message is already a complete, self-contained question (a new
  topic), return it unchanged.
- Never invent entities or metrics that were not stated by the user.
- Output the question only — no preamble.

Return ONLY JSON: {"standalone_question": "<rewritten question>"}"""


def _recent(history: list[dict[str, Any]]) -> str:
    turns = [h for h in history if h.get("role") in {"user", "assistant"}][-6:]
    return "\n".join(f"{h['role']}: {str(h.get('content', ''))[:400]}" for h in turns)


def _last_contract(history: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Most recent assistant turn's structured contract — far more reliable than
    re-parsing the answer prose. Carries the last analytical focus
    (table / metric / state / year)."""
    for h in reversed(history):
        if h.get("role") == "assistant":
            c = h.get("contract")
            if isinstance(c, dict):
                return c
            return None
    return None


def _structured_memory(history: list[dict[str, Any]]) -> str:
    c = _last_contract(history)
    if not c:
        return ""
    fields = [
        ("table(s)", c.get("tables") or ([c.get("family")] if c.get("family") else [])),
        ("metric", c.get("metric")),
        ("focus_state", c.get("focus_state")),
        ("year", c.get("year")),
        ("geography_level", c.get("geography_level")),
    ]
    lines = [f"  {k} = {v}" for k, v in fields if v]
    return ("PRIOR ANALYTICAL FOCUS (carry over unless overridden):\n" + "\n".join(lines)) if lines else ""


def contextualize(question: str, history: list[dict[str, Any]] | None) -> str:
    """Return a self-contained question. Falls back to `question` on any issue."""
    if not history:
        return question
    convo = _recent(history)
    if not convo.strip():
        return question
    memory = _structured_memory(history)
    try:
        raw = client.chat_json(
            [
                {"role": "system", "content": _SYSTEM},
                {
                    "role": "user",
                    "content": (
                        (memory + "\n\n" if memory else "")
                        + f"Conversation:\n{convo}\n\nLatest message: {question}\n\n"
                        "Rewrite it as a standalone question."
                    ),
                },
            ],
            temperature=0.0,
            max_tokens=200,
            purpose="contextualize",
        )
    except client.LLMError as exc:
        # Don't crash the pipeline if the contextualiser LLM is down — but log
        # it so we know when multi-turn behaviour becomes non-deterministic.
        # Surfacing this signal in the response envelope is left to the
        # orchestrator; here we just record it.
        try:
            from app.observability.logging import log_pipeline_event
            log_pipeline_event({
                "stage": "contextualize",
                "status": "skipped",
                "reason": f"llm_error: {exc}",
                "fallback": "returning raw question",
            })
        except Exception:
            pass
        return question
    rewritten = str(raw.get("standalone_question") or "").strip()
    return rewritten or question
