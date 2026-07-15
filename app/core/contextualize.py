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
question, using the conversation and structured analytical memory only to fill
in missing context.

Rules:
- If the latest message is a clarification answer or a follow-up, MERGE it with
  the earlier question. Preserve every still-relevant entity/geography (state,
  county, district, agency), metric/measure, filter, time period, flow
  direction, ranking size, and comparison target.
  e.g. earlier "federal spending in miami-dade" + answer "federal contracts"
       -> "federal contracts in Miami-Dade county"
  e.g. earlier "top counties in Maryland by grants" + "what about Virginia?"
       -> "top counties in Virginia by grants"
- Treat substitutions ("what about Virginia?"), additions ("and poverty?"),
  references ("both", "the second one"), transforms ("per capita", "rank it
  nationally", "same years"), and drill-downs ("which counties contribute
  most?") as follow-ups. Apply the requested change and retain the rest.
- A newly named value normally replaces the prior value for the same slot; it
  does not erase unrelated slots. Explicit comparison wording adds a value.
- If the latest message is already a complete, self-contained question (a new
  topic), return it unchanged.
- Prefer the most recent analytical context when older contexts conflict.
- Never invent entities or metrics that were not stated by the user.
- Output the question only — no preamble.

Return ONLY JSON: {"standalone_question": "<rewritten question>"}"""


def _is_clarification(turn: dict[str, Any]) -> bool:
    contract = turn.get("contract")
    if not isinstance(contract, dict):
        return False
    return (
        str(contract.get("contract_type") or "").upper() == "CLARIFY"
        or contract.get("resolution") == "needs_clarification"
    )


def _recent(history: list[dict[str, Any]]) -> str:
    # Generated analytical prose is not memory: if it was wrong, feeding it
    # back makes the next turn inherit the error.  Structured assistant
    # contracts are carried separately by structured_memory().
    lines: list[str] = []
    for turn in history[-12:]:
        role = turn.get("role")
        if role == "user":
            lines.append(f"user: {str(turn.get('content', ''))[:500]}")
            continue
        # Generated analytical prose is intentionally excluded. Clarification
        # prompts and structured clickable options are safe conversational
        # references ("the second one") and need to remain available.
        options = turn.get("suggested_followups") or turn.get("suggestedFollowups")
        if role == "assistant" and _is_clarification(turn):
            lines.append(f"assistant clarification: {str(turn.get('content', ''))[:500]}")
        if role == "assistant" and isinstance(options, list) and options:
            clean = [str(item)[:180] for item in options[:5] if str(item).strip()]
            if clean:
                lines.append("assistant options: " + " | ".join(clean))
    return "\n".join(lines[-8:])


def prior_history(
    history: list[dict[str, Any]] | None, current_question: str
) -> list[dict[str, Any]]:
    """Return history strictly before the current turn.

    Some clients append the current user message before calling the pipeline.
    Including it twice changes the effective prompt and was a major source of
    same-question drift.
    """
    cleaned = list(history or [])
    if cleaned and cleaned[-1].get("role") == "user":
        latest = " ".join(str(cleaned[-1].get("content") or "").split()).casefold()
        current = " ".join(str(current_question or "").split()).casefold()
        if latest == current:
            cleaned.pop()
    return cleaned


def _analytical_memories(history: list[dict[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
    memories: list[dict[str, Any]] = []
    for turn in reversed(history):
        if turn.get("role") != "assistant" or not isinstance(turn.get("contract"), dict):
            continue
        contract = turn["contract"]
        memory = contract.get("context_memory")
        if isinstance(memory, dict) and memory:
            memories.append(memory)
        elif contract.get("supported"):
            # Backward compatibility for conversations created before rich
            # memory was added.
            legacy = {
                "tables": contract.get("tables")
                or ([contract.get("family")] if contract.get("family") else []),
                "metrics": [contract.get("metric")] if contract.get("metric") else [],
                "geography_level": contract.get("geography_level"),
                "operation": contract.get("operation"),
                "period": contract.get("year"),
                "focus_state": contract.get("focus_state"),
                "sort_direction": contract.get("sort_direction"),
                "top_k": contract.get("top_k"),
            }
            memories.append({k: v for k, v in legacy.items() if v not in (None, "", [])})
        if len(memories) >= limit:
            break
    return memories


def structured_memory(history: list[dict[str, Any]]) -> str:
    """Compact, prose-free memory for contextualization and reasoning."""
    memories = _analytical_memories(history)
    if not memories:
        return ""
    blocks: list[str] = []
    for index, memory in enumerate(memories, start=1):
        lines = [f"  {key} = {value}" for key, value in memory.items() if value not in (None, "", [])]
        if lines:
            label = "most recent" if index == 1 else f"{index - 1} turn(s) earlier"
            blocks.append(f"ANALYTICAL CONTEXT — {label}:\n" + "\n".join(lines))
    return "\n\n".join(blocks)


def contextualize(question: str, history: list[dict[str, Any]] | None) -> str:
    """Return a self-contained question. Falls back to `question` on any issue."""
    history = prior_history(history, question)
    if not history:
        return question
    convo = _recent(history)
    if not convo.strip():
        return question
    memory = structured_memory(history)
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
