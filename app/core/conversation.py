"""Conversation memory and follow-up contextualization.

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
- First classify how the latest message relates to the prior request:
  SUBSTITUTE replaces one value in an existing slot; ADD introduces a new
  measure/dimension while retaining compatible context; COMPARE explicitly
  asks to compare old and new values; TRANSFORM changes the calculation or
  presentation; STANDALONE is a complete new request.
- A bare substitution must remain a substitution. For example, after
  "Maryland's poverty rate in 2023", "what about Virginia?" means
  "Virginia's poverty rate in 2023". It does NOT mean compare Maryland with
  Virginia. Retain both only when the latest wording explicitly requests a
  comparison (compare, versus, vs, difference, relative to) or addition.
- If the latest message is a clarification answer or a follow-up, MERGE it with
  the earlier question. Preserve every still-relevant slot (geography grain,
  metric/measure, unrelated filter, time period, flow direction, ranking size,
  and comparison target). A value replaced by SUBSTITUTE is no longer
  relevant and MUST NOT appear in the rewrite.
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
- In particular, "compare that/it with X" means compare the most recent focal
  entity or group WITH X. Retain the prior entity, measure, period, grain, and
  direction; add X as the second comparison entity and rewrite the operation
  explicitly as a comparison. A possessive such as "X's outflow" supplies the
  new comparator and measure/direction, not permission to discard the first
  side of "compare".
- When a new self-contained expression repeats part of an earlier expression
  but omits a term, the omission is intentional. Do not restore deleted
  operands. Example: after "(total assets - total liabilities) + (current
  assets - current liabilities)", the new question "total assets - total
  liabilities" contains only that first difference.
- If the latest message is already a complete, self-contained question (a new
  topic), return it unchanged.
- Decide this BEFORE rewriting. A latest message that supplies its own
  operation, measure(s), geography/scope, and named entity is standalone even
  when it concerns the same dataset as the previous turn. Do not merge an old
  correlation, ranking, metric, or entity into that complete request.
  Example: earlier "correlate Asian share with income by state" followed by
  "sort Wyoming counties by Black and Asian population" -> the latest message
  is standalone and must remain exactly that request.
- Prefer the most recent analytical context when older contexts conflict.
- When structured analytical memory contains exact table ids, preserve those
  canonical ids in the rewrite instead of carrying forward a misspelled dataset
  name from user prose.
- Treat a terse repetition such as "number of employees?", "but the total?",
  or "I mean the actual value" as a correction that keeps the most recently
  named dataset and metric and asks for the metric's numeric value. Do not
  rewrite a metric-value request into a row-count or schema question unless
  the user explicitly says rows, records, columns, variables, or schema.
- When that correction names a dataset but no individual geography, preserve
  that it asks for the dataset-wide total of the metric at the current/default
  period. Do not rewrite it as one value per state/county/district. If an
  individual geography was named, preserve that narrower lookup instead.
- Preserve the distinction between employees working in a geography and
  federal employees residing there when the conversation establishes one.
- Preserve explanatory/schema follow-ups as explanatory/schema questions. If
  the latest message asks "why", asks what a denominator or age qualifier
  means, challenges whether a prior answer is possible, or asks which groups
  are actually available, carry forward the relevant dataset and metric but do
  NOT rewrite it as another request to calculate the previous numeric value.
  Example: a prior bachelor's-attainment answer followed by "why 25+; what
  about other ages?" becomes "In the ACS dataset, why is Education >=
  Bachelor's defined for adults 25+, and are other education-by-age groups
  available?"
- Never invent entities or metrics that were not stated by the user.
- Set latest_is_standalone=true only when the latest message can be interpreted
  without the conversation. When true, standalone_question must reproduce the
  latest request without imported context.

Return ONLY JSON:
{"latest_is_standalone": <bool>,
 "followup_mode": "SUBSTITUTE|ADD|COMPARE|TRANSFORM|STANDALONE",
 "standalone_question": "<latest unchanged, or safely rewritten follow-up>",
 "context_added": ["<slots copied from history>"]}"""


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
        lines = [
            f"  {key} = {value}" for key, value in memory.items() if value not in (None, "", [])
        ]
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

            log_pipeline_event(
                {
                    "stage": "contextualize",
                    "status": "skipped",
                    "reason": f"llm_error: {exc}",
                    "fallback": "returning raw question",
                }
            )
        except Exception:
            pass
        return question
    rewritten = str(raw.get("standalone_question") or "").strip()
    if raw.get("latest_is_standalone") is True:
        return question
    return rewritten or question
