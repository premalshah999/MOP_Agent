"""Stage 1 — Question Intent.

One DeepSeek call that buckets the user's message. Only ANALYTICAL proceeds to
routing/SQL; everything else is answered without touching the database. When the
model misbehaves we fail safe to CLARIFY (ask) rather than guess with SQL.
"""

from __future__ import annotations

from typing import Any

from app.llm import client
from app.semantic.registry import domain_summary

INTENTS = {"ANALYTICAL", "CLARIFY", "UNANSWERABLE", "META", "OUT_OF_SCOPE"}

_SYSTEM = """You are the intent classifier for a US public-policy data assistant.
The assistant ONLY answers from this fixed catalog:

{domain}

Classify the user's message into exactly ONE intent:

- ANALYTICAL: a concrete question answerable with SQL over the catalog above
  (rankings, lookups, comparisons, trends, breakdowns, cross-dataset joins).
  The needed measure and scope are clear enough to write one query.
- CLARIFY: in-domain but underspecified — the measure, scope, or comparison is
  ambiguous and a reasonable analyst would ask one question back before querying
  (e.g. "how much federal money" — contracts? grants? all?; "best states" — by what?).
- UNANSWERABLE: a data question about the US, but the metric is NOT in the catalog
  (crime, unemployment, GDP, inflation, election results, weather statistics).
- META: about the assistant itself, what data exists, available years, or the
  definition/meaning of a dataset or term ("what is FINRA?", "what does grants mean?").
- OUT_OF_SCOPE: not a question about this data at all (chitchat, jokes, weather,
  coding help, general knowledge).

Rules:
- If the question names or clearly implies a specific catalog measure (grants,
  contracts, employees, subaward/subcontract inflow, financial literacy, poverty,
  assets, liabilities, debt ratio, income, population...), it is ANALYTICAL even
  if phrased as "how much/how many/where does X rank". Reserve CLARIFY for when
  the MEASURE ITSELF is ambiguous ("federal money", "best", "better", "doing well").
- Descriptive superlatives that map to ONE clear catalog measure are ANALYTICAL,
  not CLARIFY: "poorest"->poverty rate, "how wealthy"->median household income,
  "most educated"->bachelor's attainment, "most stressed"->financial constraint,
  "biggest economy/most populous"->population. Only CLARIFY if several unrelated
  measures fit equally ("best", "doing well", "healthiest").
- "trend ... over time / by year", rankings, comparisons, lookups, breakdowns,
  and cross-dataset questions are ANALYTICAL. META is ONLY about data
  availability or the meaning of a term, never an actual computed trend.
- "free cash flow", "cash flow" -> government finance (ANALYTICAL), NOT subaward flow.
- Misspellings/abbreviations are fine if intent is clear (DoD, defence, fin lit).
- A complete request is ANALYTICAL even if it omits a year (a default is applied).
- Prefer UNANSWERABLE over inventing data; prefer CLARIFY only for true ambiguity.
- Only ANALYTICAL sets requires_sql=true.

Return ONLY JSON:
{{"intent": "<one of ANALYTICAL|CLARIFY|UNANSWERABLE|META|OUT_OF_SCOPE>",
 "requires_sql": <bool>,
 "needs_clarification": <bool>,
 "clarification_question": "<a single question to ask, or empty>",
 "reason": "<short>"}}"""

_FEWSHOT = [
    ("top 10 counties in maryland by grants", "ANALYTICAL"),
    ("how many grant dollars did Maryland receive", "ANALYTICAL"),
    ("subcontract inflow to Maryland", "ANALYTICAL"),
    ("trend of average financial literacy by year", "ANALYTICAL"),
    ("Maryland congressional districts by free cash flow", "ANALYTICAL"),
    ("epartment of defence biggest deals by state", "ANALYTICAL"),
    ("How much federal money goes to Maryland?", "CLARIFY"),
    ("show me the best states", "CLARIFY"),
    ("top counties with the maximum crime rate", "UNANSWERABLE"),
    ("unemployment rate by state", "UNANSWERABLE"),
    ("what is FINRA?", "META"),
    ("what years are available for FINRA county data?", "META"),
    ("what's the weather in Baltimore today?", "OUT_OF_SCOPE"),
    ("tell me a joke", "OUT_OF_SCOPE"),
]


def _history_snippet(history: list[dict[str, Any]] | None) -> str:
    if not history:
        return ""
    recent = [h for h in history if h.get("role") in {"user", "assistant"}][-4:]
    return "\n".join(f"{h['role']}: {h.get('content', '')[:200]}" for h in recent)


def classify_intent(question: str, history: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    system = _SYSTEM.format(domain=domain_summary())
    fewshot = "\n".join(f'  "{q}" -> {label}' for q, label in _FEWSHOT)
    convo = _history_snippet(history)
    user = (
        (f"Recent conversation:\n{convo}\n\n" if convo else "")
        + f"Examples:\n{fewshot}\n\nClassify this message:\n{question}"
    )
    try:
        raw = client.chat_json(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
            max_tokens=300,
            purpose="stage1_intent",
        )
    except client.LLMError:
        return {
            "intent": "CLARIFY",
            "requires_sql": False,
            "needs_clarification": True,
            "clarification_question": "Could you rephrase or add detail to your question?",
            "reason": "intent model unavailable; failing safe to clarify",
        }

    intent = str(raw.get("intent", "")).strip().upper()
    if intent not in INTENTS:
        intent = "CLARIFY"
    return {
        "intent": intent,
        "requires_sql": intent == "ANALYTICAL",
        "needs_clarification": bool(raw.get("needs_clarification")) or intent == "CLARIFY",
        "clarification_question": str(raw.get("clarification_question") or "").strip(),
        "reason": str(raw.get("reason") or "").strip(),
    }
