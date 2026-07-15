"""Stage 1 + Stage 2 fused into a single LLM call.

The intent classification and table routing decisions share most of the context
(the catalog + the conversation) and never disagree in practice. Fusing them
removes one full LLM round-trip per analytical question — ~0.5–1s of latency
and one extra TCP+cache lookup — without giving up either stage's per-test
coverage (`classify_intent` and `route` are still importable for the stage1/
stage2 tests).

For non-ANALYTICAL intents, the routing fields are returned empty (the
orchestrator ignores them and answers via meta_answer / clarification).
"""

from __future__ import annotations

import re
from typing import Any

from app.llm import client
from app.semantic.registry import catalog_for_prompt, domain_summary, get_dataset, load_registry

INTENTS = {"ANALYTICAL", "CLARIFY", "UNANSWERABLE", "META", "OUT_OF_SCOPE"}
_VALID_TABLES = set(load_registry().datasets)


_SYSTEM = """You are the intent + routing brain of a US public-policy data assistant.
ONE call decides BOTH (1) what kind of question this is and (2) which catalog
table(s) would answer it.

DOMAIN
======
{domain}

CATALOG (use exact ids when routing)
====================================
{catalog}

STEP 1 — classify the user's message:
- ANALYTICAL: answerable with SQL over the catalog (rankings, lookups,
  comparisons, trends, breakdowns, cross-dataset joins). The needed measure
  and scope are clear enough to write one query.
- CLARIFY: in-domain but the MEASURE itself is ambiguous (e.g. "federal money").
- UNANSWERABLE: a US data question but the metric is NOT in the catalog
  (crime, unemployment, GDP, weather statistics, forecasts).
- META: about the assistant, the data catalog, available years, or term meaning.
- OUT_OF_SCOPE: not about this data (chitchat, jokes, weather, general knowledge).

Rules:
- If the question NAMES a specific catalog measure (grants, contracts, direct
  payments, employees, subaward/subcontract, financial literacy, poverty,
  assets, liabilities, debt ratio, income, population, ...), it is ANALYTICAL
  even if phrased "how much / how many / how much did X get in <measure>".
  Reserve CLARIFY for when the MEASURE itself is ambiguous: "federal money"
  (which channel?), "best states" (by what?), "doing well" (which metric?).
- Descriptive superlatives that map to ONE clear catalog measure are ANALYTICAL,
  not CLARIFY ("poorest"->poverty rate, "how wealthy"->median income,
  "most stressed"->financial constraint, "most educated"->bachelor's).
- "trend ... by year" / rankings / comparisons / lookups / breakdowns are ANALYTICAL.
- "free cash flow", "cash flow" -> gov_* (ANALYTICAL), NOT the *_flow tables.
- A complete request is ANALYTICAL even if it omits a year (defaults apply).
- Misspellings/abbreviations are fine when intent is clear.

STEP 2 (only when intent=ANALYTICAL) — pick the SMALLEST set of tables:
- Geography suffix matches the asked grain (_state / _county / _congress).
  If NO grain is named, default to _state. Never return multiple grains of the
  same family — pick exactly one.
- Federal awards by AGENCY -> spending_state_agency. Without an agency split -> contract_*.
- contract_state is the PRIMARY state-level awards table; use spending_state
  only if the user explicitly asks for the spending-category breakdown.
- "free cash flow" / fiscal health / assets / debt -> gov_*.
- "subaward" / "subcontract" / money flowing between places -> *_flow.
- demographics / population / race / education / income / poverty -> acs_*.
- financial literacy / stress / risk aversion -> finra_*.
- Cross-dataset ("X and their Y" from different families) -> return BOTH tables.
- Per-capita / share variants stay in the SAME table.
- ACS share/percent/rate questions use the percentage metric directly; do NOT
  add `Total population`. Add `Total population` only when the user asks for a
  count/number of people derived from a demographic percentage.
- If two different tables are equally plausible AND the choice changes the
  answer, set needs_clarification=true and ask which one.

Return ONLY JSON:
{{"intent": "<one of ANALYTICAL|CLARIFY|UNANSWERABLE|META|OUT_OF_SCOPE>",
 "requires_sql": <bool>,
 "needs_clarification": <bool>,
 "clarification_question": "<question to ask or empty>",
 "reason": "<short>",
 "tables": ["<exact ids, [] if not ANALYTICAL>"],
 "metric_columns": ["<exact measure columns required; no dimensions>"],
 "filter_columns": ["<exact dimension/filter columns required>"],
 "geography_level": "state|county|congress|none",
 "operation": "lookup|ranking|comparison|trend|correlation|distribution|aggregate|breakdown",
 "flow_direction": "inflow|outflow|none",
 "sort_direction": "asc|desc|none",
 "top_k": <integer or null>,
 "year_strategy": "<period to use, or 'no year filter'>",
 "join_plan": "<how to join if >1 table, else empty>",
 "assumptions": ["<only assumptions required by catalog defaults>"],
 "confidence": "high|medium|low"}}"""


_FEWSHOT = """Examples:
  "top 10 counties in maryland by grants" -> ANALYTICAL, ["contract_county"]
  "which agencies give the most grants to Maryland" -> ANALYTICAL, ["spending_state_agency"]
  "Maryland congressional districts by free cash flow" -> ANALYTICAL, ["gov_congress"]
  "subcontract inflow to Maryland" -> ANALYTICAL, ["state_flow"]
  "top 10 states by debt ratio" -> ANALYTICAL, ["gov_state"]
  "states with highest financial literacy and their government debt ratio" -> ANALYTICAL, ["finra_state","gov_state"]
  "where is the maximum asian population by count" -> ANALYTICAL, ["acs_state"]
  "how many grant dollars did Maryland receive" -> ANALYTICAL, ["contract_state"]
  "how much did California get in direct payments" -> ANALYTICAL, ["contract_state"]
  "How much federal money goes to Maryland?" -> CLARIFY, []
  "rank the states" -> CLARIFY, []
  "top counties with the maximum crime rate" -> UNANSWERABLE, []
  "what is FINRA?" -> META, []
  "tell me a joke" -> OUT_OF_SCOPE, []
"""


def _history_snippet(history: list[dict[str, Any]] | None) -> str:
    if not history:
        return ""
    # Prior assistant prose can contain an earlier model mistake.  Feed the
    # router user intent plus structured contract memory, never generated
    # analytical prose that can recursively contaminate the next answer.
    recent = [h for h in history if h.get("role") == "user"][-4:]
    return "\n".join(f"{h['role']}: {h.get('content', '')[:200]}" for h in recent)


def _safe_default() -> dict[str, Any]:
    return {
        "intent": "CLARIFY",
        "requires_sql": False,
        "needs_clarification": True,
        "clarification_question": "Could you rephrase or add detail to your question?",
        "reason": "fused intent+route model unavailable; failing safe",
        "tables": [],
        "columns": [],
        "geography_level": "none",
        "operation": "lookup",
        "flow_direction": "none",
        "sort_direction": "none",
        "top_k": None,
        "year_strategy": "",
        "join_plan": "",
        "assumptions": [],
        "confidence": "low",
        "service_unavailable": True,
    }


def classify_and_route(question: str, history: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    system = _SYSTEM.format(domain=domain_summary(), catalog=catalog_for_prompt())
    convo = _history_snippet(history)
    user = (
        (f"Recent conversation:\n{convo}\n\n" if convo else "")
        + _FEWSHOT
        + f"\nClassify and route this message:\n{question}"
    )
    try:
        raw = client.chat_json(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
            max_tokens=500,
            purpose="stage12_intent_route",
        )
    except client.LLMError:
        return _safe_default()
    if not isinstance(raw, dict):
        return _safe_default()

    intent = str(raw.get("intent", "")).strip().upper()
    if intent not in INTENTS:
        intent = "CLARIFY"
    tables = [t for t in (raw.get("tables") or []) if t in _VALID_TABLES] if intent == "ANALYTICAL" else []
    raw_metrics = raw.get("metric_columns") or raw.get("columns") or []
    metric_columns: list[str] = []
    for proposed in raw_metrics:
        proposed_norm = str(proposed).strip().casefold().replace("_", " ")
        for table in tables:
            dataset = get_dataset(table)
            if dataset is None:
                continue
            for column in dataset.metrics:
                if column.casefold().replace("_", " ") == proposed_norm and column not in metric_columns:
                    metric_columns.append(column)
    if any(table.startswith("acs_") for table in tables):
        q_lower = question.lower()
        asks_share = bool(re.search(r"\b(percent|percentage|share|rate)\b", q_lower))
        asks_count = bool(re.search(r"\b(count|number of people|how many people|people, not percent)\b", q_lower))
        if asks_share and not asks_count and len(metric_columns) > 1:
            metric_columns = [
                column for column in metric_columns
                if column not in {"Total population", "# of household"}
            ]
    dimension_count = bool(re.search(
        r"\bhow many\s+(?:states|counties|districts|agencies|rows|records)\b",
        question,
        re.IGNORECASE,
    ))
    missing_measure = intent == "ANALYTICAL" and bool(tables) and not metric_columns and not dimension_count
    needs_clar = (
        bool(raw.get("needs_clarification"))
        or intent == "CLARIFY"
        or (intent == "ANALYTICAL" and not tables)
        or missing_measure
    )
    return {
        "intent": intent,
        "requires_sql": intent == "ANALYTICAL",
        "needs_clarification": needs_clar,
        "clarification_question": (
            "Which exact measure should I calculate?"
            if missing_measure else str(raw.get("clarification_question") or "").strip()
        ),
        "reason": str(raw.get("reason") or "").strip(),
        "tables": tables,
        # Backward-compatible key; now guaranteed to contain canonical metric
        # columns only, rather than a mixture of measures and dimensions.
        "columns": metric_columns,
        "geography_level": str(raw.get("geography_level") or "none"),
        "operation": str(raw.get("operation") or "lookup"),
        "flow_direction": str(raw.get("flow_direction") or "none"),
        "sort_direction": str(raw.get("sort_direction") or "none"),
        "top_k": raw.get("top_k"),
        "year_strategy": str(raw.get("year_strategy") or ""),
        "join_plan": str(raw.get("join_plan") or ""),
        # User-visible assumptions are built from registry facts later; model
        # prose here is intentionally discarded.
        "assumptions": [],
        "confidence": str(raw.get("confidence") or "medium"),
        "clarification": (
            "Which exact measure should I calculate?"
            if missing_measure else str(raw.get("clarification_question") or "").strip()
        ),
        "service_unavailable": False,
    }
