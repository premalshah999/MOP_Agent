"""Stage 2 — Question Routing (the highest-leverage stage).

Given the question, the LLM picks the minimal set of catalog tables needed,
grounded in the full compact catalog. Wrong table = wrong answer, so this stage
is conservative: it returns the exact table ids from the catalog and flags
genuinely ambiguous routing for clarification.
"""

from __future__ import annotations

from typing import Any

from app.llm import client
from app.semantic.registry import catalog_for_prompt, load_registry

_VALID_TABLES = set(load_registry().datasets)

_SYSTEM = """You route US public-policy data questions to the correct table(s).

Pick the SMALLEST set of tables that answers the question. Use the catalog below;
table ids must be copied EXACTLY.

CATALOG
=======
{catalog}

ROUTING RULES
- Geography: choose the table whose suffix matches the asked grain —
  _state, _county, or _congress. "congressional district" -> _congress.
  If NO geography grain is named ("which state", "where is", bare ranking),
  default to the _state table only. Never return multiple grains of the same
  family — pick exactly one.
- Federal awards by AGENCY (which agency, department of X, DoD/HHS...) ->
  spending_state_agency. Federal awards WITHOUT an agency split -> contract_*.
- contract_state is the PRIMARY state-level awards table (contracts, grants,
  direct payments, employees). Use spending_state only if the user explicitly
  asks for the spending-category breakdown view; otherwise prefer contract_state.
- "free cash flow" / "cash flow" / fiscal health / assets / liabilities / debt
  ratio / pension -> gov_*. NEVER the *_flow tables for "cash flow".
- "subaward" / "subcontract" / money flowing between places -> *_flow
  (state_flow / county_flow / congress_flow).
- demographics / population / race / education / income / poverty -> acs_*.
- financial literacy / financial stress / risk aversion -> finra_*.
- Cross-dataset questions ("X and their Y" from different families) -> return
  BOTH tables.
- Same-table per-capita/share variants stay in the SAME table.
- If two different tables are equally plausible and the choice changes the
  answer, set needs_clarification=true and ask which one.

Return ONLY JSON:
{{"tables": ["<exact ids>"],
 "columns": ["<key measure/filter columns you expect to use>"],
 "geography_level": "state|county|congress|none",
 "year_strategy": "<which year/period to use, or 'no year filter'>",
 "join_plan": "<how to join if >1 table, else empty>",
 "needs_clarification": <bool>,
 "clarification": "<question to ask if ambiguous, else empty>",
 "confidence": "high|medium|low",
 "reason": "<short>"}}"""

_FEWSHOT = """Examples:
  "top 10 counties in maryland by grants" -> ["contract_county"]
  "which agencies give the most grants to Maryland" -> ["spending_state_agency"]
  "Maryland congressional districts by free cash flow" -> ["gov_congress"]
  "subcontract inflow to Maryland" -> ["state_flow"]
  "where is the maximum asian population by count" -> ["acs_state"]
  "top 10 states by debt ratio" -> ["gov_state"]
  "top counties in Maryland by college degree attainment" -> ["acs_county"]
  "financial stress in texas counties" -> ["finra_county"]
  "states with highest financial literacy and their government debt ratio" -> ["finra_state","gov_state"]
"""


def _history_snippet(history: list[dict[str, Any]] | None) -> str:
    if not history:
        return ""
    recent = [h for h in history if h.get("role") in {"user", "assistant"}][-4:]
    return "\n".join(f"{h['role']}: {h.get('content', '')[:200]}" for h in recent)


def route(question: str, history: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    system = _SYSTEM.format(catalog=catalog_for_prompt())
    convo = _history_snippet(history)
    user = (
        (f"Recent conversation (for follow-ups):\n{convo}\n\n" if convo else "")
        + _FEWSHOT
        + f"\nRoute this question:\n{question}"
    )
    try:
        raw = client.chat_json(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
            max_tokens=400,
            purpose="stage2_routing",
        )
    except client.LLMError:
        return {
            "tables": [],
            "columns": [],
            "geography_level": "none",
            "year_strategy": "",
            "join_plan": "",
            "needs_clarification": True,
            "clarification": "Which dataset should I use for this question?",
            "confidence": "low",
            "reason": "routing model unavailable",
        }

    tables = [t for t in (raw.get("tables") or []) if t in _VALID_TABLES]
    return {
        "tables": tables,
        "columns": list(raw.get("columns") or []),
        "geography_level": str(raw.get("geography_level") or "none"),
        "year_strategy": str(raw.get("year_strategy") or ""),
        "join_plan": str(raw.get("join_plan") or ""),
        "needs_clarification": bool(raw.get("needs_clarification")) or not tables,
        "clarification": str(raw.get("clarification") or "").strip(),
        "confidence": str(raw.get("confidence") or "medium"),
        "reason": str(raw.get("reason") or "").strip(),
    }
