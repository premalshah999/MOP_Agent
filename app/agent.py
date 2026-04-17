"""MOP Chat Agent — slim NL-to-SQL orchestrator.

Pipeline:
  classify → route tables → build schema → generate SQL → execute → format answer
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from app.chart_generator import generate_chart_spec
from app.classifier import classify
from app.db import execute_query
from app.formatter import format_result
from app.llm import (
    llm_available,
    llm_complete,
    llm_missing_key_message,
    llm_model,
    llm_reasoner_model,
)
from app.map_intent import build_map_intent
from app.metadata_answerer import answer_metadata_question
from app.plan_verifier import verify_execution_candidate
from app.planner import plan_query
from app.prompts import (
    CONCEPTUAL_SYSTEM,
    SQL_REPAIR_SYSTEM,
    SQL_SYSTEM_PROMPT,
    build_formatter_prompt,
    build_repair_prompt,
    get_relevant_examples,
    lookup_definition,
)
from app.query_frame import infer_query_frame
from app.router import build_schema_context, route_tables
from app.safety import is_safe
from app.sql_utils import extract_sql, prepare_sql

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MAX_RETURN_ROWS = int(os.getenv("MAX_RETURN_ROWS", "250"))
SQL_MAX_TOKENS = int(os.getenv("SQL_MAX_TOKENS", "1400"))
SQL_REPAIR_ATTEMPTS = int(os.getenv("SQL_REPAIR_ATTEMPTS", "3"))
CONCEPTUAL_MAX_TOKENS = int(os.getenv("CONCEPTUAL_MAX_TOKENS", "1800"))
SQL_PREFLIGHT_ENABLED = os.getenv("SQL_PREFLIGHT_ENABLED", "1").strip().lower() not in {"0", "false", "no"}

SQL_REPAIR_MODELS = [
    m.strip()
    for m in os.getenv("SQL_REPAIR_MODELS", f"{llm_model()},{llm_reasoner_model()}").split(",")
    if m.strip()
] or [llm_model()]

ACTIVE_LLM_MODEL = llm_model()

# Server-side follow-up context (last SQL per pseudo-session)
_last_query: dict[str, str] = {}  # "question" and "sql"
_VERIFIER_ANSWER_PREFIX = "VERIFIER_ANSWER::"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _to_json_safe_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    cleaned = df.replace([np.inf, -np.inf], np.nan)
    cleaned = cleaned.astype(object).where(pd.notna(cleaned), None)
    return cleaned.to_dict(orient="records")


def _normalize_history(history: list[Any]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for item in history[-12:]:
        if not isinstance(item, dict):
            continue
        role = item.get("role")
        content = item.get("content")
        if role not in {"user", "assistant"}:
            continue
        if not isinstance(content, str):
            content = json.dumps(content)
        out.append({"role": role, "content": content})
    return out


def _verifier_answer_frame(answer: str) -> pd.DataFrame:
    return pd.DataFrame({"message": [f"{_VERIFIER_ANSWER_PREFIX}{answer}"]})


def _clarification_answer(frame, question: str) -> str | None:
    q = question.lower().strip()

    if frame.intent == "compare" and len(frame.state_names) >= 2 and frame.metric_hint is None:
        states = " and ".join(" ".join(part.capitalize() for part in state.split()) for state in frame.state_names[:3])
        return (
            f"**I can compare {states}, but I need the metric first.** "
            f"Good options in this project are total liabilities, total assets, contracts, grants, median household income, poverty, or financial literacy."
        )

    if (
        len(frame.state_names) == 1
        and frame.metric_hint is None
        and any(token in q for token in ("tell me about", "show me", "show", "profile", "overview", "open"))
    ):
        state_label = " ".join(part.capitalize() for part in frame.state_names[0].split())
        return (
            f"**I can do that, but `{state_label}` needs a dimension.** "
            f"For example: total liabilities, contracts, grants, poverty, household income, or financial literacy."
        )

    if frame.metric_hint is None and frame.family is None and frame.intent in {"compare", "ranking"}:
        return (
            "**I need the measure before I answer this reliably.** "
            "Please name the metric you want, such as liabilities, contracts, grants, poverty, household income, or financial literacy."
        )

    return None


# ---------------------------------------------------------------------------
# Intent classification (with smarter bypass)
# ---------------------------------------------------------------------------
DATA_SIGNALS = [
    "top", "bottom", "highest", "lowest", "how much", "how many",
    "compare", "show me", "list", "rank", "which states", "which counties",
    "which districts", "correlation", "relationship",
]
CONTEXTUAL_FOLLOWUP_SIGNALS = [
    "how about",
    "what about",
    "where does",
    "where is",
    "and ",
    "what's ",
    "whats ",
]


def _classify_intent(question: str, history: list[dict[str, str]]) -> str:
    q = question.lower().strip()
    words = q.split()
    frame = infer_query_frame(question)
    has_explicit_metric_context = frame.metric_hint is not None or frame.family is not None

    # Short questions with pronouns/demonstratives are follow-ups when there's history
    words_clean = [w.strip("?.,!") for w in words]
    if len(words_clean) <= 10 and any(w in words_clean for w in ["it", "that", "them", "those", "this"]):
        if history or _last_query.get("sql"):
            return "FOLLOWUP"
    if any(phrase in q for phrase in ["which one", "what one"]):
        if history or _last_query.get("sql"):
            return "FOLLOWUP"

    # Short contextual geography/entity questions should inherit the previous metric instead of
    # being treated as brand-new free-form prompts.
    if history or _last_query.get("sql"):
        has_contextual_signal = any(signal in q for signal in CONTEXTUAL_FOLLOWUP_SIGNALS)
        has_entity_only = bool(frame.state_names or frame.geo_level or frame.wants_agency_dimension)
        missing_metric_context = frame.metric_hint is None and frame.family is None
        if (
            len(words_clean) <= 8
            and has_entity_only
            and (
                missing_metric_context
                or (has_contextual_signal and not has_explicit_metric_context)
            )
        ):
            return "FOLLOWUP"

    # Only bypass to DATA_QUERY for clear data-seeking patterns
    if any(sig in q for sig in DATA_SIGNALS):
        return "DATA_QUERY"
    return classify(question)


# ---------------------------------------------------------------------------
# Follow-up resolution
# ---------------------------------------------------------------------------
DETAIL_SEEKING_PATTERNS = [
    "what is it", "what are they", "which is it", "which one", "which are they",
    "what flow is it", "which flow", "what state", "which state", "what agency",
    "show me the", "show the details", "show details", "what are the details",
    "tell me more", "more details", "elaborate", "break it down", "drill down",
    "what specifically", "which specifically", "can you show",
]


def _resolve_followup(question: str, history: list[dict[str, str]]) -> str:
    """Augment a follow-up question with prior query context."""
    last_q = _last_query.get("question", "")
    last_sql = _last_query.get("sql", "")

    if not last_q and not last_sql:
        last_user = next((m["content"] for m in reversed(history) if m["role"] == "user"), "")
        if last_user:
            last_q = last_user

    last_answer = _last_query.get("answer_snippet", "")
    last_row_count = _last_query.get("row_count", 0)

    parts = [f"Follow-up request: {question}"]
    if last_q:
        parts.append(f"Previous question: {last_q}")
    if last_row_count:
        parts.append(f"Previous result had {last_row_count} rows.")
    if last_sql:
        parts.append(f"Previous SQL (modify as needed): {last_sql}")

    q_lower = question.lower().strip()
    frame = infer_query_frame(question)

    # If the follow-up already names the metric/family explicitly, keep it self-contained
    # instead of leaking previous-answer entities into the rewritten question.
    if frame.metric_hint is not None and frame.family is not None:
        return question

    if last_q:
        # Compact geography/entity follow-ups should inherit the prior metric directly.
        short_contextual = (
            len(q_lower.split()) <= 8
            and frame.state_names
            and frame.metric_hint is None
        )
        if short_contextual:
            if frame.state_names:
                state_label = " ".join(part.capitalize() for part in frame.state_names[0].split())
                return f"{last_q} For {state_label}, show the current value and rank."

    # Detect detail-seeking follow-ups ("what is it?", "which flow is it?", "show me the details")
    is_detail_seeking = any(pat in q_lower for pat in DETAIL_SEEKING_PATTERNS)
    # Also catch very short pronoun questions like "what is it?" / "which one?"
    if not is_detail_seeking and len(q_lower.split()) <= 6:
        if any(w in q_lower for w in ["it", "that", "them", "those"]):
            is_detail_seeking = True

    needs_answer_snippet = (
        is_detail_seeking
        or frame.metric_hint is None
        or frame.family is None
        or any(w in q_lower.split() for w in ["it", "that", "them", "those", "this", "these"])
    )

    if last_answer and needs_answer_snippet:
        parts.append(f"Previous answer (first 200 chars): {last_answer}")

    if is_detail_seeking and last_sql:
        parts.append(
            "IMPORTANT: The user wants to see the SPECIFIC record(s) or full details from the "
            "previous query result. Modify the previous SQL to SELECT ALL relevant columns "
            "(SELECT * or include identifying columns like name, state, agency, description, amount) "
            "and keep the same filtering/ordering. Use LIMIT 1 if they asked about a single record "
            "(e.g. 'the largest', 'the highest'), or a small LIMIT for 'top N'. "
            "Do NOT return aggregates — return the actual rows with all detail columns."
        )

    # Detect drill-down questions that add a new dimension (e.g., "what department?")
    _DRILLDOWN_PATTERNS = [
        "what department", "which department", "what agency", "which agency",
        "what industry", "which industry", "what year", "which year", "when was",
        "by agency", "by department", "by year", "broken down",
    ]
    is_drilldown = any(pat in q_lower for pat in _DRILLDOWN_PATTERNS)
    if is_drilldown and last_sql:
        parts.append(
            "IMPORTANT: The user is asking for a BREAKDOWN of a previously aggregated result. "
            "The previous query used SUM() or aggregation that hid detail columns. Now the user "
            "wants to see those details (e.g., by agency, by year). "
            "Add the requested dimension to the GROUP BY and SELECT, but keep the same WHERE "
            "filters from the previous query. ORDER BY the amount column DESC so the largest "
            "contributor appears first. Include the total in a window function or note it so the "
            "user can see how the breakdown relates to the previous total."
        )

    # Add anti-sycophancy instruction for challenges/corrections
    if any(phrase in q_lower for phrase in ["isn't it", "shouldn't it be", "are you sure", "i think it's", "but what about", "actually it's", "no it's"]):
        parts.append(
            "NOTE: The user may be challenging the previous result. Generate SQL that objectively "
            "answers their question from the data. Do NOT assume the user's suggestion is correct — "
            "let the data speak for itself."
        )

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Conceptual answer
# ---------------------------------------------------------------------------
def _answer_conceptually(question: str, history: list[dict[str, str]]) -> dict[str, Any]:
    # Check static definitions first (zero LLM cost)
    definition = lookup_definition(question)
    if definition:
        return {"answer": definition, "sql": None, "data": [], "row_count": 0}

    if not llm_available():
        return {"answer": llm_missing_key_message(), "sql": None, "data": [], "row_count": 0}

    answer = llm_complete(
        [{"role": "system", "content": CONCEPTUAL_SYSTEM}]
        + history[-4:]
        + [{"role": "user", "content": question}],
        model=ACTIVE_LLM_MODEL,
        max_tokens=CONCEPTUAL_MAX_TOKENS,
        temperature=0,
    )
    return {
        "answer": answer.strip(),
        "sql": None,
        "data": [],
        "row_count": 0,
    }


STRUCTURED_SQL_ENABLED = os.getenv("STRUCTURED_SQL_ENABLED", "1").strip().lower() not in {"0", "false", "no"}


# ---------------------------------------------------------------------------
# SQL generation (LLM — the primary path)
# ---------------------------------------------------------------------------
def _generate_sql(question: str, schema_ctx: str, history: list[dict[str, str]], table_names: list[str]) -> str:
    examples = get_relevant_examples(table_names)
    system_prompt = SQL_SYSTEM_PROMPT.format(
        schema_context=schema_ctx,
        examples=examples,
    )
    messages: list[dict[str, str]] = [{"role": "system", "content": system_prompt}]
    # Include last 4 messages of history for context (not full 12)
    messages.extend(history[-4:])
    messages.append({"role": "user", "content": question})

    if STRUCTURED_SQL_ENABLED:
        return _generate_sql_structured(messages)

    # Fallback: free-text generation
    raw = llm_complete(
        messages,
        model=ACTIVE_LLM_MODEL,
        max_tokens=SQL_MAX_TOKENS,
        temperature=0,
    )
    return extract_sql(raw.strip())


def _generate_sql_structured(messages: list[dict[str, str]]) -> str:
    """Generate SQL via JSON-in-message for reliable structured extraction from DeepSeek."""
    raw = llm_complete(
        messages,
        model=ACTIVE_LLM_MODEL,
        max_tokens=SQL_MAX_TOKENS,
        temperature=0,
    ).strip()

    # Try to parse JSON response (the prompt asks for {"reasoning": ..., "sql": ...})
    try:
        # Find JSON object in response
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            parsed = json.loads(match.group())
            sql = parsed.get("sql", "")
            if sql:
                return extract_sql(sql)
    except (json.JSONDecodeError, KeyError):
        pass

    # Fallback: treat entire response as raw SQL
    return extract_sql(raw)


# ---------------------------------------------------------------------------
# SQL repair
# ---------------------------------------------------------------------------
def _repair_sql(question: str, failed_sql: str, error: str, schema_ctx: str, model: str) -> str:
    user_prompt = build_repair_prompt(question, failed_sql, error, schema_ctx)
    raw = llm_complete(
        [
            {"role": "system", "content": SQL_REPAIR_SYSTEM},
            {"role": "user", "content": user_prompt},
        ],
        model=model,
        max_tokens=SQL_MAX_TOKENS,
        temperature=0,
    )
    return extract_sql(raw.strip())


# ---------------------------------------------------------------------------
# Preflight check (EXPLAIN)
# ---------------------------------------------------------------------------
def _preflight_sql(sql: str) -> Optional[str]:
    if not SQL_PREFLIGHT_ENABLED:
        return None
    try:
        execute_query(f"EXPLAIN {sql}")
        return None
    except Exception as exc:
        return str(exc)


# ---------------------------------------------------------------------------
# Result validation — catch empty results, unreasonable row counts
# ---------------------------------------------------------------------------
RESULT_VALIDATION_ENABLED = os.getenv("RESULT_VALIDATION_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
MAX_SANE_ROWS = int(os.getenv("MAX_SANE_ROWS", "100000"))


def _validate_result(df: pd.DataFrame, question: str) -> Optional[str]:
    """Return a hint string if the result looks wrong, or None if it's fine."""
    if not RESULT_VALIDATION_ENABLED:
        return None

    # Empty result — most common silent failure
    if df.empty:
        hints = []
        q = question.lower()
        # Common causes of empty results
        if any(k in q for k in ["contract", "spending", "grant"]):
            hints.append("contract/spending tables use year as VARCHAR — use year = '2024' not year = 2024")
        if any(k in q for k in ["acs", "census", "population", "poverty"]):
            hints.append("ACS tables use Year as INTEGER — use Year = 2023 not Year = '2023'")
        if any(k in q for k in ["gov", "debt", "pension", "liabilities"]):
            hints.append("gov_* tables have no year column — do NOT add a year filter")
        hints.append("check that state name casing matches the table (use LOWER() for cross-table joins)")
        return "Query returned 0 rows. Likely causes: " + "; ".join(hints)

    # Unreasonably many rows — probably missing a LIMIT or wrong JOIN
    if len(df) > MAX_SANE_ROWS:
        return f"Query returned {len(df):,} rows which seems excessive. Did you forget a LIMIT or use a wrong JOIN?"

    return None


# ---------------------------------------------------------------------------
# Execute with auto-repair + result validation
# ---------------------------------------------------------------------------
def _execute_with_repair(
    question: str, sql: str, schema_ctx: str, history: list[dict[str, str]] | None = None, table_names: list[str] | None = None,
) -> tuple[Optional[pd.DataFrame], str, Optional[str]]:
    prepared = prepare_sql(sql, question)
    if not prepared:
        return None, "", "SQL generation returned an empty query."
    if not is_safe(prepared):
        return None, prepared, "Query rejected by safety validator."

    last_sql = prepared
    errors: list[str] = []

    verification = verify_execution_candidate(question, last_sql, table_names)
    if verification.answer:
        return _verifier_answer_frame(verification.answer), last_sql, None
    if verification.error:
        errors.append(verification.error)
    else:
        # Try preflight
        preflight_err = _preflight_sql(last_sql)
        if preflight_err:
            errors.append(preflight_err)
        else:
            try:
                df = execute_query(last_sql)
                # Validate result — catch empty/unreasonable results
                validation_hint = _validate_result(df, question)
                if validation_hint is None:
                    return df, last_sql, None
                # Result is suspicious — treat as an error for repair
                errors.append(validation_hint)
            except Exception as exc:
                errors.append(str(exc))

    # Repair loop
    for attempt in range(SQL_REPAIR_ATTEMPTS):
        model = SQL_REPAIR_MODELS[attempt % len(SQL_REPAIR_MODELS)]
        try:
            repaired = _repair_sql(question, last_sql, errors[-1], schema_ctx, model)
        except Exception:
            continue

        repaired = prepare_sql(repaired, question)
        if not repaired or not is_safe(repaired):
            continue

        verification = verify_execution_candidate(question, repaired, table_names)
        if verification.answer:
            return _verifier_answer_frame(verification.answer), repaired, None
        if verification.error:
            errors.append(verification.error)
            last_sql = repaired
            continue

        preflight_err = _preflight_sql(repaired)
        if preflight_err:
            errors.append(preflight_err)
            last_sql = repaired
            continue

        try:
            df = execute_query(repaired)
            validation_hint = _validate_result(df, question)
            if validation_hint is None:
                return df, repaired, None
            # Still suspicious — continue repair loop
            errors.append(validation_hint)
            last_sql = repaired
        except Exception as exc:
            errors.append(str(exc))
            last_sql = repaired

    # If the last attempt produced data (even if suspicious), return it rather than an error
    verification = verify_execution_candidate(question, last_sql, table_names)
    if verification.answer:
        return _verifier_answer_frame(verification.answer), last_sql, None
    if verification.error:
        return None, last_sql, verification.error

    try:
        df = execute_query(last_sql)
        if not df.empty:
            return df, last_sql, None
    except Exception:
        pass

    return None, last_sql, _format_error(" | ".join(errors[-3:]))


# ---------------------------------------------------------------------------
# User-friendly error messages
# ---------------------------------------------------------------------------
_ERROR_PATTERNS = [
    (r"same column count", "The query tried to combine incompatible result sets. Try rephrasing with a simpler comparison."),
    (r"Catalog Error.*does not exist", "The query referenced a table or column that doesn't exist in the dataset."),
    (r"Binder Error", "The query has a structural error. Try asking the question differently."),
    (r"conversion error", "Data type mismatch — this often happens with year columns. Try rephrasing."),
    (r"Parser Error", "SQL syntax error. Try rephrasing your question more simply."),
    (r"(DEEPSEEK_API_KEY|GEMINI_API_KEY|LLM_API_KEY)", "LLM API key not configured. Add GEMINI_API_KEY, LLM_API_KEY, or DEEPSEEK_API_KEY to .env."),
]
_VERIFIER_ERROR_MARKERS = (
    "Relative-exposure questions",
    "Congressional district query",
    "Share-style questions",
    "Jobs-focused questions",
    "Expected explicit period filter",
    "Inflow ",
    "Outflow ",
    "Displayed-flow questions",
    "Internal-flow questions",
    "Agency flow breakdowns",
    "Industry flow breakdowns",
)


def _format_error(raw: str) -> str:
    for pattern, friendly in _ERROR_PATTERNS:
        if re.search(pattern, raw, re.IGNORECASE):
            return friendly
    return "Could not complete the query. Try rephrasing your question."


def _user_friendly_error(raw: str) -> str:
    """Ensure errors returned to users are always sanitized."""
    if any(marker in raw for marker in _VERIFIER_ERROR_MARKERS):
        return raw
    return _format_error(raw)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def ask_agent(question: str, history: list[Any]) -> dict[str, Any]:
    clean_history = _normalize_history(history)
    initial_frame = infer_query_frame(question)

    # Step 1: Classify intent
    intent = _classify_intent(question, clean_history)

    # Step 2: Resolve follow-ups
    effective_question = question
    if intent == "FOLLOWUP":
        effective_question = _resolve_followup(question, clean_history)

    # Step 3: Handle deterministic metadata / availability / safety questions
    metadata_answer = answer_metadata_question(effective_question)
    if metadata_answer:
        return {"answer": metadata_answer, "sql": None, "data": [], "row_count": 0, "mapIntent": {"enabled": False, "mapType": "none"}}

    # Step 4: Handle conceptual questions (definitions, explanations)
    if intent == "CONCEPTUAL":
        conceptual = _answer_conceptually(question, clean_history)
        conceptual["mapIntent"] = {"enabled": False, "mapType": "none"}
        return conceptual

    # Step 5: Try deterministic metadata-driven planning first
    plan = plan_query(effective_question)
    table_names = plan.table_names if plan else route_tables(effective_question)
    schema_ctx = build_schema_context(table_names)

    df: Optional[pd.DataFrame]
    final_sql: str
    error: Optional[str]

    if plan:
        df, final_sql, error = _execute_with_repair(
            effective_question,
            plan.sql,
            schema_ctx,
            table_names=plan.table_names,
        )
    else:
        clarification = _clarification_answer(infer_query_frame(effective_question), effective_question)
        if clarification:
            return {
                "answer": clarification,
                "sql": None,
                "data": [],
                "row_count": 0,
                "mapIntent": {"enabled": False, "mapType": "none"},
            }
        # Step 6: LLM fallback only when deterministic planning cannot cover the question
        if not llm_available():
            return {
                "error": llm_missing_key_message(),
                "sql": None,
                "data": [],
                "row_count": 0,
                "mapIntent": {"enabled": False, "mapType": "none"},
            }
        sql = _generate_sql(effective_question, schema_ctx, clean_history, table_names)
        df, final_sql, error = _execute_with_repair(
            effective_question,
            sql,
            schema_ctx,
            table_names=table_names,
        )

    # If the deterministic planner failed, fall back to LLM before returning an error.
    if error and plan and llm_available():
        clarification = _clarification_answer(infer_query_frame(effective_question), effective_question)
        if clarification and initial_frame.metric_hint is None:
            return {
                "answer": clarification,
                "sql": None,
                "data": [],
                "row_count": 0,
                "mapIntent": {"enabled": False, "mapType": "none"},
            }
        table_names = route_tables(effective_question)
        schema_ctx = build_schema_context(table_names)
        sql = _generate_sql(effective_question, schema_ctx, clean_history, table_names)
        df, final_sql, error = _execute_with_repair(
            effective_question,
            sql,
            schema_ctx,
            table_names=table_names,
        )

    if error:
        return {"error": _user_friendly_error(error), "sql": final_sql, "data": [], "row_count": 0, "mapIntent": {"enabled": False, "mapType": "none"}}

    # Check for DATA_NOT_AVAILABLE sentinel
    if (
        len(df.columns) == 1
        and df.columns[0] == "message"
        and len(df) >= 1
    ):
        message = str(df.iloc[0]["message"])
        if message == "DATA_NOT_AVAILABLE":
            return {
                "answer": "The requested data is not available in the current dataset.",
                "sql": final_sql,
                "data": [],
                "row_count": 0,
                "mapIntent": {"enabled": False, "mapType": "none"},
            }
        if message.startswith(_VERIFIER_ANSWER_PREFIX):
            return {
                "answer": message.removeprefix(_VERIFIER_ANSWER_PREFIX),
                "sql": final_sql,
                "data": [],
                "row_count": 0,
                "mapIntent": {"enabled": False, "mapType": "none"},
            }

    # Step 9: Format the answer
    answer = format_result(effective_question, df, sql=final_sql)

    # Step 10: Generate chart spec
    chart_spec = generate_chart_spec(df, question, sql=final_sql)

    # Save context for follow-ups (include answer snippet for pronoun resolution)
    _last_query["question"] = question
    _last_query["sql"] = final_sql
    _last_query["answer_snippet"] = answer[:200] if answer else ""
    _last_query["row_count"] = len(df)

    result: dict[str, Any] = {
        "answer": answer,
        "sql": final_sql,
        "data": _to_json_safe_records(df.head(MAX_RETURN_ROWS)),
        "row_count": len(df),
    }
    if chart_spec:
        result["chart"] = chart_spec
    result["mapIntent"] = build_map_intent(effective_question, df, table_names)
    return result
