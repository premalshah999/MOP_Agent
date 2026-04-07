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
from openai import OpenAI

from app.chart_generator import generate_chart_spec
from app.classifier import classify
from app.db import execute_query
from app.formatter import format_result
from app.prompts import (
    CONCEPTUAL_SYSTEM,
    SQL_REPAIR_SYSTEM,
    SQL_SYSTEM_PROMPT,
    build_formatter_prompt,
    build_repair_prompt,
    get_relevant_examples,
    lookup_definition,
)
from app.router import build_schema_context, route_tables
from app.safety import is_safe
from app.sql_utils import extract_sql, prepare_sql

load_dotenv()

# ---------------------------------------------------------------------------
# LLM client
# ---------------------------------------------------------------------------
client = OpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
    timeout=float(os.getenv("DEEPSEEK_TIMEOUT", "45")),
)

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
    for m in os.getenv("SQL_REPAIR_MODELS", "deepseek-chat,deepseek-reasoner").split(",")
    if m.strip()
] or [os.getenv("DEEPSEEK_MODEL", "deepseek-chat")]

DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

# Server-side follow-up context (last SQL per pseudo-session)
_last_query: dict[str, str] = {}  # "question" and "sql"


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


# ---------------------------------------------------------------------------
# Intent classification (with smarter bypass)
# ---------------------------------------------------------------------------
DATA_SIGNALS = [
    "top", "bottom", "highest", "lowest", "how much", "how many",
    "compare", "show me", "list", "rank", "which states", "which counties",
    "which districts", "correlation", "relationship",
]


def _classify_intent(question: str, history: list[dict[str, str]]) -> str:
    q = question.lower().strip()
    words = q.split()

    # Short questions with pronouns/demonstratives are follow-ups when there's history
    words_clean = [w.strip("?.,!") for w in words]
    if len(words_clean) <= 10 and any(w in words_clean for w in ["it", "that", "them", "those", "this"]):
        if history or _last_query.get("sql"):
            return "FOLLOWUP"
    if any(phrase in q for phrase in ["which one", "what one"]):
        if history or _last_query.get("sql"):
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
    if last_answer:
        parts.append(f"Previous answer (first 200 chars): {last_answer}")
    if last_row_count:
        parts.append(f"Previous result had {last_row_count} rows.")
    if last_sql:
        parts.append(f"Previous SQL (modify as needed): {last_sql}")

    q_lower = question.lower().strip()

    # Detect detail-seeking follow-ups ("what is it?", "which flow is it?", "show me the details")
    is_detail_seeking = any(pat in q_lower for pat in DETAIL_SEEKING_PATTERNS)
    # Also catch very short pronoun questions like "what is it?" / "which one?"
    if not is_detail_seeking and len(q_lower.split()) <= 6:
        if any(w in q_lower for w in ["it", "that", "them", "those"]):
            is_detail_seeking = True

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

    if not os.getenv("DEEPSEEK_API_KEY"):
        return {"answer": "Please configure DEEPSEEK_API_KEY to enable answers.", "sql": None, "data": [], "row_count": 0}

    response = client.chat.completions.create(
        model=DEEPSEEK_MODEL,
        max_tokens=CONCEPTUAL_MAX_TOKENS,
        temperature=0,
        messages=[{"role": "system", "content": CONCEPTUAL_SYSTEM}]
        + history[-4:]
        + [{"role": "user", "content": question}],
    )
    return {
        "answer": (response.choices[0].message.content or "").strip(),
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
    response = client.chat.completions.create(
        model=DEEPSEEK_MODEL,
        max_tokens=SQL_MAX_TOKENS,
        temperature=0,
        messages=messages,
    )
    return extract_sql((response.choices[0].message.content or "").strip())


def _generate_sql_structured(messages: list[dict[str, str]]) -> str:
    """Generate SQL via JSON-in-message for reliable structured extraction from DeepSeek."""
    response = client.chat.completions.create(
        model=DEEPSEEK_MODEL,
        max_tokens=SQL_MAX_TOKENS,
        temperature=0,
        messages=messages,
    )
    raw = (response.choices[0].message.content or "").strip()

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
    response = client.chat.completions.create(
        model=model,
        max_tokens=SQL_MAX_TOKENS,
        temperature=0,
        messages=[
            {"role": "system", "content": SQL_REPAIR_SYSTEM},
            {"role": "user", "content": user_prompt},
        ],
    )
    return extract_sql((response.choices[0].message.content or "").strip())


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
    (r"DEEPSEEK_API_KEY", "API key not configured. Add DEEPSEEK_API_KEY to .env."),
]


def _format_error(raw: str) -> str:
    for pattern, friendly in _ERROR_PATTERNS:
        if re.search(pattern, raw, re.IGNORECASE):
            return friendly
    return "Could not complete the query. Try rephrasing your question."


def _user_friendly_error(raw: str) -> str:
    """Ensure errors returned to users are always sanitized."""
    return _format_error(raw)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def ask_agent(question: str, history: list[Any]) -> dict[str, Any]:
    clean_history = _normalize_history(history)

    # Step 1: Classify intent
    intent = _classify_intent(question, clean_history)

    # Step 2: Handle conceptual questions (definitions, explanations)
    if intent == "CONCEPTUAL":
        return _answer_conceptually(question, clean_history)

    # Step 3: Resolve follow-ups
    effective_question = question
    if intent == "FOLLOWUP":
        effective_question = _resolve_followup(question, clean_history)

    # Step 4: Check API key
    if not os.getenv("DEEPSEEK_API_KEY"):
        return {
            "error": "DEEPSEEK_API_KEY is not set. Add it to .env to enable SQL generation.",
            "sql": None,
            "data": [],
            "row_count": 0,
        }

    # Step 5: Route to tables (LLM-assisted)
    table_names = route_tables(effective_question)

    # Step 6: Build schema context
    schema_ctx = build_schema_context(table_names)

    # Step 7: Generate SQL (LLM is the primary and only path)
    sql = _generate_sql(effective_question, schema_ctx, clean_history, table_names)

    # Step 8: Execute with auto-repair
    df, final_sql, error = _execute_with_repair(effective_question, sql, schema_ctx)

    if error:
        return {"error": _user_friendly_error(error), "sql": final_sql, "data": [], "row_count": 0}

    # Check for DATA_NOT_AVAILABLE sentinel
    if (
        len(df.columns) == 1
        and df.columns[0] == "message"
        and len(df) >= 1
        and str(df.iloc[0]["message"]) == "DATA_NOT_AVAILABLE"
    ):
        return {
            "answer": "The requested data is not available in the current dataset.",
            "sql": final_sql,
            "data": [],
            "row_count": 0,
        }

    # Step 9: Format the answer
    answer = format_result(question, df, sql=final_sql)

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
    return result
