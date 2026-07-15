"""Stage 4a — SQL generation with a self-repair loop.

The LLM writes one DuckDB SELECT from the grounding pack. We validate it with the
existing safety validator, execute it read-only, and on any failure (invalid SQL,
DuckDB error, or an empty result that almost certainly means a bad filter) we
feed the exact error back and let the model fix it, up to a small retry budget.
"""

from __future__ import annotations

import os
import re
import json
from typing import Any

from app.core.analysis_contract import AnalysisContract
from app.duckdb.connection import execute_select
from app.llm import client
from app.semantic.value_resolver import distinct_values, resolve_filter_value
from app.sql.validator import SqlValidationError, validate_sql
from app.sql.semantic_validator import normalize_generated_sql, validate_semantic_sql

# `LOWER(col) = 'value'` or `col = 'value'` — capture column + literal.
_FILTER_RE = re.compile(
    r"""(?:LOWER\s*\(\s*)?(?:["`]?)(\w+)(?:["`]?)\s*\)?\s*=\s*'([^']+)'""",
    re.IGNORECASE,
)


def _probe_empty_filters(sql: str, tables: list[str]) -> str:
    """When SQL returned 0 rows, look at its `col = 'value'` filters; for any
    that target a known dimension column, suggest the closest real value(s).
    Returns extra feedback text to append; empty string if nothing useful."""
    suggestions: list[str] = []
    seen: set[tuple[str, str]] = set()
    for col, value in _FILTER_RE.findall(sql):
        key = (col.lower(), value.lower())
        if key in seen or col.lower() == "year":
            continue
        seen.add(key)
        for table in tables:
            try:
                match = resolve_filter_value(table, col, value, min_score=0.88)
            except Exception:
                match = None
            if not match or match[0].lower() == value.lower():
                continue
            try:
                values = distinct_values(table, col)
            except Exception:
                values = ()
            v_low = value.lower()
            siblings = [v for v in values if v_low in v.lower() and v != match[0]][:4]
            sibling_txt = f" (also nearby: {siblings})" if siblings else ""
            suggestions.append(
                f"  filter `{col} = {value!r}` found 0 rows; closest real value in "
                f"{table}.{col} is {match[0]!r}{sibling_txt}"
            )
            break
    return ("\n" + "\n".join(suggestions)) if suggestions else ""

MAX_ATTEMPTS = 3

_SYSTEM = """You are a DuckDB SQL expert for a fixed analytics catalog.

Write ONE read-only query (SELECT or WITH ... SELECT) that answers the question,
using ONLY the schema, resolved values, and conventions in the grounding below.

Hard rules:
- Query the mart_<table> views only. No DDL/DML, no PRAGMA, no read_parquet.
- Obey every CRITICAL WARNING (state-name casing, double-quoted columns,
  string vs integer year, gov tables have no year filter, flow-table quirks).
- NEVER invent or guess a filter value. You may only add an equality/LIKE
  filter on a value that is (a) explicitly stated in the question, or (b)
  listed in RESOLVED FILTER VALUES. If the question names a geography LEVEL
  but no specific entity ("which county", "top districts"), do NOT filter on
  that level — GROUP BY / rank across it instead. A fabricated filter that
  returns 0 rows is the worst possible outcome.
- Use the RESOLVED FILTER VALUES exactly as given (exact casing/spelling).
- Quote any column containing a space/comma/ampersand with double quotes.
- Normalize state casing with LOWER() in filters and joins.
- For cross-dataset queries, join on the shared geography. Join year columns
  ONLY when the analysis contract requires the same period in both tables.
  When catalog defaults differ (for example FINRA 2021 and ACS 2023), filter
  each table to its own required period and NEVER equate their year columns.
- Return a focused result: include the label/dimension column(s) and the
  measure(s); ORDER BY the measure and LIMIT when the user asks for "top N".
- Rankings must use a stable tie-breaker after the measure (normally the label
  column), so identical requests cannot reshuffle tied rows between runs.
- Keep every WHERE filter the question requires (scope, year, casing) EXACTLY
  as needed — never drop a filter. Separately, ADD the geographic identifier to
  the SELECT list so results can be mapped: county -> also SELECT `state` and
  `county`; congressional -> also SELECT `cd_118`; state -> also SELECT `state`.
  This is an ADDITIONAL select column, never a replacement for the WHERE clause.
- Prefer correctness over cleverness. One statement only.

Return ONLY JSON: {"sql": "<the query>", "explanation": "<one sentence>"}"""


def _ask_for_sql(messages: list[dict[str, str]]) -> str:
    raw = client.chat_json(messages, temperature=0.0, max_tokens=900, purpose="stage4_sql")
    if not isinstance(raw, dict):
        raise client.LLMError("SQL generator returned a non-object JSON response")
    return str(raw.get("sql") or "").strip()


def generate_and_execute(
    question: str,
    grounding_text: str,
    history: list[dict[str, Any]] | None = None,
    tables: list[str] | None = None,
    exemplar: dict[str, Any] | None = None,
    contract: AnalysisContract | None = None,
    resolved: dict[str, Any] | None = None,
) -> dict[str, Any]:
    max_rows = int(os.getenv("MAX_RETURN_ROWS", "250"))
    exemplar_block = ""
    if exemplar:
        notes = f"\nAnalyst notes: {exemplar['notes']}" if exemplar.get("notes") else ""
        exemplar_block = (
            "\n\nVERIFIED REFERENCE QUERY\n========================\n"
            "An analyst verified this SQL for a SIMILAR (not identical) question. "
            "Reuse its table choice, column quoting, and filter patterns — but "
            "adapt the sort direction, LIMIT, year, geography, and metric to "
            "match the CURRENT question exactly. Do not copy it blindly.\n"
            f"Reference question: {exemplar['question']}\n"
            f"Reference SQL:\n{str(exemplar['sql']).strip()}{notes}\n"
        )
    base_user = (
        f"GROUNDING\n========\n{grounding_text}{exemplar_block}\n\n"
        + (f"ANALYSIS CONTRACT (must satisfy exactly)\n{json.dumps(contract.model_dump(), default=str)}\n\n" if contract else "")
        + f"QUESTION: {question}\n\nWrite the DuckDB SQL."
    )
    messages: list[dict[str, str]] = [
        {"role": "system", "content": _SYSTEM},
        {"role": "user", "content": base_user},
    ]

    attempts: list[dict[str, Any]] = []
    sql = ""
    for attempt in range(MAX_ATTEMPTS):
        try:
            sql = _ask_for_sql(messages)
            if contract is not None:
                sql = normalize_generated_sql(sql, contract)
        except client.LLMError as exc:
            return {"sql": sql, "rows": [], "error": f"LLM error: {exc}", "attempts": attempts, "truncated": False}

        record: dict[str, Any] = {"sql": sql}
        try:
            validate_sql(sql)
            if contract is not None:
                validate_semantic_sql(sql, question, contract, resolved)
            fetched_rows = execute_select(sql, max_rows=max_rows + 1)
            truncated = len(fetched_rows) > max_rows
            rows = fetched_rows[:max_rows]
        except SqlValidationError as exc:
            record["error"] = f"validation: {exc}"
        except Exception as exc:  # duckdb execution error
            record["error"] = f"duckdb: {exc}"
        else:
            record["row_count"] = len(rows)
            attempts.append(record)
            if rows or attempt == MAX_ATTEMPTS - 1:
                return {
                    "sql": sql,
                    "rows": rows,
                    "error": None if rows else "empty_result",
                    "attempts": attempts,
                    "truncated": truncated,
                }
            # Empty result with retries left — likely a bad filter/casing/year.
            probe = _probe_empty_filters(sql, tables or [])
            feedback = (
                "That query returned 0 rows. Re-check the RESOLVED FILTER VALUES "
                "(exact casing), the year handling in the CRITICAL WARNINGS, and "
                "join casing. If cross-dataset tables use different required "
                "periods, filter each table separately and do not join their year "
                "columns. Return corrected JSON." + probe
            )
            messages += [
                {"role": "assistant", "content": f'{{"sql": {sql!r}}}'},
                {"role": "user", "content": feedback},
            ]
            continue

        attempts.append(record)
        messages += [
            {"role": "assistant", "content": f'{{"sql": {sql!r}}}'},
            {
                "role": "user",
                "content": f"That query failed: {record['error']}. "
                f"Fix it and return corrected JSON only.",
            },
        ]

    return {"sql": sql, "rows": [], "error": attempts[-1].get("error") if attempts else "no sql", "attempts": attempts, "truncated": False}
