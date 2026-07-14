"""Stage 4a — SQL generation with a self-repair loop.

The LLM writes one DuckDB SELECT from the grounding pack. We validate it with the
existing safety validator, execute it read-only, and on any failure (invalid SQL,
DuckDB error, or an empty result that almost certainly means a bad filter) we
feed the exact error back and let the model fix it, up to a small retry budget.
"""

from __future__ import annotations

import os
import re
from typing import Any

from app.duckdb.connection import execute_select
from app.llm import client
from app.semantic.value_resolver import distinct_values, resolve_filter_value
from app.sql.validator import SqlValidationError, validate_sql

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
                match = resolve_filter_value(table, col, value, min_score=0.4)
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

_AUDIT_SYSTEM = """You audit a SQL query against the question it claims to answer.
You are the last line of defense before results reach a state-government user.

Check ONLY these three failure classes (ignore style, aliases, LIMIT choices):

1. UNREQUESTED FILTER — a WHERE condition that NARROWS scope in a way the
   question never asked for. The classic failure: the question says "state
   average" (a geography concept) and the SQL filters
   agency_name = 'Department of State'. Filtering an agency/industry/category
   is ONLY valid when the question names one.
2. TIME SCOPE — the conventions require the latest single year when the user
   named none. Flag a query that aggregates across multiple years without the
   user asking for a multi-year total/trend, or that picks a non-latest year
   for no stated reason. (A missing year filter on a table that HAS a year
   column = an implicit multi-year sum — flag it.) EXCEPTION: gov_state /
   gov_county / gov_congress are single-snapshot tables — never flag them
   for time scope.
3. DIRECTION — for *_flow tables: "receives/inflow" must use the subawardee_*
   side; "sends/outflow" must use the rcpt_* side.

Return ONLY JSON: {"ok": <bool>, "problems": ["<specific problem>", ...]}
ok=true when none of the three classes applies. Do not invent problems."""


def _audit_scope(question: str, sql: str, conventions: str) -> list[str]:
    """One fast LLM pass over the generated SQL. Returns [] when clean.
    Never raises — an audit failure must not take down the pipeline."""
    try:
        raw = client.chat_json(
            [
                {"role": "system", "content": _AUDIT_SYSTEM},
                {"role": "user", "content": (
                    f"QUESTION: {question}\n\nSQL:\n{sql}\n\n"
                    f"CONVENTIONS IN FORCE:\n{conventions[:1800]}\n\nAudit it."
                )},
            ],
            temperature=0.0,
            max_tokens=300,
            purpose="sql_scope_audit",
        )
        if raw.get("ok"):
            return []
        return [str(x) for x in (raw.get("problems") or [])][:4]
    except Exception:
        return []

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
- Return a focused result: include the label/dimension column(s) and the
  measure(s); ORDER BY the measure and LIMIT when the user asks for "top N".
- Keep every WHERE filter the question requires (scope, year, casing) EXACTLY
  as needed — never drop a filter. Separately, ADD the geographic identifier to
  the SELECT list so results can be mapped: county -> also SELECT `state` and
  `county`; congressional -> also SELECT `cd_118`; state -> also SELECT `state`.
  This is an ADDITIONAL select column, never a replacement for the WHERE clause.
- Prefer correctness over cleverness. One statement only.

Return ONLY JSON: {"sql": "<the query>", "explanation": "<one sentence>"}"""


def _ask_for_sql(messages: list[dict[str, str]]) -> str:
    raw = client.chat_json(messages, temperature=0.0, max_tokens=900, purpose="stage4_sql")
    return str(raw.get("sql") or "").strip()


def generate_and_execute(
    question: str,
    grounding_text: str,
    history: list[dict[str, Any]] | None = None,
    tables: list[str] | None = None,
    exemplar: dict[str, Any] | None = None,
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
        f"QUESTION: {question}\n\nWrite the DuckDB SQL."
    )
    messages: list[dict[str, str]] = [
        {"role": "system", "content": _SYSTEM},
        {"role": "user", "content": base_user},
    ]

    attempts: list[dict[str, Any]] = []
    sql = ""
    audited = False
    for attempt in range(MAX_ATTEMPTS):
        try:
            sql = _ask_for_sql(messages)
        except client.LLMError as exc:
            return {"sql": sql, "rows": [], "error": f"LLM error: {exc}", "attempts": attempts}

        record: dict[str, Any] = {"sql": sql}
        try:
            validate_sql(sql)
            rows = execute_select(sql, max_rows=max_rows)
        except SqlValidationError as exc:
            record["error"] = f"validation: {exc}"
        except Exception as exc:  # duckdb execution error
            record["error"] = f"duckdb: {exc}"
        else:
            record["row_count"] = len(rows)
            attempts.append(record)
            if rows and not audited:
                # Scope audit: catch unrequested filters / wrong time scope /
                # flow-direction mix-ups BEFORE the answer is written. One
                # round only; the critique goes back through the same loop.
                audited = True
                conventions_start = grounding_text.find("ANALYSIS CONVENTIONS")
                conventions = grounding_text[conventions_start:conventions_start + 2000] if conventions_start >= 0 else ""
                problems = _audit_scope(question, sql, conventions)
                if problems:
                    record["scope_audit"] = problems
                    messages += [
                        {"role": "assistant", "content": f'{{"sql": {sql!r}}}'},
                        {"role": "user", "content": (
                            "A scope audit found problems with that query:\n- "
                            + "\n- ".join(problems)
                            + "\nRewrite the SQL to fix them, following the "
                            "ANALYSIS CONVENTIONS. Return corrected JSON only."
                        )},
                    ]
                    continue
            if rows or attempt == MAX_ATTEMPTS - 1:
                return {"sql": sql, "rows": rows, "error": None if rows else "empty_result", "attempts": attempts}
            # Empty result with retries left — likely a bad filter/casing/year.
            probe = _probe_empty_filters(sql, tables or [])
            feedback = (
                "That query returned 0 rows. Re-check the RESOLVED FILTER VALUES "
                "(exact casing), the year handling in the CRITICAL WARNINGS, and "
                "join casing. Return corrected JSON." + probe
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

    return {"sql": sql, "rows": [], "error": attempts[-1].get("error") if attempts else "no sql", "attempts": attempts}
