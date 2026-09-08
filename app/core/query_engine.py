"""SQL generation, validation, repair, and execution.

The LLM writes one DuckDB SELECT from the grounding pack. We validate it with the
existing safety validator, execute it read-only, and on any failure (invalid SQL,
DuckDB error, or an empty result that almost certainly means a bad filter) we
feed the exact error back and let the model fix it, up to a small retry budget.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

from app.core.analysis_plan import AnalysisContract
from app.duckdb.connection import execute_select
from app.llm import client
from app.semantic.value_resolver import distinct_values, resolve_filter_value
from app.sql.semantic_validator import (
    normalize_generated_sql,
    validate_result_shape,
    validate_semantic_sql,
)
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
- The ANALYSIS CONTRACT is the authoritative interpretation. In particular,
  obey `statistic`, `result_unit`, ordered `formula.operands`, `formula.scale`,
  the typed `predicate`,
  `observation_grain`, `result_scope`, `sort_direction`, `top_k`,
  `include_component_measures`, and `output_dimensions`. Do not reinterpret
  the user's prose into different semantics at this stage.
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
  The ANALYSIS CONTRACT's `period_by_table` is authoritative for those
  table-specific filters.
- DOCUMENTED CONTRACT DERIVATIONS: the source dictionary defines
  Sub-contract Out as the rcpt/origin-side SUM of subaward amount;
  Sub-Contract In as the subawardee/destination-side SUM; Net Sub-Contract as
  inflow minus outflow. Build these from the matching contract_* and *_flow
  tables at one geography and compatible periods. These are expressions, not
  physical columns. For percentages use 100.0 * numerator / NULLIF(denominator, 0).
  When the user asks for all geographies, use the contract table as the base
  and LEFT JOIN aggregated flows so zero-flow geographies are not dropped;
  COALESCE the missing event aggregate to 0 before a ratio or correlation.
  Never combine state_flow (no year field) with a default 2024 table in one
  derived value unless the analysis contract explicitly records that the user
  accepted the mixed scopes.
- Do not generate SQL for Federal Contracts (Indirect). The runtime lacks the
  newest dictionary's `fed_act_obl_indirect` field, and the older dictionary's
  Contracts + Net Sub-Contract formula conflicts with that definition.
- FLOW/CONTRACT JOIN KEYS: state uses normalized state names; county uses
  contract_county.county_fips = county_flow.rcpt_cty for outflow and
  = subawardee_cty for inflow. Congressional flow integer codes can be joined
  only after canonical conversion of contract_congress.cd_118:
  CAST(CONCAT(CAST(state_fips AS INTEGER), RIGHT(cd_118, 2)) AS INTEGER),
  matched to prime_awardee_stcd118 (outflow) or subawardee_stcd118 (inflow).
  Never compare the human-readable 'Maryland CD-08' name directly with 'MD-08'.
  ACS/FINRA/government congressional tables do not carry state_fips. When one
  of them is joined to congress_flow, use the routed contract_congress table as
  a DISTINCT cd_118 -> state_fips bridge (filtered to its planned/default year),
  then apply the canonical conversion.
- Return a focused result: include the label/dimension column(s) and the
  measure(s); ORDER BY the measure and LIMIT when the user asks for "top N".
- Give every derived output a meaningful alias based on the analysis
  contract's formula.output_label (for example inflow_per_resident), never a
  generic alias such as value, v, metric, or rate. The alias is part of the
  evidence contract used by tables and visualizations.
- A correlation query must return both CORR(x, y) AS correlation and the count
  of non-null paired observations AS sample_size. The sample size must count
  the same valid pairs used by CORR, not unmatched rows from an outer join.
- For joined threshold/median analyses with different missing-value patterns,
  return the eligible geography count and a separate non-null sample count for
  each computed statistic. If the question asks about exclusions or coverage,
  also return the missing geography labels. Never label a metric-specific
  median as computed over every geography when its source column contains nulls.
- If the question requests outliers, implement and expose a defensible method
  (for example IQR, z-score, or residual distance), return the actual outlier
  rows and method inputs, and do not substitute a min/max range.
- For a correlation matrix, either return one clearly labeled paired sample
  count for EACH coefficient, or first filter every matrix variable to non-null
  complete cases and return one shared sample_size. Never attach the first
  pair's count to every coefficient when missing-value patterns may differ.
- Prefer the compact complete-case shape for a correlation matrix: one CTE
  that filters every matrix variable to non-null, followed by one SELECT with
  all coefficient aliases and one shared sample_size. This is less error-prone
  than a long UNION. If a UNION is necessary, use SQL-standard doubled single
  quotes inside labels (for example 'Bachelor''s'), never backslash escaping.
- In DuckDB, use MEDIAN(value) or QUANTILE_CONT(value, fraction) for percentile
  summaries. Do not generate PERCENTILE_CONT ... WITHIN GROUP syntax.
- For a one-year correlation within one named state, filter the routed county
  table to that state and correlate across its counties. Never collapse to the
  state table's single row or group to one row before computing CORR.
- When `formula.operator=divide` and `include_component_measures=false`, return
  only the planned output dimensions and final derived measure. Use numerator
  and denominator internally, but do not expose them as result columns.
- Obey the ANALYSIS CONTRACT's operation. For operation=aggregate, a request
  for one total/number/average must compute the measure with SUM/AVG/COUNT as
  appropriate and return one result row (or one row per explicitly named
  comparison entity). Do not replace the requested metric with COUNT(*) and
  do not return every geography as a lookup table. Only GROUP BY when the user
  asks for a breakdown ("by state", "each county", "by agency", etc.).
- Each schema measure includes a `default-row-aggregation`. Use it when
  combining rows unless the question explicitly requests another statistic.
  A dataset's physical row count is never the answer to "how many <measure>".
- For a request asking how many unique geographies a dataset contains, use
  COUNT(DISTINCT <geography key>) and apply the analysis contract's period.
  Do not confuse distinct geography coverage with physical rows across years.
- Never answer a requested demographic intersection by multiplying marginal
  percentage columns. Never use a denominator merely because it is numeric:
  follow the exact denominator stated in the schema and critical warnings. If
  the required joint field or denominator is absent, no valid SQL exists; do
  not substitute Total population or a broader category.
- Never convert a FINRA share or index into a resident count by multiplying it
  by ACS population. The runtime FINRA data has no documented population
  denominator, survey weights, or respondent count.
- For arithmetic follow-ups, use exactly the operands in the current
  standalone question. Do not carry a current-assets term into a new request
  that asks only for Total_Assets - Total_Liabilities.
- For `formula.operator=subtract`, compute `formula.operands[0] -
  formula.operands[1]` exactly. Never substitute an equivalent field whose
  displayed sign or label differs from the planned formula.
- The contract_* "Per 1000" fields are published normalized fields with an
  untraceable denominator and must never be relabeled as per capita/per 1,000
  residents. For state-level published per-1,000-resident measures use
  spending_state when that is the routed table. Never recompute or silently
  reinterpret a stored normalized field.
- The '2020-2024' federal-spending row is a precomputed summary of undocumented
  aggregation type. Filter to it only when requested; never SUM it with 2024
  or write SQL that assumes it equals the sum of five annual rows.
- FLOW SIDE FILTERS: for a named inflow geography, the equality/IN filter must
  use subawardee_*; for a named outflow geography, it must use rcpt_* or the
  congress_flow prime_awardee_stcd118 origin identifier. A column
  from the correct side merely appearing in SELECT/GROUP BY does not satisfy
  this rule. Agency breakdowns group by agency_name; industry breakdowns group
  by naics_2digit_title.
- FLOW RANKING DIMENSION: "which geography receives the most inflow" groups
  and selects the subawardee/destination geography; "which geography sends the
  most outflow" groups and selects the rcpt/origin geography. Do not rank the
  opposite side unless the question explicitly asks for sources/origins or
  destinations.
- For `formula.operator=net_flow`, net subaward/subcontract flow for a place is destination inflow
  minus origin outflow. Compute two conditional SUMs over the same flow table
  (subawardee_* match minus rcpt_* match) and return their subtraction. Never
  relabel a destination-only inflow total as net flow.
- Rankings must use a stable tie-breaker after the measure (normally the label
  column), so identical requests cannot reshuffle tied rows between runs.
- For a base-measure ranking, ORDER BY the exact `sort_columns` in their typed
  priority order, then add a stable label. Other `metric_columns` may be
  accompanying display values and must not silently become ranking keys. For
  a derived-formula ranking, order by the derived result, then the label.
- Preserve `result_scope`: full means no LIMIT; single means one row; top_n
  means LIMIT exactly `top_k`; grouped means one row per planned group.
- For count_distinct, use the table's stable geography key rather than listing
  rows. When the question asks for counts at two levels (for example unique
  counties and represented states), return both COUNT(DISTINCT ...) values in
  the same scalar result.
- For result_scope=single, the final SELECT must return exactly one row. A
  GROUP BY is valid only for a dimension already fixed to one value by the
  question (for example, the named state shown beside its scalar). Never group
  by the repeated observation grain and return one aggregate per observation.
- Keep every WHERE filter the question requires (scope, year, casing) EXACTLY
  as needed — never drop a filter. Separately, ADD the geographic identifier to
  the SELECT list so results can be mapped: county -> also SELECT `state` and
  `county`; congressional -> also SELECT `cd_118`; state -> also SELECT `state`.
  This is an ADDITIONAL select column, never a replacement for the WHERE clause.
- Congressional tables encode state scope inside their district label. When a
  state is named and `cd_118` is the geography, constrain its postal prefix
  (for example `cd_118 LIKE 'MD-%'`) using the resolved district values in the
  grounding. Never return a national district list for a state-scoped request.
- Prefer correctness over cleverness. One statement only.
- Before returning, silently verify that the SQL's result shape, aggregation,
  metric, filters, and period each match the ANALYSIS CONTRACT and question.

Return ONLY JSON: {"sql": "<the query>", "explanation": "<one sentence>"}"""


def _ask_for_sql(messages: list[dict[str, str]]) -> str:
    raw = client.chat_json(
        messages,
        temperature=0.0,
        max_tokens=1400,
        purpose="stage4_sql",
    )
    if not isinstance(raw, dict):
        raise client.LLMError("SQL generator returned a non-object JSON response")
    return str(raw.get("sql") or "").strip()


def generate_and_execute(
    question: str,
    grounding_text: str,
    history: list[dict[str, Any]] | None = None,
    tables: list[str] | None = None,
    contract: AnalysisContract | None = None,
    resolved: dict[str, Any] | None = None,
) -> dict[str, Any]:
    max_rows = int(os.getenv("MAX_RETURN_ROWS", "250"))
    base_user = (
        f"GROUNDING\n========\n{grounding_text}\n\n"
        + (
            f"ANALYSIS CONTRACT (must satisfy exactly)\n{json.dumps(contract.model_dump(), default=str)}\n\n"
            if contract
            else ""
        )
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
        except client.LLMError as exc:
            return {
                "sql": sql,
                "rows": [],
                "error": f"LLM error: {exc}",
                "attempts": attempts,
                "truncated": False,
            }

        record: dict[str, Any] = {"sql": sql}
        try:
            # Normalization parses model SQL and can therefore fail for the
            # same reasons as validation. Keep it inside the repair loop so a
            # malformed quote/escape becomes feedback to the model instead of
            # escaping the orchestrator as a server error.
            if contract is not None:
                sql = normalize_generated_sql(sql, contract)
                record["sql"] = sql
            validate_sql(sql)
            if contract is not None:
                validate_semantic_sql(sql, question, contract, resolved)
            fetched_rows = execute_select(sql, max_rows=max_rows + 1)
            truncated = len(fetched_rows) > max_rows
            rows = fetched_rows[:max_rows]
            if contract is not None:
                validate_result_shape(rows, contract)
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

    return {
        "sql": sql,
        "rows": [],
        "error": attempts[-1].get("error") if attempts else "no sql",
        "attempts": attempts,
        "truncated": False,
    }
