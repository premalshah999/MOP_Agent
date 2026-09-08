"""Stage 3 — Retrieval / grounding.

Deterministic (no LLM). Assembles everything the SQL writer needs to be correct
on the FIRST try: exact column schema for the routed tables, the critical
warnings that apply, cross-table join patterns, the global SQL conventions, and
— crucially — the exact stored values for any entity the question names
(state casing, canonical agency names) read live from DuckDB.
"""

from __future__ import annotations

from typing import Any

from app.semantic.registry import (
    common_question_patterns,
    critical_warnings_for,
    get_dataset,
    join_hints_for,
    table_schema_block,
)
from app.semantic.value_resolver import resolve_entities


def _resolved_block(
    question: str,
    tables: list[str],
    filter_columns: list[str] | None = None,
    output_dimensions: list[str] | None = None,
    flow_direction: str = "none",
) -> tuple[str, dict[str, Any]]:
    lines: list[str] = []
    resolved: dict[str, Any] = {}
    for table in tables:
        effective_filter_columns = filter_columns
        dataset = get_dataset(table)
        if dataset is not None and filter_columns is not None and output_dimensions is not None:
            # The language plan normally names every filter dimension. Recover
            # only an exact/high-confidence canonical entity that the plan left
            # in a non-output dimension. This is schema grounding, not intent
            # inference: generic dimension words resolve to nothing, fuzzy
            # candidates below 0.99 are ignored, and an output grouping is never
            # collapsed into a filter.
            implicit_dimensions = [
                column
                for column in dataset.dimensions
                if column not in output_dimensions and column not in filter_columns
            ]
            if table.endswith("_flow") and flow_direction in {"inflow", "outflow"}:
                correct_prefix = "subawardee_" if flow_direction == "inflow" else "rcpt_"
                opposite_prefix = "rcpt_" if flow_direction == "inflow" else "subawardee_"
                implicit_dimensions = [
                    column
                    for column in implicit_dimensions
                    if column.startswith(correct_prefix) or not column.startswith(opposite_prefix)
                ]
            implicit_candidates = resolve_entities(
                table,
                question,
                allowed_columns=implicit_dimensions,
            )
            implicit_columns = [
                column
                for column, info in implicit_candidates.items()
                if float(info.get("score") or 0.0) >= 0.99
            ]
            if implicit_columns:
                effective_filter_columns = [*filter_columns, *implicit_columns]
        if (
            dataset is not None
            and dataset.label_column == "cd_118"
            and filter_columns is not None
            and "cd_118" not in filter_columns
        ):
            # State is encoded as the postal prefix of cd_118 on congressional
            # tables. Always attempt that label resolution; when no state is
            # named it returns nothing and adds no constraint.
            effective_filter_columns = [*filter_columns, "cd_118"]
        entities = resolve_entities(
            table,
            question,
            allowed_columns=effective_filter_columns,
        )
        if not entities:
            continue
        resolved[table] = entities
        for column, info in entities.items():
            raw_values = info.get("values")
            values = list(raw_values) if isinstance(raw_values, (list, tuple)) else [info["value"]]
            lines.append(
                f"  - {table}.{column} = {values!r}  "
                f"(use this EXACT value/casing; match score {info['score']})"
            )
    if not lines:
        return "", resolved
    return (
        "RESOLVED FILTER VALUES (the question names these — use exactly):\n" + "\n".join(lines)
    ), resolved


def build_grounding(
    question: str,
    tables: list[str],
    *,
    year_strategy: str = "",
    join_plan: str = "",
    filter_columns: list[str] | None = None,
    output_dimensions: list[str] | None = None,
    flow_direction: str = "none",
) -> dict[str, Any]:
    """Return {text, tables, resolved} — `text` is injected into the SQL prompt."""
    schema_blocks = [table_schema_block(t) for t in tables]
    warnings = critical_warnings_for(tables)
    joins = join_hints_for(tables)
    resolved_text, resolved = _resolved_block(
        question,
        tables,
        filter_columns=filter_columns,
        output_dimensions=output_dimensions,
        flow_direction=flow_direction,
    )
    patterns = common_question_patterns()

    default_years = []
    period_rows = []
    for t in tables:
        ds = get_dataset(t)
        if ds is not None:
            default_years.append(f"  - {t}: default year = {ds.default_year}")
            periods = [str(value) for value in ds.available_years if "-" in str(value)]
            if periods and ds.year_column:
                period_rows.append(
                    f"  - {t}: {ds.year_column} stores precomputed multi-year summary "
                    f"row(s) {periods}. Filter to the period label when requested, but "
                    "do not call it a multi-year sum unless the catalog explicitly says "
                    "so; NEVER invent annual rows or add it to a single-year row."
                )

    parts: list[str] = ["SCHEMA FOR THE ROUTED TABLE(S)", "=" * 32, *schema_blocks]
    if warnings:
        parts += ["", "CRITICAL WARNINGS (must obey):", *[f"  * {w}" for w in warnings]]
    if joins:
        parts += ["", "CROSS-TABLE JOIN GUIDANCE:", *[f"  * {j}" for j in joins if j]]
    if resolved_text:
        parts += ["", resolved_text]
    if default_years:
        parts += [
            "",
            "DEFAULT YEAR PER TABLE (use unless the question says otherwise):",
            *default_years,
        ]
    if period_rows:
        parts += ["", "PRECOMPUTED MULTI-YEAR SUMMARY ROWS (critical):", *period_rows]

    # Scope conventions — the #1 source of run-to-run answer drift was the
    # model re-deciding time scope / direction / filters each run. These are
    # ANALYST CONVENTIONS, stated once, applied everywhere.
    parts += [
        "",
        "ANALYSIS CONVENTIONS (defaults — apply unless the user explicitly asks otherwise):",
        "  - TIME SCOPE: when the user names no year or period, filter to the LATEST",
        "    single year for each table (county/congress flow: act_dt_fis_yr = 2024;",
        "    state_flow: no year filter because it has no year column; contract/spending:",
        "    year = '2024'; ACS: Year = 2023; FINRA state:",
        "    Year = 2021). NEVER aggregate across multiple years unless the user",
        "    explicitly asks for a multi-year total or a trend — a silent multi-year",
        "    SUM changes the meaning of every number in the answer.",
        "  - The word 'state' in phrases like 'state average', 'state total', or",
        "    'state level' is a GEOGRAPHY term. It is NEVER the U.S. Department of",
        "    State. Only filter agency_name/agency when the user explicitly NAMES a",
        "    federal agency.",
        "  - 'State average per district/county' means: that state's own total",
        "    divided by (or AVG across) its own districts/counties, computed from",
        "    the same table.",
    ]
    if any(t.endswith("_flow") for t in tables):
        parts += [
            "  - FLOW DIRECTION: 'receives / inflow / incoming money' = the",
            "    SUBAWARDEE side — group or filter on subawardee_cd_name /",
            "    subawardee_state_name / subawardee_cty_name. 'sends / outflow /",
            "    money going out' = the prime-recipient side (rcpt_*). Never mix",
            "    the two sides in one aggregation.",
        ]
    if year_strategy:
        parts += ["", f"ROUTER YEAR STRATEGY: {year_strategy}"]
    if join_plan:
        parts += ["", f"ROUTER JOIN PLAN: {join_plan}"]
    if any(t.startswith("acs_") for t in tables):
        parts += [
            "",
            "ACS DENOMINATOR AND CROSS-TAB RULES:",
            "  - The loaded ACS tables are a CURATED SUBSET of the broader upstream "
            "documentation. Only fields present in the schema above are queryable; "
            "never substitute a nearby field for an unavailable age, sex, education, "
            "race, income, poverty, or housing category.",
            "  - Percentage columns are separate MARGINAL estimates. Never multiply "
            "two percentage columns or otherwise infer an intersection (for example, "
            "Hispanic AND bachelor's, women AND graduate degree, or age 85+ AND educated).",
            "  - Education percentages apply to adults age 25+. The tables do not "
            "contain the age-25+ denominator, so report those percentages directly; "
            "a number of people with that education level is unavailable.",
            "  - AVG or MEDIAN across acs_state rows is an unweighted summary of "
            "represented state-level geographies (50 states, District of Columbia, "
            "and Puerto Rico), not a population-weighted U.S. national statistic. "
            "The missing age-25+ denominator means a true national education rate "
            "cannot be derived from this runtime table.",
            "  - Total-population count derivation is allowed only for percentage "
            "columns explicitly defined as shares of total population: Age 18-65, "
            "White, Black, Asian, and Hispanic.",
            '  - Household-income percentages may use "# of household" as their '
            "denominator. Do not use Total population for household measures.",
            "  - Poverty and owner/renter percentages have no matching denominator "
            "column in this curated table; do not turn them into counts.",
            "  - For a share / percentage / rate, use the percentage column directly.",
            '  - "Total population" and "# of household" are already counts.',
        ]
    if any(t.startswith("finra_") for t in tables):
        parts += [
            "",
            "FINRA DENOMINATOR RULES:",
            "  - FINRA geographic measures are survey shares or indices. The loaded",
            "    tables do not contain respondent counts, population denominators, or",
            "    survey weights that support converting them to resident headcounts.",
            "  - Report the FINRA share/index directly. Never multiply it by ACS Total",
            "    population to estimate a count of satisfied, risk-averse, constrained,",
            "    literate, or alternative-financing residents.",
        ]
    parts += [
        "",
        "GLOBAL SQL CONVENTIONS:",
        *[f"  - {k}: {v}" for k, v in patterns.items()],
        "  - Only SELECT/WITH. Query the mart_<table> views. Normalize state "
        "casing with LOWER() when filtering or joining across tables.",
    ]

    return {"text": "\n".join(parts), "tables": tables, "resolved": resolved}
