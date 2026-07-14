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


def _resolved_block(question: str, tables: list[str]) -> tuple[str, dict[str, Any]]:
    lines: list[str] = []
    resolved: dict[str, Any] = {}
    for table in tables:
        entities = resolve_entities(table, question)
        if not entities:
            continue
        resolved[table] = entities
        for column, info in entities.items():
            lines.append(
                f"  - {table}.{column} = {info['value']!r}  "
                f"(use this EXACT value/casing; match score {info['score']})"
            )
    if not lines:
        return "", resolved
    return (
        "RESOLVED FILTER VALUES (the question names these — use exactly):\n"
        + "\n".join(lines)
    ), resolved


def build_grounding(
    question: str,
    tables: list[str],
    *,
    year_strategy: str = "",
    join_plan: str = "",
) -> dict[str, Any]:
    """Return {text, tables, resolved} — `text` is injected into the SQL prompt."""
    schema_blocks = [table_schema_block(t) for t in tables]
    warnings = critical_warnings_for(tables)
    joins = join_hints_for(tables)
    resolved_text, resolved = _resolved_block(question, tables)
    patterns = common_question_patterns()

    default_years = []
    for t in tables:
        ds = get_dataset(t)
        if ds is not None:
            default_years.append(f"  - {t}: default year = {ds.default_year}")

    parts: list[str] = ["SCHEMA FOR THE ROUTED TABLE(S)", "=" * 32, *schema_blocks]
    if warnings:
        parts += ["", "CRITICAL WARNINGS (must obey):", *[f"  * {w}" for w in warnings]]
    if joins:
        parts += ["", "CROSS-TABLE JOIN GUIDANCE:", *[f"  * {j}" for j in joins if j]]
    if resolved_text:
        parts += ["", resolved_text]
    if default_years:
        parts += ["", "DEFAULT YEAR PER TABLE (use unless the question says otherwise):", *default_years]
    if year_strategy:
        parts += ["", f"ROUTER YEAR STRATEGY: {year_strategy}"]
    if join_plan:
        parts += ["", f"ROUTER JOIN PLAN: {join_plan}"]
    if any(t.startswith("acs_") for t in tables):
        parts += [
            "",
            "ACS DERIVED-METRIC RULE:",
            "  - Race / education / income / age / poverty / housing columns are "
            'PERCENTAGES of the relevant population (0–100), NOT counts.',
            '  - For a COUNT or "number of people" (e.g. "asian population by '
            'count"): compute "Total population" * "<pct column>" / 100.0.',
            "  - For a SHARE / percentage / rate: use the percentage column directly.",
            '  - "Total population" and "# of household" are already counts.',
        ]
    parts += [
        "",
        "GLOBAL SQL CONVENTIONS:",
        *[f"  - {k}: {v}" for k, v in patterns.items()],
        "  - Only SELECT/WITH. Query the mart_<table> views. Normalize state "
        "casing with LOWER() when filtering or joining across tables.",
    ]

    return {"text": "\n".join(parts), "tables": tables, "resolved": resolved}
