"""Typed tools for multi-step analytical reasoning.

Each tool is a small deterministic function that wraps an existing primitive
(catalog inspector, value resolver, SQL validator+executor). The agent calls
them via OpenAI-style tool calling; the loop in `reasoning.py` dispatches
on `name` and feeds the JSON result back as a `role:tool` message.

Tools are intentionally narrow and well-named so the model can compose them
without inventing behaviour — every quantitative claim ultimately traces back
to a `run_sql` row (validator-gated, read-only) or a `peer_stats` aggregate.
"""

from __future__ import annotations

import os
from typing import Any, Callable

from app.duckdb.connection import execute_select
from app.semantic.registry import (
    get_dataset,
    load_registry,
    quote_identifier,
    table_schema_block,
)
from app.semantic.value_resolver import distinct_values as _resolver_distinct
from app.sql.validator import SqlValidationError, validate_sql

# --------------------------------------------------------------------------- #
# OpenAI-format tool schemas the agent sees                                   #
# --------------------------------------------------------------------------- #
TOOL_SCHEMAS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_schema",
            "description": (
                "Get the full column schema and CRITICAL warnings for a catalog "
                "table (state-name casing, year handling, special-character "
                "columns, flow-table quirks). Call this before run_sql when "
                "you're not 100% sure of the schema."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "Exact catalog table id (e.g. 'contract_county', 'gov_state').",
                    },
                },
                "required": ["table"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "distinct_values",
            "description": (
                "List distinct values of a column in a table, read live from "
                "DuckDB. Use this to verify a filter value (state casing, agency "
                "canonical name, county name) BEFORE writing SQL. Optional "
                "`pattern` does a case-insensitive substring filter."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "column": {"type": "string"},
                    "pattern": {
                        "type": "string",
                        "description": "Optional case-insensitive substring to narrow the list.",
                    },
                    "limit": {"type": "integer", "default": 50},
                },
                "required": ["table", "column"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_sql",
            "description": (
                "Execute one read-only DuckDB SELECT or WITH...SELECT against "
                "the mart_<table> views. Returns rows (capped). Validator-gated. "
                "Obey every CRITICAL warning from get_schema (state casing, year-as-string, etc.)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "Single SELECT/WITH statement. Query mart_<table> views only.",
                    },
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "peer_stats",
            "description": (
                "Distribution stats (min/p25/median/mean/p75/max + top-5 + "
                "bottom-5) for a numeric measure across all entities at a "
                "geography level — exactly what you need to position a value "
                "('is Maryland's debt ratio high?'). Internally runs 2 SQLs."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "measure": {"type": "string", "description": "Numeric column name."},
                    "geo_column": {
                        "type": "string",
                        "default": "state",
                        "description": "Label column (state / county / cd_118).",
                    },
                    "where": {
                        "type": "string",
                        "description": "Optional SQL WHERE clause without the WHERE keyword, e.g. \"year = '2024'\".",
                    },
                    "focus_value": {
                        "type": "string",
                        "description": (
                            "Optional named geography whose exact value and ascending/descending "
                            "ranks should be returned."
                        ),
                    },
                },
                "required": ["table", "measure"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "answer",
            "description": (
                "TERMINATE: produce the final answer to the user. Call this "
                "when (and only when) you have enough evidence. Every "
                "quantitative claim in `text` must be supported by data you "
                "received from earlier tool calls — no fabrication. Select the "
                "one evidence_id whose rows should be displayed as the primary "
                "Data/SQL result, and cite any other results used in "
                "supporting_evidence_ids."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "Concise markdown answer for the user.",
                    },
                    "key_numbers": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "label": {"type": "string"},
                                "value": {"type": ["string", "number"]},
                                "unit": {"type": "string"},
                            },
                            "required": ["label", "value"],
                        },
                    },
                    "caveats": {"type": "array", "items": {"type": "string"}},
                    "primary_evidence_id": {
                        "type": "string",
                        "description": (
                            "The evidence_id of the successful run_sql or peer_stats "
                            "result that most directly answers the question. Use an "
                            "empty string only when no row-producing evidence exists."
                        ),
                    },
                    "supporting_evidence_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Other evidence_ids whose results support claims in the "
                            "answer. Do not cite schema lookups, errors, or unused work."
                        ),
                    },
                },
                "required": ["text", "primary_evidence_id", "supporting_evidence_ids"],
            },
        },
    },
]


# --------------------------------------------------------------------------- #
# Tool executors                                                              #
# --------------------------------------------------------------------------- #
_VALID_TABLES = set(load_registry().datasets)


def tool_get_schema(table: str) -> dict[str, Any]:
    if table not in _VALID_TABLES:
        return {"error": f"unknown table {table!r}; valid: {sorted(_VALID_TABLES)[:8]}…"}
    return {"table": table, "schema": table_schema_block(table)}


def tool_distinct_values(
    table: str, column: str, pattern: str = "", limit: int = 50
) -> dict[str, Any]:
    if table not in _VALID_TABLES:
        return {"error": f"unknown table {table!r}"}
    ds = get_dataset(table)
    if not ds or column not in ds.columns:
        return {"error": f"column {column!r} not in {table}; columns: {ds.columns if ds else []}"}
    values = _resolver_distinct(table, column)
    if pattern:
        p = pattern.lower()
        values = tuple(v for v in values if p in v.lower())
    truncated = len(values) > limit
    return {
        "table": table,
        "column": column,
        "count": len(values),
        "values": list(values[:limit]),
        "truncated": truncated,
    }


def tool_run_sql(sql: str, semantic_guard: Callable[[str], None] | None = None) -> dict[str, Any]:
    max_rows = int(os.getenv("MAX_RETURN_ROWS", "250"))
    try:
        validate_sql(sql)
        if semantic_guard is not None:
            semantic_guard(sql)
    except SqlValidationError as exc:
        return {"sql": sql, "error": f"validation: {exc}", "rows": [], "row_count": 0}
    try:
        fetched = execute_select(sql, max_rows=max_rows + 1)
        truncated = len(fetched) > max_rows
        rows = fetched[:max_rows]
    except Exception as exc:  # duckdb error
        return {"sql": sql, "error": f"duckdb: {exc}", "rows": [], "row_count": 0}
    return {"sql": sql, "rows": rows, "row_count": len(rows), "truncated": truncated}


def tool_peer_stats(
    table: str,
    measure: str,
    geo_column: str = "state",
    where: str = "",
    focus_value: str = "",
    sort_direction: str = "desc",
    semantic_guard: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if table not in _VALID_TABLES:
        return {"error": f"unknown table {table!r}"}
    ds = get_dataset(table)
    if not ds:
        return {"error": f"unknown table {table!r}"}
    if measure not in ds.columns:
        return {"error": f"measure {measure!r} not in {table}; columns: {ds.columns}"}
    if geo_column not in ds.columns:
        return {"error": f"geo_column {geo_column!r} not in {table}"}
    m = quote_identifier(measure)
    g = quote_identifier(geo_column)
    where_sql = f"WHERE {where}" if where else ""
    stats_sql = (
        f"SELECT COUNT(*) AS n, MIN({m}) AS min, MAX({m}) AS max, "
        f"AVG({m}) AS mean, MEDIAN({m}) AS median, "
        f"QUANTILE_CONT({m}, 0.25) AS p25, QUANTILE_CONT({m}, 0.75) AS p75 "
        f"FROM mart_{table} {where_sql}"
    )
    bot_sql = (
        f"SELECT {g} AS label, {m} AS v FROM mart_{table} {where_sql} "
        f"ORDER BY {m} ASC NULLS LAST, {g} ASC LIMIT 5"
    )
    top_sql = (
        f"SELECT {g} AS label, {m} AS v FROM mart_{table} {where_sql} "
        f"ORDER BY {m} DESC NULLS LAST, {g} ASC LIMIT 5"
    )
    focus_sql = ""
    if focus_value:
        escaped_focus = focus_value.replace("'", "''")
        scoped_filter = f"({where}) AND {m} IS NOT NULL" if where else f"{m} IS NOT NULL"
        requested_rank = "rank_asc" if sort_direction == "asc" else "rank_desc"
        focus_sql = (
            "WITH ranked AS ("
            f"SELECT {g} AS label, {m} AS v, "
            f"RANK() OVER (ORDER BY {m} ASC NULLS LAST) AS rank_asc, "
            f"RANK() OVER (ORDER BY {m} DESC NULLS LAST) AS rank_desc, "
            f"COUNT({m}) OVER () AS total FROM mart_{table} WHERE {scoped_filter}"
            ") SELECT label, v, rank_asc, rank_desc, "
            f"{requested_rank} AS rank, total FROM ranked "
            f"WHERE LOWER(CAST(label AS VARCHAR)) = LOWER('{escaped_focus}')"
        )
    try:
        for q in (stats_sql, top_sql, bot_sql, focus_sql):
            if not q:
                continue
            validate_sql(q)
            if semantic_guard is not None:
                semantic_guard(q)
        stats = execute_select(stats_sql, max_rows=1)
        top5 = execute_select(top_sql, max_rows=5)
        bot5 = execute_select(bot_sql, max_rows=5)
        focus = execute_select(focus_sql, max_rows=1) if focus_sql else []
    except SqlValidationError as exc:
        return {"error": f"validation: {exc}"}
    except Exception as exc:
        return {"error": f"duckdb: {exc}"}
    return {
        "table": table,
        "measure": measure,
        "where": where,
        # The ranking statement and rows are the peer tool's primary visible
        # evidence. Distribution statistics remain alongside them in the tool
        # trail for synthesis and faithfulness checks.
        "sql": focus_sql if focus else top_sql,
        "rows": focus if focus else top5,
        "row_count": len(focus if focus else top5),
        "stats": stats[0] if stats else {},
        "top5": top5,
        "bottom5": bot5,
        "focus": focus[0] if focus else None,
        "rank_direction": sort_direction if focus else None,
    }


def execute_tool(
    name: str,
    args: dict[str, Any],
    *,
    semantic_guard: Callable[[str], None] | None = None,
    allowed_tables: set[str] | None = None,
) -> dict[str, Any]:
    """Dispatch a tool call by name; returns a JSON-serialisable dict.

    The `answer` tool is the agent's terminator — not executed here; the loop
    detects it and exits.
    """
    table_arg = str(args.get("table", ""))
    if allowed_tables is not None and name in {"get_schema", "distinct_values", "peer_stats"}:
        if table_arg not in allowed_tables:
            return {"error": f"table {table_arg!r} is outside the routed analysis contract"}
    if name == "get_schema":
        return tool_get_schema(str(args.get("table", "")))
    if name == "distinct_values":
        return tool_distinct_values(
            str(args.get("table", "")),
            str(args.get("column", "")),
            str(args.get("pattern", "") or ""),
            int(args.get("limit", 50) or 50),
        )
    if name == "run_sql":
        return tool_run_sql(str(args.get("sql", "")), semantic_guard)
    if name == "peer_stats":
        return tool_peer_stats(
            str(args.get("table", "")),
            str(args.get("measure", "")),
            str(args.get("geo_column", "state") or "state"),
            str(args.get("where", "") or ""),
            str(args.get("focus_value", "") or ""),
            str(args.get("sort_direction", "desc") or "desc"),
            semantic_guard,
        )
    return {"error": f"unknown tool {name!r}"}
