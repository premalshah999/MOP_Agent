"""Reasoning-mode tool set.

Each tool is a small deterministic function that wraps an existing primitive
(catalog inspector, value resolver, SQL validator+executor). The agent calls
them via OpenAI-style tool calling; the loop in `reasoning_agent.py` dispatches
on `name` and feeds the JSON result back as a `role:tool` message.

Tools are intentionally narrow and well-named so the model can compose them
without inventing behaviour — every quantitative claim ultimately traces back
to a `run_sql` row (validator-gated, read-only) or a `peer_stats` aggregate.
"""

from __future__ import annotations

import os
from typing import Any

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
                    "table": {"type": "string", "description": "Exact catalog table id (e.g. 'contract_county', 'gov_state')."},
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
                    "pattern": {"type": "string", "description": "Optional case-insensitive substring to narrow the list."},
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
                    "sql": {"type": "string", "description": "Single SELECT/WITH statement. Query mart_<table> views only."},
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
                    "geo_column": {"type": "string", "default": "state", "description": "Label column (state / county / cd_118)."},
                    "where": {"type": "string", "description": "Optional SQL WHERE clause without the WHERE keyword, e.g. \"year = '2024'\"."},
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
                "received from earlier tool calls — no fabrication."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "text": {"type": "string", "description": "Concise markdown answer for the user."},
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
                },
                "required": ["text"],
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


def tool_distinct_values(table: str, column: str, pattern: str = "", limit: int = 50) -> dict[str, Any]:
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


def tool_run_sql(sql: str) -> dict[str, Any]:
    max_rows = int(os.getenv("MAX_RETURN_ROWS", "250"))
    try:
        validate_sql(sql)
    except SqlValidationError as exc:
        return {"sql": sql, "error": f"validation: {exc}", "rows": [], "row_count": 0}
    try:
        rows = execute_select(sql, max_rows=max_rows)
    except Exception as exc:  # duckdb error
        return {"sql": sql, "error": f"duckdb: {exc}", "rows": [], "row_count": 0}
    return {"sql": sql, "rows": rows, "row_count": len(rows)}


def tool_peer_stats(
    table: str,
    measure: str,
    geo_column: str = "state",
    where: str = "",
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
    top_sql = (
        f"SELECT {g} AS label, {m} AS v FROM mart_{table} {where_sql} "
        f"ORDER BY {m} DESC NULLS LAST LIMIT 5"
    )
    bot_sql = (
        f"SELECT {g} AS label, {m} AS v FROM mart_{table} {where_sql} "
        f"ORDER BY {m} ASC NULLS LAST LIMIT 5"
    )
    try:
        for q in (stats_sql, top_sql, bot_sql):
            validate_sql(q)
        stats = execute_select(stats_sql, max_rows=1)
        top5 = execute_select(top_sql, max_rows=5)
        bot5 = execute_select(bot_sql, max_rows=5)
    except SqlValidationError as exc:
        return {"error": f"validation: {exc}"}
    except Exception as exc:
        return {"error": f"duckdb: {exc}"}
    return {
        "table": table,
        "measure": measure,
        "where": where,
        "stats": stats[0] if stats else {},
        "top5": top5,
        "bottom5": bot5,
    }


def execute_tool(name: str, args: dict[str, Any]) -> dict[str, Any]:
    """Dispatch a tool call by name; returns a JSON-serialisable dict.

    The `answer` tool is the agent's terminator — not executed here; the loop
    detects it and exits.
    """
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
        return tool_run_sql(str(args.get("sql", "")))
    if name == "peer_stats":
        return tool_peer_stats(
            str(args.get("table", "")),
            str(args.get("measure", "")),
            str(args.get("geo_column", "state") or "state"),
            str(args.get("where", "") or ""),
        )
    return {"error": f"unknown tool {name!r}"}
