"""Peer / comparative context for single-entity answers.

When SQL returns one row for one state, the answer is almost always more useful
if it can say "MD ranks 8th of 51" or "above the national median of $X" or
"up 4% from 2022" — but the LLM only knows what's in the rows. This module
runs 3 cheap side-queries (rank, median, prior-year) and returns a small JSON
block the answer prompt can use as `PEER CONTEXT`.

State-level only for now (covers ACS/gov/FINRA state tables, where most demo
questions live). County-level peer context is a separate, harder problem
(in-state vs cross-state peers) and is deferred.
"""

from __future__ import annotations

from typing import Any

from app.duckdb.connection import execute_select
from app.semantic.registry import get_dataset, quote_identifier
from app.sql.validator import SqlValidationError, validate_sql


def _looks_like_state_table(table: str, dataset: Any) -> bool:
    if dataset is None:
        return False
    geo = (getattr(dataset, "geography", "") or "").lower()
    return geo == "state" and "state" in (dataset.columns or [])


def _pick_measure(
    routing_columns: list[str], row: dict[str, Any], dataset_columns: list[str]
) -> str | None:
    """Pick a column to use for peer queries. MUST be a real dataset column
    (SQL aliases are useless for the peer queries we issue against the raw
    table). Prefer the routing's primary metric, else any non-Year numeric
    column present in BOTH the row and the dataset schema."""
    real = {c for c in (dataset_columns or [])}
    row_numeric = {
        k for k, v in row.items()
        if isinstance(v, (int, float)) and not isinstance(v, bool) and k.lower() not in {"year", "state"}
    }
    # Routing's metric column is most authoritative if it's real and numeric.
    for c in routing_columns or []:
        if c.lower() in {"state", "year"} or c not in real:
            continue
        # Real column may not be a row key (SQL aliased it). That's fine —
        # peer queries query the underlying column directly.
        return c
    # Fallback: pick any real numeric column the row evaluates.
    for k in row_numeric:
        if k in real:
            return k
    return None


def _pick_year_col(dataset: Any) -> str | None:
    for cand in ("Year", "year", "fiscal_year"):
        if cand in (dataset.columns or []):
            return cand
    return None


def compute_peer_context(
    *,
    table: str,
    focus_state: str | None,
    year: Any,
    routing_columns: list[str],
    rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Return a small dict {measure, year, value, national_median, rank,
    total_states, prior_year, prior_value, yoy_change_pct} or None if not
    applicable. Failures (validation, DB, no measure) silently return None —
    peer context is never load-bearing on correctness."""
    # Peer context describes exactly one focus entity. Attaching it to a
    # two-state comparison makes the rank/median language ambiguous.
    if not focus_state or len(rows) != 1:
        return None
    dataset = get_dataset(table)
    if dataset is None or not _looks_like_state_table(table, dataset):
        return None
    row_state = rows[0].get("state") or rows[0].get("state_name")
    if isinstance(row_state, str) and row_state.casefold() != focus_state.casefold():
        return None
    measure = _pick_measure(routing_columns, rows[0], list(dataset.columns or []))
    if not measure:
        return None
    year_col = _pick_year_col(dataset)
    # Value: prefer the real-column key, else fall back to whichever numeric
    # value the row carries (SQL may have aliased the column).
    val = rows[0].get(measure)
    if not isinstance(val, (int, float)) or isinstance(val, bool):
        for k, v in rows[0].items():
            if k.lower() in {"state", "year"}:
                continue
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                val = v
                break
    if not isinstance(val, (int, float)) or isinstance(val, bool):
        return None

    m = quote_identifier(measure)
    where = "WHERE 1=1"
    yr_int: int | None = None
    if year_col and year not in (None, ""):
        y = quote_identifier(year_col)
        try:
            yr_int = int(str(year))
        except ValueError:
            yr_int = None
        if yr_int is not None:
            # Some tables (contract_state) store year as a STRING. Compare
            # as text first; DuckDB will coerce '2024' = '2024' correctly.
            # Falls back gracefully — if the column is actually numeric,
            # CAST will still match.
            where += f" AND CAST({y} AS VARCHAR) = '{yr_int}'"

    rank_sql = (
        f"SELECT (SELECT COUNT(*) FROM mart_{table} {where} AND {m} > "
        f"(SELECT {m} FROM mart_{table} {where} AND LOWER(state) = '{focus_state.lower()}' LIMIT 1) "
        f"AND {m} IS NOT NULL) + 1 AS rank, "
        f"(SELECT COUNT(*) FROM mart_{table} {where} AND {m} IS NOT NULL) AS n, "
        f"(SELECT MEDIAN({m}) FROM mart_{table} {where}) AS national_median"
    )
    try:
        validate_sql(rank_sql)
        result = execute_select(rank_sql, max_rows=1)
    except (SqlValidationError, Exception):
        return None
    if not result:
        return None
    row0 = result[0]
    rank_val = row0.get("rank")
    n_val = row0.get("n")
    # Sanity gate: if total_states > 60 something went wrong (multi-year join,
    # broken filter). Bail rather than emit a misleading "35 of 102" claim.
    if isinstance(n_val, (int, float)) and n_val > 60:
        return None
    out: dict[str, Any] = {
        "focus_state": focus_state,
        "measure": measure,
        "year": yr_int,
        "value": val,
        "rank": rank_val,
        "total_states": n_val,
        "national_median": row0.get("national_median"),
    }

    # Prior-year value for YoY (only when we have a clean year filter)
    if yr_int is not None and year_col:
        y = quote_identifier(year_col)
        prior_sql = (
            f"SELECT {m} AS prior_value FROM mart_{table} "
            f"WHERE LOWER(state) = '{focus_state.lower()}' "
            f"AND CAST({y} AS VARCHAR) = '{yr_int - 1}' LIMIT 1"
        )
        try:
            validate_sql(prior_sql)
            prior = execute_select(prior_sql, max_rows=1)
        except (SqlValidationError, Exception):
            prior = []
        if prior and isinstance(prior[0].get("prior_value"), (int, float)):
            pv = float(prior[0]["prior_value"])
            out["prior_year"] = yr_int - 1
            out["prior_value"] = pv
            if pv != 0:
                out["yoy_change_pct"] = round((float(val) - pv) / pv * 100, 1)
    return out


def render_peer_context(ctx: dict[str, Any]) -> str:
    """Compact text the answer prompt can consume verbatim."""
    if not ctx:
        return ""
    lines = []
    measure = ctx.get("measure", "")
    focus = ctx.get("focus_state") or "Focus state"
    if ctx.get("rank") and ctx.get("total_states"):
        lines.append(
            f"- {focus} highest-first rank for {measure}: "
            f"#{ctx['rank']} of {ctx['total_states']}"
        )
    nm = ctx.get("national_median")
    val = ctx.get("value")
    if isinstance(nm, (int, float)) and isinstance(val, (int, float)) and nm != 0:
        rel = (val - nm) / nm * 100
        direction = "above" if rel > 0 else "below"
        lines.append(
            f"- {focus} vs national median ({_format_peer_value(nm)}): "
            f"{abs(rel):.1f}% {direction}"
        )
    if ctx.get("yoy_change_pct") is not None and ctx.get("prior_year"):
        ch = ctx["yoy_change_pct"]
        arrow = "+" if ch >= 0 else ""
        lines.append(
            f"- {focus} vs {ctx['prior_year']}: {arrow}{ch}% "
            f"(was {_format_peer_value(ctx['prior_value'])})"
        )
    return "\n".join(lines)


def _format_peer_value(value: float | int) -> str:
    """Preserve index/ratio precision instead of rounding 0.7873 to 1."""
    number = float(value)
    if abs(number) >= 1000:
        return f"{number:,.0f}"
    if abs(number) >= 10:
        return f"{number:,.2f}".rstrip("0").rstrip(".")
    return f"{number:,.4f}".rstrip("0").rstrip(".")
