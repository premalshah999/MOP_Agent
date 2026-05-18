"""Live filter-value resolution.

The LLM must filter on values that actually exist in the data with the exact
casing/spelling each table uses (state casing differs per table; agency names are
long canonical strings). This module reads DISTINCT values straight from DuckDB and
fuzzy-matches the user's phrasing to a real value, so the grounding pack can tell the
SQL writer the exact string to use.
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from functools import lru_cache

from app.duckdb.connection import execute_select
from app.semantic.registry import get_dataset, quote_identifier


# Columns worth resolving against real data (entity-like, not measures).
RESOLVABLE_COLUMNS = (
    "state", "county", "cd_118", "agency", "agency_name",
    "rcpt_state_name", "subawardee_state_name", "rcpt_cd_name", "subawardee_cd_name",
    "naics_2digit_title",
)

# Common shorthand the user might type for long canonical strings.
_ALIASES = {
    "dod": "defense",
    "dept of defense": "defense",
    "department of defense": "defense",
    "hhs": "health and human services",
    "hud": "housing and urban development",
    "dhs": "homeland security",
    "doj": "justice",
    "doe": "energy",
    "usda": "agriculture",
    "va": "veterans affairs",
    "dot": "transportation",
    "treasury": "treasury",
    "dc": "district of columbia",
}


_GENERIC_TOKENS = {"department", "of", "the", "and", "office", "us", "u", "s"}


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", str(text).lower())).strip()


def _content_tokens(text: str) -> set[str]:
    return {t for t in _norm(text).split() if t not in _GENERIC_TOKENS and len(t) > 2}


@lru_cache(maxsize=256)
def distinct_values(table_name: str, column: str, limit: int = 2000) -> tuple[str, ...]:
    """DISTINCT non-null values of `column` in `table_name`, read live from DuckDB."""
    dataset = get_dataset(table_name)
    if dataset is None or column not in dataset.columns:
        return ()
    col = quote_identifier(column)
    rows = execute_select(
        f"SELECT DISTINCT CAST({col} AS VARCHAR) AS v FROM {dataset.view_name} "
        f"WHERE {col} IS NOT NULL ORDER BY v",
        max_rows=limit,
    )
    return tuple(str(r["v"]) for r in rows if r.get("v") not in (None, ""))


def _similarity(query_norm: str, candidate: str) -> float:
    cand = _norm(candidate)
    if not query_norm or not cand:
        return 0.0
    if cand in query_norm:
        return 1.0
    cand_tokens = set(cand.split())
    q_tokens = set(query_norm.split())
    overlap = len(cand_tokens & q_tokens) / len(cand_tokens) if cand_tokens else 0.0
    best_window = 0.0
    qt = query_norm.split()
    size = len(cand.split())
    if size and len(qt) >= size:
        for i in range(0, len(qt) - size + 1):
            window = " ".join(qt[i : i + size])
            best_window = max(best_window, SequenceMatcher(None, window, cand).ratio())
    return max(SequenceMatcher(None, query_norm, cand).ratio(), best_window, overlap * 0.95)


def resolve_filter_value(
    table_name: str,
    column: str,
    question: str,
    *,
    min_score: float = 0.7,
) -> tuple[str, float] | None:
    """Best canonical value of `column` mentioned in `question`, or None.

    Returns (exact_value_as_stored, score). The value has the exact casing used by
    this table so it can be dropped straight into a WHERE clause.
    """
    values = distinct_values(table_name, column)
    if not values:
        return None
    q = _norm(question)
    expansions = [
        expansion
        for alias, expansion in _ALIASES.items()
        if re.search(rf"\b{re.escape(alias)}\b", q)
    ]
    q_tokens = set(q.split())
    best: tuple[str, float] | None = None
    for value in values:
        cand_norm = _norm(value)
        cand_tokens = set(cand_norm.split())
        score = _similarity(q, value)
        # An alias expansion whose words are all inside this candidate is a
        # strong signal (e.g. "defense" -> "Department of Defense").
        for expansion in expansions:
            exp_tokens = set(expansion.split())
            if exp_tokens and (exp_tokens <= cand_tokens or expansion in cand_norm):
                score = max(score, 0.97)
        # All of the candidate's distinctive words appear in the question
        # (e.g. "veterans affairs" -> "Department of Veterans Affairs").
        content = _content_tokens(value)
        if content and content <= q_tokens:
            score = max(score, 0.93)
        if best is None or score > best[1]:
            best = (value, score)
    if best and best[1] >= min_score:
        return best
    return None


def resolve_entities(table_name: str, question: str) -> dict[str, dict[str, object]]:
    """For every resolvable column present in the table, the best value the
    question refers to. Shape: {column: {"value": str, "score": float}}."""
    dataset = get_dataset(table_name)
    if dataset is None:
        return {}
    resolved: dict[str, dict[str, object]] = {}
    for column in RESOLVABLE_COLUMNS:
        if column not in dataset.columns:
            continue
        match = resolve_filter_value(table_name, column, question)
        if match:
            resolved[column] = {"value": match[0], "score": round(match[1], 3)}
    return resolved
