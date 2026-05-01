from __future__ import annotations

from difflib import SequenceMatcher
from functools import lru_cache
from typing import Any

from app.duckdb.connection import execute_select
from app.semantic.matcher import normalize_text
from app.semantic.registry import get_dataset, quote_identifier


def _score_value(question: str, value: str) -> float:
    q = normalize_text(question)
    candidate = normalize_text(value)
    if not q or not candidate:
        return 0.0
    if candidate in q:
        return 1.0
    candidate_tokens = set(candidate.split())
    question_tokens = set(q.split())
    if not candidate_tokens:
        return 0.0
    overlap = len(candidate_tokens & question_tokens) / len(candidate_tokens)
    ratio = SequenceMatcher(None, q, candidate).ratio()
    window_ratio = 0.0
    q_tokens = q.split()
    c_tokens = candidate.split()
    if len(q_tokens) >= len(c_tokens):
        size = len(c_tokens)
        for index in range(0, len(q_tokens) - size + 1):
            window = " ".join(q_tokens[index : index + size])
            window_ratio = max(window_ratio, SequenceMatcher(None, window, candidate).ratio())
    return max(ratio, window_ratio, overlap * 0.9)


@lru_cache(maxsize=128)
def dimension_values(dataset_id: str, dimension_id: str) -> tuple[str, ...]:
    dataset = get_dataset(dataset_id)
    if not dataset or dimension_id not in dataset.dimensions:
        return ()
    column = quote_identifier(dataset.dimensions[dimension_id].column)
    rows = execute_select(
        f"""
        SELECT DISTINCT CAST({column} AS VARCHAR) AS value
        FROM {dataset.view_name}
        WHERE {column} IS NOT NULL
        ORDER BY value
        """,
        max_rows=2000,
    )
    values = [str(row["value"]) for row in rows if row.get("value")]
    return tuple(values)


def resolve_dimension_value(
    dataset_id: str,
    dimension_id: str,
    question: str,
    *,
    min_score: float = 0.72,
) -> tuple[str, float] | None:
    best: tuple[str, float] | None = None
    for value in dimension_values(dataset_id, dimension_id):
        score = _score_value(question, value)
        if best is None or score > best[1]:
            best = (value, score)
    if best and best[1] >= min_score:
        return best
    return None
