"""Resolve explicitly mentioned entities to canonical values in DuckDB.

This deliberately does *not* compare an entire question with every value.  A
full-question fuzzy score made generic language such as "state average" look
like a mention of the Department of State, and could even invent a geography.
Resolution now requires an exact phrase, a column-appropriate alias, or a very
close typo-sized token window.  Multiple named values are retained.
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from functools import lru_cache

from app.duckdb.connection import execute_select
from app.semantic.registry import get_dataset, quote_identifier


RESOLVABLE_COLUMNS = (
    "state", "county", "cd_118", "agency", "agency_name",
    "rcpt_state_name", "subawardee_state_name", "rcpt_cd_name", "subawardee_cd_name",
    "naics_2digit_title",
)

_AGENCY_ALIASES = {
    "dod": "defense", "dept of defense": "defense",
    "department of defense": "defense", "defense department": "defense",
    "department of defence": "defense", "defence department": "defense",
    "hhs": "health and human services",
    "hud": "housing and urban development", "dhs": "homeland security",
    "doj": "justice", "doe": "energy", "energy department": "energy",
    "usda": "agriculture",
    "va": "veterans affairs", "dot": "transportation",
    "treasury": "treasury", "state department": "state",
}
_STATE_ABBREVIATIONS = {
    "AL": "alabama", "AK": "alaska", "AZ": "arizona", "AR": "arkansas",
    "CA": "california", "CO": "colorado", "CT": "connecticut", "DE": "delaware",
    "FL": "florida", "GA": "georgia", "HI": "hawaii", "ID": "idaho",
    "IL": "illinois", "IN": "indiana", "IA": "iowa", "KS": "kansas",
    "KY": "kentucky", "LA": "louisiana", "ME": "maine", "MD": "maryland",
    "MA": "massachusetts", "MI": "michigan", "MN": "minnesota", "MS": "mississippi",
    "MO": "missouri", "MT": "montana", "NE": "nebraska", "NV": "nevada",
    "NH": "new hampshire", "NJ": "new jersey", "NM": "new mexico", "NY": "new york",
    "NC": "north carolina", "ND": "north dakota", "OH": "ohio", "OK": "oklahoma",
    "OR": "oregon", "PA": "pennsylvania", "RI": "rhode island", "SC": "south carolina",
    "SD": "south dakota", "TN": "tennessee", "TX": "texas", "UT": "utah",
    "VT": "vermont", "VA": "virginia", "WA": "washington", "WV": "west virginia",
    "WI": "wisconsin", "WY": "wyoming", "DC": "district of columbia",
}
_GENERIC = {
    "department", "of", "the", "and", "office", "agency", "state", "county",
    "district", "government", "federal", "us", "u", "s",
}


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", str(text).lower())).strip()


def _content_tokens(text: str) -> list[str]:
    return [t for t in _norm(text).split() if t not in _GENERIC and len(t) > 2]


@lru_cache(maxsize=2048)
def distinct_values(table_name: str, column: str, limit: int = 2000) -> tuple[str, ...]:
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


def _exact_phrase(question_norm: str, candidate_norm: str) -> bool:
    return bool(candidate_norm and re.search(rf"\b{re.escape(candidate_norm)}\b", question_norm))


def _window_score(question_norm: str, candidate_norm: str) -> float:
    """Best same-size n-gram score; never score the whole question."""
    q_tokens = question_norm.split()
    c_tokens = candidate_norm.split()
    if not c_tokens:
        return 0.0
    best = 0.0
    sizes = {len(c_tokens)}
    if len(c_tokens) > 1:
        sizes.add(len(c_tokens) - 1)
        sizes.add(len(c_tokens) + 1)
    for size in sizes:
        if size <= 0 or size > len(q_tokens):
            continue
        for i in range(len(q_tokens) - size + 1):
            window = " ".join(q_tokens[i:i + size])
            best = max(best, SequenceMatcher(None, window, candidate_norm).ratio())
    return best


def _alias_expansions(column: str, question: str) -> list[str]:
    expansions: list[str] = []
    q_norm = _norm(question)
    if column in {"agency", "agency_name"}:
        for alias, expansion in _AGENCY_ALIASES.items():
            if re.search(rf"\b{re.escape(alias)}\b", q_norm):
                expansions.append(expansion)
    if column in {"state", "rcpt_state_name", "subawardee_state_name"}:
        # Two-letter words such as IN/OR/ME are unsafe after lower-casing.
        # Postal aliases are accepted only when the user typed uppercase.
        for code, expansion in _STATE_ABBREVIATIONS.items():
            if re.search(rf"(?<![A-Za-z]){code}(?![A-Za-z])", question):
                expansions.append(expansion)
    return expansions


def resolve_filter_values(
    table_name: str,
    column: str,
    question: str,
    *,
    min_score: float = 0.88,
) -> list[tuple[str, float]]:
    """All canonical values explicitly named in ``question``, best first."""
    values = distinct_values(table_name, column)
    if not values:
        return []
    q_norm = _norm(question)
    expansions = _alias_expansions(column, question)
    exact_spans: dict[str, list[tuple[int, int]]] = {}
    for value in values:
        cand = _norm(value)
        if not cand:
            continue
        candidate_spans = [
            match.span() for match in re.finditer(rf"\b{re.escape(cand)}\b", q_norm)
        ]
        if candidate_spans:
            exact_spans[value] = candidate_spans
    strong_matches: list[tuple[str, float]] = []
    fuzzy_matches: list[tuple[str, float]] = []
    for value in values:
        cand = _norm(value)
        score = 0.0
        if _exact_phrase(q_norm, cand):
            score = 1.0
        for expansion in expansions:
            if expansion == cand or _exact_phrase(cand, expansion):
                score = max(score, 0.99)
        if score > 0.0:
            strong_matches.append((value, score))
            continue
        if score == 0.0:
            # Typo recovery is allowed only for a distinctive candidate.  A
            # candidate whose only content is "state"/"county" is not an entity.
            content = _content_tokens(value)
            if content:
                score = _window_score(q_norm, cand)
        if score >= min_score:
            fuzzy_matches.append((value, score))

    # Prefer the most specific exact phrase at an overlapping location.  A
    # request for "West Virginia" contains the shorter string "Virginia", but
    # it does not name both states unless "Virginia" also occurs separately.
    exact_items = list(exact_spans.items())
    filtered_strong: list[tuple[str, float]] = []
    for value, score in strong_matches:
        exact_value_spans = exact_spans.get(value)
        if exact_value_spans and all(
            any(
                other_value != value
                and other_start <= start
                and end <= other_end
                and (other_start, other_end) != (start, end)
                for other_value, other_spans in exact_items
                for other_start, other_end in other_spans
            )
            for start, end in exact_value_spans
        ):
            continue
        filtered_strong.append((value, score))
    strong_matches = filtered_strong

    # A typo-sized window around an exact shorter entity must not introduce a
    # broader sibling.  For example, "Virginia's poverty rate" previously
    # admitted "West Virginia" because the surrounding words happened to
    # reach the fuzzy threshold.  Preserve genuine typo recovery ("Marylnd")
    # while rejecting fuzzy candidates that merely contain an already exact
    # entity phrase.
    strong_norms = [_norm(value) for value, _ in strong_matches]
    for value, score in fuzzy_matches:
        cand = _norm(value)
        if any(
            strong != cand and re.search(rf"\b{re.escape(strong)}\b", cand)
            for strong in strong_norms
        ):
            continue
        strong_matches.append((value, score))

    matches = strong_matches
    matches.sort(key=lambda item: (item[1], len(item[0])), reverse=True)
    return matches


def resolve_filter_value(
    table_name: str,
    column: str,
    question: str,
    *,
    min_score: float = 0.88,
) -> tuple[str, float] | None:
    matches = resolve_filter_values(table_name, column, question, min_score=min_score)
    return matches[0] if matches else None


def resolve_entities(table_name: str, question: str) -> dict[str, dict[str, object]]:
    dataset = get_dataset(table_name)
    if dataset is None:
        return {}
    resolved: dict[str, dict[str, object]] = {}
    for column in RESOLVABLE_COLUMNS:
        if column not in dataset.columns:
            continue
        matches = resolve_filter_values(table_name, column, question)
        if matches:
            resolved[column] = {
                "value": matches[0][0],
                "values": [value for value, _ in matches],
                "score": round(matches[0][1], 3),
            }
    return resolved
