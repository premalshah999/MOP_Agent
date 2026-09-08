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
    "state",
    "county",
    "cd_118",
    "agency",
    "agency_name",
    "rcpt_state_name",
    "subawardee_state_name",
    "rcpt_cd_name",
    "subawardee_cd_name",
    "rcpt_state",
    "subawardee_state",
    "rcpt_cty_name",
    "subawardee_cty_name",
    "rcpt_full_name",
    "subawardee_full_name",
    "naics_2digit_title",
)

_AGENCY_ALIASES = {
    "dod": "defense",
    "defense": "defense",
    "defence": "defense",
    "dept of defense": "defense",
    "department of defense": "defense",
    "defense department": "defense",
    "department of defence": "defense",
    "defence department": "defense",
    "hhs": "health and human services",
    "hud": "housing and urban development",
    "dhs": "homeland security",
    "doj": "justice",
    "doe": "energy",
    "energy department": "energy",
    "usda": "agriculture",
    "va": "veterans affairs",
    "dot": "transportation",
    "treasury": "treasury",
    "state department": "state",
}
_STATE_ABBREVIATIONS = {
    "AL": "alabama",
    "AK": "alaska",
    "AZ": "arizona",
    "AR": "arkansas",
    "CA": "california",
    "CO": "colorado",
    "CT": "connecticut",
    "DE": "delaware",
    "FL": "florida",
    "GA": "georgia",
    "HI": "hawaii",
    "ID": "idaho",
    "IL": "illinois",
    "IN": "indiana",
    "IA": "iowa",
    "KS": "kansas",
    "KY": "kentucky",
    "LA": "louisiana",
    "ME": "maine",
    "MD": "maryland",
    "MA": "massachusetts",
    "MI": "michigan",
    "MN": "minnesota",
    "MS": "mississippi",
    "MO": "missouri",
    "MT": "montana",
    "NE": "nebraska",
    "NV": "nevada",
    "NH": "new hampshire",
    "NJ": "new jersey",
    "NM": "new mexico",
    "NY": "new york",
    "NC": "north carolina",
    "ND": "north dakota",
    "OH": "ohio",
    "OK": "oklahoma",
    "OR": "oregon",
    "PA": "pennsylvania",
    "RI": "rhode island",
    "SC": "south carolina",
    "SD": "south dakota",
    "TN": "tennessee",
    "TX": "texas",
    "UT": "utah",
    "VT": "vermont",
    "VA": "virginia",
    "WA": "washington",
    "WV": "west virginia",
    "WI": "wisconsin",
    "WY": "wyoming",
    "DC": "district of columbia",
}
_GENERIC = {
    "department",
    "of",
    "the",
    "and",
    "office",
    "agency",
    "state",
    "county",
    "district",
    "government",
    "federal",
    "us",
    "u",
    "s",
}
_COUNTY_MARKERS = {"county", "counties", "parish", "parishes", "borough", "boroughs"}


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
    # Compare content words only. Otherwise ordinary syntax such as "state"
    # becomes a near-perfect typo match for Tate County, which can add an
    # invented county filter to a state-scoped question.
    q_tokens = _content_tokens(question_norm)
    c_tokens = _content_tokens(candidate_norm)
    if not c_tokens:
        return 0.0
    candidate_content = " ".join(c_tokens)
    best = 0.0
    sizes = {len(c_tokens)}
    if len(c_tokens) > 1:
        sizes.add(len(c_tokens) - 1)
        sizes.add(len(c_tokens) + 1)
    for size in sizes:
        if size <= 0 or size > len(q_tokens):
            continue
        for i in range(len(q_tokens) - size + 1):
            window = " ".join(q_tokens[i : i + size])
            best = max(best, SequenceMatcher(None, window, candidate_content).ratio())
    return best


def _is_county_column(column: str) -> bool:
    normalized = column.casefold()
    return normalized == "county" or "cty" in normalized


def _county_fuzzy_score(question_norm: str, candidate_norm: str) -> float:
    """Score typo recovery only next to an explicit county-type marker.

    County tables contain values that resemble ordinary analytical language.
    In particular, ``owner`` is a very close fuzzy match for Towner County.
    Comparing every question token to every county therefore manufactures
    filters. Exact county names remain supported anywhere; fuzzy recovery is
    limited to phrases such as ``Prince Gorg County`` or ``county of Prnce
    George`` where the user clearly attempted to name a county.
    """
    q_tokens = question_norm.split()
    candidate = " ".join(_content_tokens(candidate_norm))
    candidate_size = len(candidate.split())
    if not candidate or candidate_size <= 0:
        return 0.0
    best = 0.0
    sizes = {candidate_size}
    if candidate_size > 1:
        sizes.update({candidate_size - 1, candidate_size + 1})
    for marker_index, token in enumerate(q_tokens):
        if token not in _COUNTY_MARKERS:
            continue
        for size in sizes:
            if size <= 0:
                continue
            if marker_index >= size:
                before = " ".join(q_tokens[marker_index - size : marker_index])
                best = max(best, SequenceMatcher(None, before, candidate).ratio())
            start = marker_index + 1
            if start < len(q_tokens) and q_tokens[start] == "of":
                start += 1
            if start + size <= len(q_tokens):
                after = " ".join(q_tokens[start : start + size])
                best = max(best, SequenceMatcher(None, after, candidate).ratio())
    return best


def _county_name_is_explicit(question_norm: str, candidate_norm: str) -> bool:
    base = re.sub(r"\s+(?:county|parish|borough)$", "", candidate_norm).strip()
    return bool(
        base
        and re.search(
            rf"(?:\b{re.escape(base)}\s+(?:county|parish|borough)\b|"
            rf"\b(?:county|parish|borough)\s+of\s+{re.escape(base)}\b)",
            question_norm,
        )
    )


def _metric_collision(table_name: str, candidate_norm: str) -> bool:
    """Whether a county value is also a measure name in the same dataset."""
    dataset = get_dataset(table_name)
    if dataset is None:
        return False

    def singularized(value: str) -> str:
        words = []
        for word in _norm(value).split():
            if len(word) > 4 and word.endswith("s") and not word.endswith("ss"):
                word = word[:-1]
            words.append(word)
        return " ".join(words)

    candidate = singularized(candidate_norm)
    for metric in dataset.metrics.values():
        names = (metric.id, metric.label, *metric.synonyms)
        if candidate in {singularized(name) for name in names}:
            return True
    return False


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
    if column in {"cd_118", "rcpt_cd_name", "subawardee_cd_name"}:
        # Congressional tables encode state scope inside the district label
        # (MD-05, Maryland CD-05) rather than in a separate state column.
        # Resolve a plainly named state to every matching district so the SQL
        # layer receives an explicit, enforceable geographic constraint.
        for code, state_name in _STATE_ABBREVIATIONS.items():
            names_state = _exact_phrase(q_norm, state_name)
            names_code = bool(re.search(rf"(?<![A-Za-z]){code}(?![A-Za-z])", question))
            # An exact district identifier is an entity, not a request for all
            # districts in that state. Without this exclusion, the state-code
            # prefix in MD-08 expanded the filter to MD-01 through MD-08.
            names_exact_district = bool(
                re.search(rf"(?<![A-Za-z0-9]){code}\s*[- ]\s*0?\d{{1,2}}(?!\d)", question, re.I)
                or re.search(
                    rf"\b{re.escape(state_name)}\s+(?:cd|district)\s*[- ]?\s*0?\d{{1,2}}\b",
                    question,
                    re.I,
                )
            )
            district_match = re.search(
                rf"(?<![A-Za-z0-9]){code}\s*[- ]\s*(0?\d{{1,2}})(?!\d)",
                question,
                re.I,
            ) or re.search(
                rf"\b{re.escape(state_name)}\s+(?:cd|district)\s*[- ]?\s*(0?\d{{1,2}})\b",
                question,
                re.I,
            )
            if district_match:
                number = int(district_match.group(1))
                expansions.extend(
                    (f"{state_name} cd {number:02d}", f"{code.casefold()} {number:02d}")
                )
            if (names_state or names_code) and not names_exact_district:
                expansions.extend((state_name, code.casefold()))
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
        candidate_spans = [match.span() for match in re.finditer(rf"\b{re.escape(cand)}\b", q_norm)]
        if candidate_spans:
            exact_spans[value] = candidate_spans
    strong_matches: list[tuple[str, float]] = []
    fuzzy_matches: list[tuple[str, float]] = []
    for value in values:
        cand = _norm(value)
        score = 0.0
        if _exact_phrase(q_norm, cand):
            # A real county can share a name with an analytical measure. Do
            # not turn "grant funding by county" into Grant County; require
            # explicit county syntax for the colliding place name.
            if not (
                _is_county_column(column)
                and _metric_collision(table_name, cand)
                and not _county_name_is_explicit(q_norm, cand)
            ):
                score = 1.0
        for expansion in expansions:
            # Postal state codes expand to one exact state. Treating the
            # expansion as a substring made VA select both Virginia and West
            # Virginia. Agency aliases may intentionally match a longer
            # canonical department name (for example "defense"), and
            # congressional identifiers encode the state as a prefix (MD-01).
            alias_matches = expansion == cand or (
                column
                in {
                    "agency",
                    "agency_name",
                    "cd_118",
                    "rcpt_cd_name",
                    "subawardee_cd_name",
                }
                and _exact_phrase(cand, expansion)
            )
            if alias_matches:
                score = max(score, 0.99)
        if score > 0.0:
            strong_matches.append((value, score))
            continue
        if score == 0.0:
            # Typo recovery is allowed only for a distinctive candidate.  A
            # candidate whose only content is "state"/"county" is not an entity.
            content = _content_tokens(value)
            if content:
                score = (
                    _county_fuzzy_score(q_norm, cand)
                    if _is_county_column(column)
                    else _window_score(q_norm, cand)
                )
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


def resolve_entities(
    table_name: str,
    question: str,
    *,
    allowed_columns: list[str] | None = None,
) -> dict[str, dict[str, object]]:
    """Resolve only entity dimensions selected by the language planner.

    ``None`` preserves the legacy/catalog-inspection behavior of considering
    every resolvable dimension. An explicit list, including an empty one, is
    authoritative. This prevents a geography word that describes the result
    grain (for example, "New York county") from also becoming an invented
    county filter when the plan says that New York is the state filter.
    """
    dataset = get_dataset(table_name)
    if dataset is None:
        return {}
    resolved: dict[str, dict[str, object]] = {}
    allowed = (
        {str(column).casefold() for column in allowed_columns}
        if allowed_columns is not None
        else None
    )
    candidate_columns = [
        column for column in RESOLVABLE_COLUMNS if allowed is None or column.casefold() in allowed
    ]

    # Resolve state-like columns first, then use that entity type as evidence
    # when a county happens to share the same name. "Wyoming counties" names
    # the state of Wyoming, not a county called Wyoming in another state;
    # "Wyoming County" remains an explicit county mention.
    state_columns = [
        column
        for column in candidate_columns
        if column in dataset.columns and "state" in column.casefold()
    ]
    other_columns = [
        column
        for column in candidate_columns
        if column in dataset.columns and column not in state_columns
    ]
    state_values: set[str] = set()
    for column in state_columns:
        matches = resolve_filter_values(table_name, column, question)
        if len(matches) > 1:
            q_norm = _norm(question)
            matches = [
                (value, score)
                for value, score in matches
                if not re.search(
                    rf"\b{re.escape(_norm(value))}\s+(?:county|parish|borough)\b",
                    q_norm,
                )
            ]
        if not matches:
            continue
        resolved[column] = {
            "value": matches[0][0],
            "values": [value for value, _ in matches],
            "score": round(matches[0][1], 3),
        }
        state_values.update(_norm(value) for value, _ in matches)

    for column in other_columns:
        if column not in dataset.columns:
            continue
        matches = resolve_filter_values(table_name, column, question)
        if "cty" in column.casefold() or column.casefold() == "county":
            filtered: list[tuple[str, float]] = []
            q_norm = _norm(question)
            for value, score in matches:
                value_norm = _norm(value)
                value_base = re.sub(r"\s+(?:county|parish|borough)$", "", value_norm)
                duplicates_state = value_base in state_values
                explicit_county = bool(
                    re.search(
                        rf"(?:\b{re.escape(value_base)}\s+(?:county|parish|borough)\b|"
                        rf"\b(?:county|parish|borough)\s+of\s+{re.escape(value_base)}\b)",
                        q_norm,
                    )
                )
                if duplicates_state and not explicit_county:
                    continue
                filtered.append((value, score))
            matches = filtered
        if matches:
            resolved[column] = {
                "value": matches[0][0],
                "values": [value for value, _ in matches],
                "score": round(matches[0][1], 3),
            }
    return resolved
