"""Verified Query Repository — lightweight retrieval over hand-blessed
(question, SQL) pairs. When the user's question matches a verified entry
above threshold, we use that entry's SQL directly and skip LLM SQL
generation — provably correct retrieval, no hallucination surface.

Why not embeddings? At ~30-50 entries the dependency cost (sentence-transformers
or fastembed at hundreds of MB) outweighs the benefit. A normalized
Jaccard + difflib ratio gets us to acceptable precision for paraphrase
matching at this scale; we can swap to embeddings later if recall suffers.

Production pattern reference: Snowflake Cortex Analyst's Verified Query
Repository (https://docs.snowflake.com/en/user-guide/snowflake-cortex/
cortex-analyst/verified-query-repository).
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from app.paths import DATA_DIR


VQR_PATH = DATA_DIR / "verified_queries.yaml"

# Match threshold: max(Jaccard, difflib ratio) >= this → use blessed SQL.
# Empirically 0.65 was TOO LOW — "top 5 states by gdp per capita" falsely
# matched the poverty-rate entry at 0.67 just because of shared boilerplate
# tokens ("top 5 states by"). 0.78 is conservative: catches true
# paraphrases but rejects shape-collision false positives. Use the
# `metric_token` requirement below for additional safety.
_DEFAULT_THRESHOLD = 0.78

# Domain-significant tokens that MUST overlap between the user question and
# the verified entry for a match to be considered valid. This prevents
# matching on boilerplate alone — even a 0.80 lexical score doesn't help if
# the user's metric word (income / poverty / debt / grants / literacy) isn't
# in the candidate.
_METRIC_KEYWORDS = {
    "income", "poverty", "debt", "ratio", "grants", "contracts",
    "literacy", "wealth", "population", "asian", "black", "hispanic",
    "white", "education", "bachelors", "college", "degree", "employment",
    "unemployment", "wage", "wages", "household", "households", "median",
    "average", "subaward", "subcontract", "subawards", "flow", "flows",
    "cash", "assets", "liabilities", "pension", "revenue", "expenses",
    "spending", "funding", "constraint", "stress", "satisfaction",
    "risk", "alternative", "financing",
    # Government-finance discriminators (added when expanding VQR with
    # per-capita ranking shapes from production log): guarantees that
    # "net position per capita" cannot collide with "net OPEB per capita"
    # etc. even when the boilerplate ranking tokens overlap heavily.
    "position", "opeb", "defense",
    # Metrics we do NOT have data for — still worth listing so the guard
    # blocks these questions from matching an unrelated per-capita entry
    # ("top 5 states by gdp per capita" was matching vq027 expenses).
    "gdp", "crime",
}

# Salient tokens that flip the MEANING of a question even when the lexical
# similarity is high. "bottom 10 states by debt ratio" is 0.91-similar to
# "top 10 states by debt ratio" but the correct answer is the exact opposite
# rows. A match whose direction / numbers / geography signature differs from
# the user's question must never be executed verbatim — it is demoted to an
# LLM exemplar (see `match()` `_mode`).
_DIRECTION_TOKENS = {
    "highest", "lowest", "top", "bottom", "most", "least", "best", "worst",
    "largest", "smallest", "biggest", "max", "maximum", "min", "minimum",
    "richest", "poorest", "strongest", "weakest", "inflow", "outflow",
    "incoming", "outgoing", "into", "out", "ascending", "descending",
    "above", "below", "over", "under",
}

# Geography guard: if the user's question explicitly names a place and the
# candidate names a DIFFERENT place, the match is rejected no matter how
# high the lexical score is. Caught in adversarial testing: "top counties in
# guam by grants" matched the Maryland-counties entry at 0.80 and confidently
# served Maryland rows. One-way strict: a question that names no geography
# may still match a geo-specific entry (this app's domain default is Maryland).
_GEO_TOKENS = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "ohio", "oklahoma", "oregon",
    "pennsylvania", "tennessee", "texas", "utah", "vermont", "virginia",
    "washington", "wisconsin", "wyoming", "guam", "samoa",
    # multi-word states appear as their distinctive token after tokenization
    "hampshire", "jersey", "mexico", "york", "carolina", "dakota", "rhode",
    "columbia", "rico", "mariana",
}

_STOPWORDS = {
    "the", "a", "an", "of", "in", "on", "to", "for", "by", "with", "at", "is",
    "are", "was", "were", "be", "been", "what", "which", "how", "who", "where",
    "when", "do", "does", "did", "and", "or", "vs", "versus", "between", "than",
    "as", "from", "into", "that", "this", "it", "its", "their", "our", "my",
    "your", "have", "has", "had", "i", "we", "you", "they", "me", "us",
}


def _normalize(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^\w\s]", " ", s)  # strip punctuation
    s = re.sub(r"\s+", " ", s)
    return s


def _tokens(s: str) -> frozenset[str]:
    return frozenset(t for t in _normalize(s).split() if t and t not in _STOPWORDS and len(t) > 1)


def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, _normalize(a), _normalize(b)).ratio()


@lru_cache(maxsize=1)
def _load_repo() -> list[dict[str, Any]]:
    """Read + cache the verified-queries YAML. Only entries with
    `verified: true` are considered for matching."""
    if not VQR_PATH.exists():
        return []
    try:
        with VQR_PATH.open() as f:
            data = yaml.safe_load(f) or []
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        if not entry.get("verified"):
            continue
        if not entry.get("question") or not entry.get("sql"):
            continue
        out.append(entry)
    return out


def reload_repo() -> int:
    """Drop the lru cache. Useful for tests / hot reload. Returns new count."""
    _load_repo.cache_clear()
    return len(_load_repo())


def _salient_signature(text: str) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    """(direction words, numbers, geographies) — computed on the raw
    normalized word set, NOT `_tokens`, because several direction words
    ("into", "out") are stopwords there."""
    words = frozenset(_normalize(text).split())
    nums = frozenset(w for w in words if w.isdigit())
    return (words & _DIRECTION_TOKENS, nums, words & _GEO_TOKENS)


def match(question: str, threshold: float | None = None) -> dict[str, Any] | None:
    """Return the best-matching verified entry above threshold, else None.

    The returned dict gets two extra keys (the entry itself is not mutated):
      `_score` — float in [0,1] for observability/UI.
      `_mode`  — "exact": the question means the same thing as the blessed
                 one (same direction, numbers, geography) and the blessed SQL
                 may be executed verbatim.
                 "exemplar": lexically similar but salient tokens differ
                 (bottom vs top, 2019 vs 2023, outflow vs inflow, …) — the
                 blessed SQL is a REFERENCE for the LLM SQL writer, never a
                 substitute for it. Executing it verbatim would answer a
                 different question than the user asked."""
    entries = _load_repo()
    if not entries:
        return None
    thr = threshold if threshold is not None else _DEFAULT_THRESHOLD
    q_tokens = _tokens(question)
    q_metric_tokens = q_tokens & _METRIC_KEYWORDS
    q_geo_tokens = q_tokens & _GEO_TOKENS
    best: tuple[float, dict[str, Any]] | None = None
    for entry in entries:
        candidates = [entry["question"], *(entry.get("paraphrases") or [])]
        for c in candidates:
            c_tokens = _tokens(c)
            # Domain-keyword overlap guard: at least one metric word must be
            # shared. This is what prevents "top 5 states by gdp" from
            # matching "top 5 states by poverty rate" — different metric.
            c_metric_tokens = c_tokens & _METRIC_KEYWORDS
            if q_metric_tokens and c_metric_tokens and not (q_metric_tokens & c_metric_tokens):
                continue
            # Geography guard: the question names a place the candidate
            # doesn't cover → wrong-geography SQL, reject outright.
            c_geo_tokens = c_tokens & _GEO_TOKENS
            if q_geo_tokens and c_geo_tokens and not (q_geo_tokens & c_geo_tokens):
                continue
            if q_geo_tokens and not c_geo_tokens and any(
                _tokens(x) & _GEO_TOKENS for x in [entry["question"]]
            ):
                # candidate paraphrase omitted the state but the entry itself
                # is geo-specific — compare against the entry's geography
                if not (q_geo_tokens & (_tokens(entry["question"]) & _GEO_TOKENS)):
                    continue
            j = _jaccard(q_tokens, c_tokens)
            r = _ratio(question, c)
            score = max(j, r)
            if score >= thr and (best is None or score > best[0]):
                best = (score, entry, c)
                break  # don't double-score the same entry from its paraphrases
    if best is None:
        return None
    score, entry, matched_text = best
    # Exact only when every salient signal agrees with BOTH the matched
    # candidate and the canonical question (the canonical question is what
    # the SQL actually answers — a paraphrase may omit a year or LIMIT that
    # the SQL hardcodes).
    q_sig = _salient_signature(question)
    mode = (
        "exact"
        if q_sig == _salient_signature(matched_text)
        and q_sig[0] == _salient_signature(entry["question"])[0]
        else "exemplar"
    )
    return {**entry, "_score": score, "_mode": mode}
