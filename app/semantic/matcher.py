from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from functools import lru_cache
from typing import Iterable

from app.semantic.models import DatasetDefinition, MetricDefinition


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SPACE_RE = re.compile(r"\s+")
_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "by", "can", "for", "from", "give", "has", "have",
    "how", "i", "in", "is", "it", "me", "of", "on", "or", "show", "tell", "that", "the",
    "this", "to", "was", "were", "what", "which", "with",
}
_QUESTION_WORDS = {
    "top", "bottom", "highest", "lowest", "maximum", "minimum", "max", "min", "most", "least",
    "rank", "ranked", "ranking", "county", "counties", "state", "states", "district", "districts",
    "congress", "congressional", "agency", "agencies",
}
_EXPANSIONS = (
    (re.compile(r"\bp\s*/\s*c\b"), " per capita "),
    (re.compile(r"\bper\s*cap\b"), " per capita "),
    (re.compile(r"\bpercapita\b"), " per capita "),
    (re.compile(r"\bpc\b"), " per capita "),
    (re.compile(r"\bp\s+(?=(asset|assets|liabilit|revenue|expense|expenditure|cash|pension))"), " per capita "),
    (re.compile(r"\bfin\s+lit\b"), " financial literacy "),
    (re.compile(r"\bfinlit\b"), " financial literacy "),
    (re.compile(r"\bfcf\b"), " free cash flow "),
    (re.compile(r"\bhh\b"), " household "),
)


@dataclass(frozen=True)
class MetricMatch:
    metric_id: str
    score: float
    reason: str


def normalize_text(text: str) -> str:
    value = text.lower()
    value = value.replace("&", " and ")
    value = value.replace("+", " plus ")
    value = value.replace("%", " percent ")
    value = value.replace("$", " dollars ")
    value = value.replace("_", " ")
    value = value.replace("-", " ")
    value = value.replace("/", " / ")
    for pattern, replacement in _EXPANSIONS:
        value = pattern.sub(replacement, value)
    value = re.sub(r"[^a-z0-9/ ]+", " ", value)
    return _SPACE_RE.sub(" ", value).strip()


def _stem(token: str) -> str:
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith("ses") and len(token) > 4:
        return token[:-2]
    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]
    return token


def tokens(text: str, *, keep_question_words: bool = False) -> set[str]:
    ignored = _STOPWORDS if keep_question_words else _STOPWORDS | _QUESTION_WORDS
    return {
        _stem(token)
        for token in _TOKEN_RE.findall(normalize_text(text))
        if token not in ignored and len(token) > 1
    }


def metric_aliases(metric: MetricDefinition) -> list[str]:
    phrases = [
        metric.id,
        metric.id.replace("_", " "),
        metric.label,
        metric.description,
        *metric.synonyms,
        *metric.default_for,
    ]
    aliases: list[str] = []
    for phrase in phrases:
        normalized = normalize_text(phrase)
        if not normalized or normalized in aliases:
            continue
        aliases.append(normalized)
        phrase_tokens = normalized.split()
        if len(phrase_tokens) == 1:
            token = phrase_tokens[0]
            singular = _stem(token)
            if singular != token and singular not in aliases:
                aliases.append(singular)
            plural = f"{singular}s"
            if len(singular) > 2 and plural not in aliases:
                aliases.append(plural)
    return aliases


def _best_ngram_ratio(question_tokens: list[str], alias_tokens: list[str]) -> float:
    if not question_tokens or not alias_tokens:
        return 0.0
    size = len(alias_tokens)
    if len(question_tokens) < size:
        return SequenceMatcher(None, " ".join(question_tokens), " ".join(alias_tokens)).ratio()
    alias = " ".join(alias_tokens)
    best = 0.0
    for index in range(0, len(question_tokens) - size + 1):
        candidate = " ".join(question_tokens[index : index + size])
        best = max(best, SequenceMatcher(None, candidate, alias).ratio())
    return best


def _wants_per_capita(question: str) -> bool:
    q = normalize_text(question)
    return any(phrase in q for phrase in ("per capita", "per person", "per resident", "per household"))


def _wants_per_1000(question: str) -> bool:
    q = normalize_text(question)
    return any(phrase in q for phrase in ("per 1000", "per 1 000", "per thousand"))


def _wants_total(question: str) -> bool:
    q = normalize_text(question)
    return any(phrase in q for phrase in ("total", "overall", "all "))


def _wants_current(question: str) -> bool:
    return "current" in tokens(question, keep_question_words=True)


def score_metric(question: str, metric: MetricDefinition) -> float:
    q = normalize_text(question)
    q_tokens = tokens(q)
    q_all_tokens = [token for token in _TOKEN_RE.findall(q) if token not in _STOPWORDS]
    best = 0.0
    for alias in metric_aliases(metric):
        alias_tokens = tokens(alias)
        if not alias_tokens:
            continue
        if re.search(rf"\b{re.escape(alias)}\b", q):
            best = max(best, 80.0 + min(len(alias_tokens), 5) * 4)
            continue
        overlap = len(q_tokens & alias_tokens)
        if overlap:
            coverage = overlap / max(len(alias_tokens), 1)
            best = max(best, 12.0 + 40.0 * coverage + 4.0 * overlap)
        if len(alias_tokens) <= 4:
            ratio = _best_ngram_ratio(q_all_tokens, list(alias_tokens))
            if ratio >= 0.84:
                best = max(best, 24.0 + 35.0 * ratio)

    metric_id = metric.id
    unit = metric.unit.lower()
    is_per_capita = "per_capita" in metric_id or "per person" in unit
    is_per_1000 = "per_1000" in metric_id or "per 1,000" in unit
    wants_per_capita = _wants_per_capita(q)
    wants_per_1000 = _wants_per_1000(q)

    if wants_per_capita:
        best += 30 if is_per_capita else -18
    elif wants_per_1000:
        best += 30 if is_per_1000 else -18
    elif is_per_capita or is_per_1000:
        best -= 8

    if _wants_total(q):
        if metric_id.startswith("total_") or metric_id == "total_federal_funding":
            best += 10
        if metric_id.startswith("current_"):
            best -= 8
    if _wants_current(q):
        if metric_id.startswith("current_"):
            best += 18
        if metric_id.startswith("total_"):
            best -= 8
    if any(token in q_tokens for token in ("rate", "ratio", "share", "percent")) and unit in {"percent", "ratio"}:
        best += 8
    if "average" in q_tokens or "avg" in q_tokens:
        if metric.aggregation == "avg":
            best += 6
    return max(best, 0.0)


def best_metric_match(
    dataset: DatasetDefinition,
    question: str,
    *,
    context_metric_ids: Iterable[str] = (),
    min_score: float = 26.0,
) -> MetricMatch | None:
    context_ids = set(context_metric_ids)
    candidates: list[MetricMatch] = []
    for metric in dataset.metrics.values():
        score = score_metric(question, metric)
        if metric.id in context_ids:
            score += 10
        candidates.append(MetricMatch(metric_id=metric.id, score=score, reason="semantic metric match"))
    candidates.sort(key=lambda item: item.score, reverse=True)
    if not candidates or candidates[0].score < min_score:
        return None
    if len(candidates) > 1 and candidates[0].score == candidates[1].score:
        non_per = [item for item in candidates[:2] if "per_" not in item.metric_id]
        if non_per and not (_wants_per_capita(question) or _wants_per_1000(question)):
            return non_per[0]
    return candidates[0]


@lru_cache(maxsize=512)
def normalized_question_tokens(question: str) -> frozenset[str]:
    return frozenset(tokens(question, keep_question_words=True))
