from __future__ import annotations

import re
from dataclasses import dataclass

from app.semantic.matcher import normalize_text
from app.semantic.models import DatasetDefinition


@dataclass(frozen=True)
class MetricVariantSelection:
    metric_id: str
    reason: str


_COUNT_REQUEST_TERMS = (
    "amount",
    "count",
    "number",
    "absolute",
    "raw number",
    "people",
    "residents",
    "population count",
    "population amount",
    "based on amount",
    "based on count",
    "based on population count",
    "amount based",
    "count based",
    "not ratio",
    "not percentage",
    "not percent",
)
_SHARE_REQUEST_TERMS = (
    "ratio",
    "share",
    "percentage",
    "percent",
    "proportion",
    "based on ratio",
    "based on percentage",
    "based on percent",
    "ratio based",
    "percentage based",
    "percent based",
)
_PER_CAPITA_REQUEST_TERMS = (
    "per capita",
    "per person",
    "per resident",
    "per 1 000",
    "per 1000",
    "per thousand",
    "p/c",
    "pc basis",
)
_VARIANT_FOLLOW_UP_PREFIXES = (
    "based on",
    "base it on",
    "use",
    "switch to",
    "change to",
    "show",
    "make it",
    "same thing",
    "instead",
)
_AMOUNT_VARIANTS = ("count", "amount", "absolute")
_SHARE_VARIANTS = ("share", "percent", "ratio")
_PER_CAPITA_VARIANTS = ("per_capita", "per_1000")


def _contains_phrase(question: str, phrase: str) -> bool:
    return bool(re.search(rf"\b{re.escape(phrase)}\b", normalize_text(question)))


def asks_for_count_or_amount(question: str) -> bool:
    normalized = normalize_text(question)
    return any(_contains_phrase(normalized, term) for term in _COUNT_REQUEST_TERMS)


def asks_for_share_or_ratio(question: str) -> bool:
    normalized = normalize_text(question)
    if any(term in normalized for term in ("not ratio", "not percentage", "not percent")):
        return False
    return any(_contains_phrase(normalized, term) for term in _SHARE_REQUEST_TERMS)


def asks_for_per_capita(question: str) -> bool:
    normalized = normalize_text(question)
    return any(_contains_phrase(normalized, term) for term in _PER_CAPITA_REQUEST_TERMS)


def has_metric_variant_request(question: str) -> bool:
    return asks_for_count_or_amount(question) or asks_for_share_or_ratio(question) or asks_for_per_capita(question)


def looks_like_metric_variant_follow_up(question: str) -> bool:
    normalized = normalize_text(question)
    if not has_metric_variant_request(normalized):
        return False
    if any(normalized.startswith(prefix) for prefix in _VARIANT_FOLLOW_UP_PREFIXES):
        return True
    return len(normalized.split()) <= 5


def select_metric_variant(dataset: DatasetDefinition | None, metric_id: str | None, question: str) -> MetricVariantSelection | None:
    if not dataset or not metric_id:
        return None
    metric = dataset.metrics.get(metric_id)
    if not metric:
        return None

    requested_variants: list[tuple[str, tuple[str, ...]]] = []
    if asks_for_count_or_amount(question):
        requested_variants.append(("amount/count", _AMOUNT_VARIANTS))
    if asks_for_per_capita(question):
        requested_variants.append(("per-capita/per-1,000", _PER_CAPITA_VARIANTS))
    if asks_for_share_or_ratio(question):
        requested_variants.append(("ratio/share/percentage", _SHARE_VARIANTS))

    for reason, variants in requested_variants:
        for variant in variants:
            candidate_id = metric.related_variants.get(variant)
            if candidate_id and candidate_id != metric_id and candidate_id in dataset.metrics:
                return MetricVariantSelection(metric_id=candidate_id, reason=reason)
    return None
