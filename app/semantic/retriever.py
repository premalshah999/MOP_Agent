from __future__ import annotations

import re
from collections import Counter

from app.schemas.semantic_context import RetrievedDataset, RetrievedMetric, SemanticContext
from app.semantic.matcher import score_metric, tokens
from app.semantic.models import DatasetDefinition, MetricDefinition
from app.semantic.registry import load_registry


_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _tokens(text: str) -> set[str]:
    return set(_TOKEN_RE.findall(text.lower()))


def _phrase_score(question: str, phrases: list[str]) -> float:
    q = question.lower()
    score = 0.0
    for phrase in phrases:
        normalized = phrase.lower()
        if normalized and normalized in q:
            score += 6.0 + min(len(normalized.split()), 4)
    return score


def _metric_text(metric: MetricDefinition) -> str:
    return " ".join(
        [
            metric.id,
            metric.label,
            metric.description,
            " ".join(metric.synonyms),
            " ".join(metric.default_for),
        ]
    )


def _dataset_text(dataset: DatasetDefinition) -> str:
    return " ".join(
        [
            dataset.id,
            dataset.display_name,
            dataset.description,
            dataset.geography,
            dataset.grain,
            " ".join(dataset.example_questions),
        ]
    )


def _geo_hint(question: str) -> str | None:
    q = question.lower()
    if "county" in q or "counties" in q:
        return "county"
    if "congress" in q or "district" in q:
        return "congress"
    if "state" in q or "states" in q:
        return "state"
    return None


def retrieve_semantic_context(question: str, *, max_datasets: int = 5, max_metrics: int = 8) -> SemanticContext:
    registry = load_registry()
    question_tokens = tokens(question, keep_question_words=True)
    geo = _geo_hint(question)
    dataset_scores: Counter[str] = Counter()
    metric_hits: list[RetrievedMetric] = []

    for dataset in registry.datasets.values():
        dataset_score = len(question_tokens & tokens(_dataset_text(dataset), keep_question_words=True))
        if geo and dataset.geography == geo:
            dataset_score += 4
        if any(token in question.lower() for token in ("fund", "spending", "money", "grant", "contract", "deal", "deals", "award", "procurement")):
            if dataset.id.startswith("contract_"):
                dataset_score += 8
            if dataset.id == "spending_state_agency" and any(token in question.lower() for token in ("agency", "agencies", "department", "defense", "defence", "dod", "deal", "deals")):
                dataset_score += 12
            if dataset.id.endswith("_flow") and any(token in question.lower() for token in ("flow", "subaward", "subcontract", "inflow", "outflow")):
                dataset_score += 8
        if any(token in question.lower() for token in ("poverty", "income", "population", "education")) and dataset.id.startswith("acs_"):
            dataset_score += 8
        if any(token in question.lower() for token in ("liabilities", "revenue", "debt", "cash flow", "asset", "assets", "expense", "expenses", "pension", "net position")) and dataset.id.startswith("gov_"):
            dataset_score += 8
        if any(token in question.lower() for token in ("financial literacy", "satisfaction", "risk", "constraint")) and dataset.id.startswith("finra_"):
            dataset_score += 8

        for metric in dataset.metrics.values():
            metric_tokens = tokens(_metric_text(metric), keep_question_words=True)
            score = float(len(question_tokens & metric_tokens))
            score += _phrase_score(question, metric.synonyms + metric.default_for + [metric.label, metric.id.replace("_", " ")])
            score += score_metric(question, metric)
            if score > 0:
                metric_hits.append(
                    RetrievedMetric(
                        dataset_id=dataset.id,
                        metric_id=metric.id,
                        label=metric.label,
                        description=metric.description,
                        score=score,
                    )
                )
                dataset_score += score

        if dataset_score > 0:
            dataset_scores[dataset.id] += dataset_score

    datasets = [
        RetrievedDataset(
            dataset_id=dataset_id,
            display_name=registry.datasets[dataset_id].display_name,
            description=registry.datasets[dataset_id].description,
            score=float(score),
        )
        for dataset_id, score in dataset_scores.most_common(max_datasets)
    ]
    metrics = sorted(metric_hits, key=lambda item: item.score, reverse=True)[:max_metrics]

    caveats: list[str] = []
    for item in datasets:
        for caveat in registry.datasets[item.dataset_id].caveats:
            if caveat not in caveats:
                caveats.append(caveat)

    return SemanticContext(datasets=datasets, metrics=metrics, caveats=caveats)
