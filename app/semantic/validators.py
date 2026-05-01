from __future__ import annotations

from app.schemas.query_plan import QueryPlan
from app.semantic.registry import get_dataset


class PlanValidationError(ValueError):
    pass


def validate_query_plan(plan: QueryPlan) -> None:
    if plan.intent in {"DEFINITION", "AMBIGUOUS", "UNANSWERABLE"}:
        return
    if not plan.queries:
        raise PlanValidationError("Plan requires SQL but has no query specs.")
    for query in plan.queries:
        dataset = get_dataset(query.dataset)
        if not dataset:
            raise PlanValidationError(f"Unknown dataset `{query.dataset}`.")
        if query.metric and query.metric not in dataset.metrics:
            raise PlanValidationError(f"Unknown metric `{query.metric}` for dataset `{query.dataset}`.")
        for dimension in query.dimensions:
            if dimension != dataset.label_column and dimension not in dataset.dimensions and dimension not in dataset.columns:
                raise PlanValidationError(f"Unknown dimension `{dimension}` for dataset `{query.dataset}`.")
        for filter_ in query.filters:
            if filter_.field == "year":
                if not dataset.year_column:
                    raise PlanValidationError(f"Dataset `{query.dataset}` does not support year filtering.")
                continue
            if filter_.field not in dataset.dimensions and filter_.field not in dataset.columns:
                raise PlanValidationError(f"Unknown filter field `{filter_.field}` for dataset `{query.dataset}`.")
