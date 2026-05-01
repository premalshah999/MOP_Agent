from __future__ import annotations

from typing import Any

from app.schemas.query_plan import QueryPlan
from app.semantic.registry import get_dataset


def _pretty_label(value: Any) -> str:
    text = str(value)
    if text.islower() or text.isupper():
        return text.title()
    return text


def _map_dataset(dataset_id: str) -> str | None:
    if dataset_id.startswith("acs_"):
        return "census"
    if dataset_id.startswith("gov_"):
        return "gov_spending"
    if dataset_id.startswith("finra_"):
        return "finra"
    if dataset_id.startswith("contract_"):
        return "contract_static"
    if dataset_id == "spending_state_agency":
        return "contract_agency"
    if dataset_id.endswith("_flow"):
        return "fund_flow"
    return None


def _chart_type(operation: str) -> str:
    if operation == "trend":
        return "line"
    return "bar"


def build_chart(plan: QueryPlan, executions: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not executions or not plan.queries:
        return None
    rows = executions[0].get("rows") or []
    if len(rows) < 2:
        return None
    query = plan.queries[0]
    dataset = get_dataset(query.dataset)
    metric = dataset.metrics.get(query.metric) if dataset and query.metric else None
    chart_type = _chart_type(query.operation)
    display_rows = [{**row, "label": _pretty_label(row.get("label", ""))} for row in rows]
    encoding = {
        "tooltip": [
            {"field": "label", "type": "nominal", "title": "Label"},
            {"field": "metric_value", "type": "quantitative", "title": metric.label if metric else "Value", "format": ",.2f"},
        ]
    }
    if chart_type == "line":
        encoding.update(
            {
                "x": {"field": "label", "type": "ordinal", "title": "Period"},
                "y": {"field": "metric_value", "type": "quantitative", "title": metric.label if metric else "Value", "axis": {"grid": True}},
            }
        )
    else:
        encoding.update(
            {
                "y": {"field": "label", "type": "nominal", "sort": "-x", "title": None, "axis": {"labelLimit": 220}},
                "x": {"field": "metric_value", "type": "quantitative", "title": metric.label if metric else "Value", "axis": {"grid": True, "format": "~s" if metric and metric.unit in {"count", "dollars"} else None}},
                "color": {"value": "#3458a5"},
            }
        )
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
        "width": "container",
        "height": max(180, min(420, len(rows) * 28)),
        "data": {"values": display_rows},
        "mark": {"type": chart_type, "tooltip": True, "cornerRadiusEnd": 3} if chart_type == "bar" else {"type": chart_type, "tooltip": True, "point": True},
        "encoding": encoding,
        "config": {
            "view": {"stroke": None},
            "axis": {"labelFont": "Inter, system-ui, sans-serif", "titleFont": "Inter, system-ui, sans-serif"},
        },
    }


def build_map_intent(plan: QueryPlan, executions: list[dict[str, Any]]) -> dict[str, Any]:
    if not plan.queries or not plan.datasets or not plan.metrics:
        return {"enabled": False, "mapType": "none"}
    query = plan.queries[0]
    dataset = get_dataset(query.dataset)
    if not dataset:
        return {"enabled": False, "mapType": "none"}
    metric = dataset.metrics.get(query.metric) if query.metric else None

    if dataset.family == "fund_flow":
        return {
            "enabled": True,
            "mapType": "flow-map",
            "dataset": "fund_flow",
            "level": dataset.geography,
            "metric": query.metric,
            "title": "Fund flow",
            "buttonLabel": "Map flow",
            "reason": "Flow contracts can be mapped from origin and destination coordinates where available.",
        }

    if dataset.geography not in {"state", "county", "congress"} or query.operation in {"trend", "breakdown"}:
        return {"enabled": False, "mapType": "none"}
    map_dataset = _map_dataset(dataset.id)
    if not map_dataset:
        return {"enabled": False, "mapType": "none"}
    focus_state = None
    for filter_ in query.filters:
        if filter_.field == "state" and isinstance(filter_.value, str):
            focus_state = filter_.value
    return {
        "enabled": True,
        "mapType": "single-state-ranked-subregions" if dataset.geography in {"county", "congress"} and focus_state else "atlas-single-metric",
        "dataset": map_dataset,
        "level": dataset.geography,
        "metric": query.metric,
        "year": str(dataset.default_year) if dataset.default_year is not None else None,
        "state": focus_state,
        "topN": query.limit,
        "buttonLabel": "Map result",
        "title": metric.label if metric else dataset.display_name,
        "subtitle": f"{dataset.display_name} · {dataset.geography.title()} level",
        "reason": "Mapped from the same validated result metric used in the answer.",
        "showLegend": True,
    }
