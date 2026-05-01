from __future__ import annotations

import re
from dataclasses import dataclass

from app.semantic.registry import get_dataset


@dataclass(frozen=True)
class MetricProxyResolution:
    metric_id: str
    assumptions: list[str]
    alternatives: list[str]


def _asks_for_rate(question: str) -> bool:
    q = question.lower()
    return bool(
        re.search(r"\b(per\s*capita|per\s*person|per\s*resident|per\s*1000|per\s*1,000|rate|density)\b", q)
    )


def resolve_unavailable_metric_proxy(
    *,
    question: str,
    dataset_id: str,
    matches_elsewhere: list[tuple[str, str, float]],
) -> MetricProxyResolution | None:
    """Apply documented nearest-metric defaults when exact geography support is missing.

    This is intentionally narrow. A proxy can be used only when:
    - the requested metric is clearly present elsewhere in the registry;
    - the requested dataset has a documented nearby metric; and
    - the response can state the substitution as an assumption.
    """

    dataset = get_dataset(dataset_id)
    if not dataset or not matches_elsewhere:
        return None

    requested_metric_id = matches_elsewhere[0][1]
    requested_label = requested_metric_id.replace("_", " ").title()

    if dataset_id == "contract_county" and requested_metric_id in {"employees", "employees_per_1000"}:
        proxy_metric_id = "federal_residents_per_1000" if _asks_for_rate(question) else "federal_residents"
        if proxy_metric_id not in dataset.metrics:
            return None
        proxy_metric = dataset.metrics[proxy_metric_id]
        return MetricProxyResolution(
            metric_id=proxy_metric_id,
            assumptions=[
                (
                    f"County-level {requested_label} is not loaded. Used {proxy_metric.label} as the documented "
                    "county-level employment-related proxy."
                ),
                "This is a proxy answer, not a direct employee-count answer.",
            ],
            alternatives=[
                "For actual Employees, use the loaded state or agency level.",
                "Ask `rank agencies in Maryland by federal employees` for Maryland agency employment.",
                "Ask `rank top 10 states based on employment` for state-level employment.",
            ],
        )

    if dataset_id == "contract_county" and requested_metric_id in {"employees_wage", "employees_wage_per_1000"}:
        proxy_metric_id = "resident_wage_per_1000" if _asks_for_rate(question) else "resident_wage"
        if proxy_metric_id not in dataset.metrics:
            return None
        proxy_metric = dataset.metrics[proxy_metric_id]
        return MetricProxyResolution(
            metric_id=proxy_metric_id,
            assumptions=[
                (
                    f"County-level {requested_label} is not loaded. Used {proxy_metric.label} as the documented "
                    "county-level wage proxy."
                ),
                "This is a proxy answer, not a direct employee-wage answer.",
            ],
            alternatives=[
                "For actual Employees wage, use the loaded state level.",
                "For county analysis, compare Resident wage or Resident wage per 1,000.",
            ],
        )

    return None
