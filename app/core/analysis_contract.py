"""Typed analytical intent shared by planning, SQL validation, and the UI.

The LLM still decides what the question means.  This module turns that decision
into a small, provider-neutral contract and reconciles only facts that are
explicit in the user's wording (years, top-N, sort and flow direction).  Those
facts then become invariants instead of suggestions buried in a prompt.
"""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.semantic.registry import get_dataset


Operation = Literal[
    "lookup", "ranking", "comparison", "trend", "correlation",
    "distribution", "aggregate", "breakdown",
]
FlowDirection = Literal["inflow", "outflow", "none"]
SortDirection = Literal["asc", "desc", "none"]

_OPERATIONS = {
    "lookup", "ranking", "comparison", "trend", "correlation",
    "distribution", "aggregate", "breakdown",
}
_YEAR_RE = re.compile(r"\b(?:fy\s*)?((?:19|20)\d{2})\b", re.IGNORECASE)
_PERIOD_RE = re.compile(
    r"\b((?:19|20)\d{2})\s*(?:-|–|—|to|through)\s*((?:19|20)\d{2})\b",
    re.IGNORECASE,
)
_TOP_RE = re.compile(
    r"\b(?:top|bottom|first|last|highest|lowest|largest|smallest|most|least)\s+(\d{1,3})\b",
    re.IGNORECASE,
)


class AnalysisContract(BaseModel):
    intent: str = "ANALYTICAL"
    tables: list[str] = Field(default_factory=list)
    metric_columns: list[str] = Field(default_factory=list)
    geography_level: str = "none"
    operation: Operation = "lookup"
    flow_direction: FlowDirection = "none"
    explicit_year: int | None = None
    requested_years: list[int] = Field(default_factory=list)
    requested_period: str | None = None
    effective_period: Any = None
    year_strategy: str = ""
    sort_direction: SortDirection = "none"
    top_k: int | None = None
    join_plan: str = ""
    assumptions: list[str] = Field(default_factory=list)


def _explicit_operation(question: str, proposed: Any) -> Operation:
    q = question.lower()
    rank_q = re.sub(r"\bat\s+(?:least|most)\b", "", q)
    if re.search(r"\b(correlat\w*|relationship|association)\b", q):
        return "correlation"
    if re.search(r"\b(trend|over time|year[- ]over[- ]year|by year)\b", q):
        return "trend"
    if re.search(r"\b(top|bottom|rank|highest|lowest|largest|smallest|most|least)\b", rank_q):
        return "ranking"
    if re.search(r"\b(compare|versus|vs\.?|difference between)\b", q):
        return "comparison"
    if re.search(r"\b(by agency|by category|breakdown|composition)\b", q):
        return "breakdown"
    if re.search(r"\b(total|average|mean|median|sum)\b", q):
        return "aggregate"
    if re.search(r"\bat\s+(?:least|most)\b", q):
        return "lookup"
    proposed_norm = str(proposed or "").strip().lower()
    if proposed_norm in _OPERATIONS:
        return proposed_norm  # type: ignore[return-value]
    return "lookup"


def _flow_direction(question: str, proposed: Any) -> FlowDirection:
    q = question.lower()
    # User wording wins over the model because these phrases are unambiguous.
    if re.search(r"\b(receiv\w*|inflow|incoming|coming in|flowing to)\b", q):
        return "inflow"
    if re.search(r"\b(send\w*|outflow|outgoing|going out|flowing from)\b", q):
        return "outflow"
    value = str(proposed or "").strip().lower()
    return value if value in {"inflow", "outflow"} else "none"  # type: ignore[return-value]


def _sort_direction(
    question: str,
    *,
    operation: Operation,
) -> SortDirection:
    q = re.sub(r"\bat\s+(?:least|most)\b", "", question.lower())
    if re.search(r"\b(bottom|lowest|smallest|least|fewest|ascending)\b", q):
        return "asc"
    if re.search(r"\b(top|highest|largest|most|greatest|descending)\b", q):
        return "desc"
    if re.search(r"\bat\s+(?:least|most)\b", question.lower()):
        return "none"
    # Sorting is semantically meaningful only for a ranking.  For lookup,
    # aggregate, comparison, correlation, and trend questions, accepting a
    # model-proposed direction made the same scalar query alternate between
    # `none` and `desc` even though its SQL and result were identical.  A bare
    # ranking uses the conventional highest-first ordering; explicit user
    # wording above always wins.
    if operation == "ranking":
        return "desc"
    return "none"


def build_analysis_contract(question: str, routing: dict[str, Any]) -> AnalysisContract:
    years = [int(value) for value in _YEAR_RE.findall(question)]
    period_match = _PERIOD_RE.search(question)
    requested_period = (
        f"{period_match.group(1)}-{period_match.group(2)}" if period_match else None
    )
    top = _TOP_RE.search(question)
    proposed_top = routing.get("top_k")
    try:
        proposed_top_int = int(proposed_top) if proposed_top is not None else None
    except (TypeError, ValueError):
        proposed_top_int = None
    operation = _explicit_operation(question, routing.get("operation"))
    top_k = int(top.group(1)) if top else proposed_top_int
    if top_k is None and operation == "ranking":
        q = question.lower()
        if re.search(r"\b(which|what|where)\b.*\b(most|highest|lowest|largest|smallest|maximum|minimum|worst|best)\b", q):
            top_k = 1
        elif re.search(r"\b(most|highest|lowest|largest|smallest|maximum|minimum|worst|best)\b", q):
            top_k = 10
    if top_k is not None and not 1 <= top_k <= 250:
        top_k = None

    # Assumptions shown to users must come from the catalog, not an unverified
    # free-text model field (which previously mislabeled ACS percentages as counts).
    assumptions: list[str] = []
    effective_by_table: dict[str, Any] = {}
    for table in routing.get("tables") or []:
        dataset = get_dataset(str(table))
        if dataset is None:
            continue
        available = {str(value) for value in dataset.available_years}
        if requested_period and requested_period in available:
            effective_by_table[str(table)] = requested_period
        elif len(set(years)) > 1:
            effective_by_table[str(table)] = list(dict.fromkeys(years))
        elif years:
            effective_by_table[str(table)] = years[-1]
        elif operation == "trend":
            effective_by_table[str(table)] = "all available periods"
        elif dataset.default_year is not None:
            effective_by_table[str(table)] = dataset.default_year
        else:
            effective_by_table[str(table)] = "catalog snapshot"
    if len(effective_by_table) == 1:
        effective_period: Any = next(iter(effective_by_table.values()))
    else:
        effective_period = effective_by_table or None
    if not years and operation != "trend":
        for table in routing.get("tables") or []:
            dataset = get_dataset(str(table))
            if dataset is not None and dataset.default_year is not None:
                assumptions.append(f"{table} uses its catalog default period {dataset.default_year}.")
    if requested_period:
        assumptions.append(f"Uses the catalog's pre-aggregated {requested_period} period where available.")
    is_flow_query = any(str(table).endswith("_flow") for table in (routing.get("tables") or []))
    return AnalysisContract(
        tables=list(routing.get("tables") or []),
        metric_columns=[str(x) for x in (routing.get("columns") or [])],
        geography_level=str(routing.get("geography_level") or "none"),
        operation=operation,
        flow_direction=(
            _flow_direction(question, routing.get("flow_direction"))
            if is_flow_query else "none"
        ),
        explicit_year=years[-1] if years else None,
        requested_years=years,
        requested_period=requested_period,
        effective_period=effective_period,
        year_strategy=str(routing.get("year_strategy") or ""),
        sort_direction=_sort_direction(
            question,
            operation=operation,
        ),
        top_k=top_k,
        join_plan=str(routing.get("join_plan") or ""),
        assumptions=assumptions,
    )
