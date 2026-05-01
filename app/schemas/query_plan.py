from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


Intent = Literal[
    "DIRECT_LOOKUP",
    "AGGREGATION",
    "TREND",
    "COMPARISON",
    "BREAKDOWN",
    "ROOT_CAUSE",
    "DEFINITION",
    "DATA_QUALITY",
    "AMBIGUOUS",
    "UNANSWERABLE",
]


class TimeRange(BaseModel):
    start: str
    end: str


class Filter(BaseModel):
    field: str
    operator: Literal["=", "!=", ">", ">=", "<", "<=", "IN", "LIKE"]
    value: Any


class QuerySpec(BaseModel):
    name: str
    purpose: str
    dataset: str
    operation: Literal["ranking", "lookup", "position", "compare", "trend", "breakdown", "flow_ranking", "flow_pair"] = "ranking"
    metric: str | None = None
    dimensions: list[str] = Field(default_factory=list)
    filters: list[Filter] = Field(default_factory=list)
    order: Literal["ASC", "DESC"] = "DESC"
    limit: int | None = None


class QueryPlan(BaseModel):
    interpreted_question: str
    intent: Intent
    datasets: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    time_range: TimeRange | None = None
    comparison_time_range: TimeRange | None = None
    filters: list[Filter] = Field(default_factory=list)
    queries: list[QuerySpec] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    ambiguities: list[str] = Field(default_factory=list)
    alternatives: list[str] = Field(default_factory=list)
