from __future__ import annotations

from pydantic import BaseModel, Field


class RetrievedMetric(BaseModel):
    dataset_id: str
    metric_id: str
    label: str
    description: str
    score: float


class RetrievedDataset(BaseModel):
    dataset_id: str
    display_name: str
    description: str
    score: float


class SemanticContext(BaseModel):
    datasets: list[RetrievedDataset] = Field(default_factory=list)
    metrics: list[RetrievedMetric] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
