from __future__ import annotations

from pydantic import BaseModel, Field


class DimensionDefinition(BaseModel):
    id: str
    column: str
    label: str
    description: str = ""
    synonyms: list[str] = Field(default_factory=list)


class MetricDefinition(BaseModel):
    id: str
    label: str
    description: str
    sql: str
    unit: str = "value"
    aggregation: str = "sum"
    synonyms: list[str] = Field(default_factory=list)
    default_for: list[str] = Field(default_factory=list)
    semantic_concept: str | None = None
    semantic_variant: str | None = None
    related_variants: dict[str, str] = Field(default_factory=dict)


class DatasetDefinition(BaseModel):
    id: str
    display_name: str
    description: str
    table_name: str
    view_name: str
    grain: str
    geography: str
    family: str = "general"
    primary_key: str | None = None
    date_column: str | None = None
    year_column: str | None = None
    default_year: str | int | None = None
    available_years: list[str | int] = Field(default_factory=list)
    label_column: str
    dimensions: dict[str, DimensionDefinition] = Field(default_factory=dict)
    metrics: dict[str, MetricDefinition] = Field(default_factory=dict)
    default_filters: list[str] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
    example_questions: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)


class SemanticRegistrySnapshot(BaseModel):
    version: str
    datasets: dict[str, DatasetDefinition]
