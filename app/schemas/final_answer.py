from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class KeyNumber(BaseModel):
    label: str
    value: float | int | str
    unit: str | None = None


class FinalAnswer(BaseModel):
    answer: str
    key_numbers: list[KeyNumber] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
    sql_used: list[str] = Field(default_factory=list)
    confidence: Literal["high", "medium", "low"] = "medium"
