from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


AssistantMode = Literal[
    "GENERAL_ASSISTANT",
    "ASSISTANT_HELP",
    "DATASET_DISCOVERY",
    "METRIC_DEFINITION",
    "SIMPLE_ANALYTICS",
    "COMPLEX_ANALYTICS",
    "ROOT_CAUSE_ANALYSIS",
    "FOLLOW_UP_ANALYTICS",
    "VISUALIZATION_REQUEST",
    "CLARIFICATION_RESPONSE",
    "OUT_OF_SCOPE",
]


class RouterOutput(BaseModel):
    mode: AssistantMode
    confidence: Literal["high", "medium", "low"] = "medium"
    requires_sql: bool = False
    requires_metadata: bool = True
    is_follow_up: bool = False
    needs_clarification: bool = False
    clarification_question: str | None = None
    reason: str = ""

