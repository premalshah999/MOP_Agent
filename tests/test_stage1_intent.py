"""Stage 1 — Question Intent.

Asserts the classifier maps each golden question to the right intent bucket.
Skips until Stage 1 exists (Phase 2). Needs a live key or recorded fixtures.
"""

from __future__ import annotations

import pytest

from tests.conftest import llm_available
from tests.ground_truth import load_golden

intent_mod = pytest.importorskip("app.core.intent", reason="Stage 1 not implemented yet")

ACCURACY_THRESHOLD = 0.90


@pytest.mark.skipif(not llm_available(), reason="no LLM key/fixtures for Stage 1")
def test_intent_classification_accuracy() -> None:
    cases = load_golden()
    wrong: list[str] = []
    for case in cases:
        predicted = intent_mod.classify_intent(case.question).get("intent")
        if predicted != case.intent:
            wrong.append(f"{case.id}: {case.question!r} -> got {predicted}, want {case.intent}")
    accuracy = 1 - len(wrong) / len(cases)
    detail = "\n".join(wrong)
    assert accuracy >= ACCURACY_THRESHOLD, (
        f"intent accuracy {accuracy:.0%} < {ACCURACY_THRESHOLD:.0%}\n{detail}"
    )


@pytest.mark.skipif(not llm_available(), reason="no LLM key/fixtures for Stage 1")
@pytest.mark.parametrize("intent", ["CLARIFY", "UNANSWERABLE", "META", "OUT_OF_SCOPE"])
def test_non_analytical_buckets_are_not_routed_to_sql(intent: str) -> None:
    """The dangerous failure is a non-answerable question being treated as SQL."""
    cases = [c for c in load_golden() if c.intent == intent]
    leaked = [
        c.id
        for c in cases
        if intent_mod.classify_intent(c.question).get("intent") == "ANALYTICAL"
    ]
    assert not leaked, f"{intent} questions misrouted to ANALYTICAL: {leaked}"
