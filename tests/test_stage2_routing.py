"""Dataset-routing checks for the production planner."""

from __future__ import annotations

import pytest

from app.core.planner import classify_and_route
from tests.conftest import llm_available
from tests.ground_truth import cases_by_intent

TABLE_ACCURACY_THRESHOLD = 0.90


def _routed_tables(question: str) -> set[str]:
    result = classify_and_route(question)
    return {t for t in result.get("tables", [])}


@pytest.mark.skipif(not llm_available(), reason="no configured LLM provider")
def test_routing_table_exact_match() -> None:
    cases = cases_by_intent("ANALYTICAL")
    wrong: list[str] = []
    for case in cases:
        got = _routed_tables(case.question)
        if got != set(case.tables):
            wrong.append(f"{case.id}: {case.question!r} -> got {sorted(got)}, want {case.tables}")
    accuracy = 1 - len(wrong) / len(cases)
    assert accuracy >= TABLE_ACCURACY_THRESHOLD, (
        f"routing table accuracy {accuracy:.0%} < {TABLE_ACCURACY_THRESHOLD:.0%}\n"
        + "\n".join(wrong)
    )


@pytest.mark.skipif(not llm_available(), reason="no configured LLM provider")
def test_no_fund_flow_for_cash_flow_question() -> None:
    """Classic trap: 'free cash flow' is gov_*, never the *_flow subaward tables."""
    got = _routed_tables("Maryland congressional districts by free cash flow")
    assert "gov_congress" in got
    assert not (got & {"state_flow", "county_flow", "congress_flow"})
