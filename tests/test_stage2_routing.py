"""Stage 2 — Question Routing (the most important gate).

Exact-table match between the router's chosen tables and the golden tables.
Skips until Stage 2 exists (Phase 2).
"""

from __future__ import annotations

import pytest

from tests.conftest import llm_available
from tests.ground_truth import cases_by_intent

router_mod = pytest.importorskip("app.core.router", reason="Stage 2 not implemented yet")

TABLE_ACCURACY_THRESHOLD = 0.90


def _routed_tables(question: str) -> set[str]:
    result = router_mod.route(question)
    return {t for t in result.get("tables", [])}


@pytest.mark.skipif(not llm_available(), reason="no LLM key/fixtures for Stage 2")
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


@pytest.mark.skipif(not llm_available(), reason="no LLM key/fixtures for Stage 2")
def test_no_fund_flow_for_cash_flow_question() -> None:
    """Classic trap: 'free cash flow' is gov_*, never the *_flow subaward tables."""
    got = _routed_tables("Maryland congressional districts by free cash flow")
    assert "gov_congress" in got
    assert not (got & {"state_flow", "county_flow", "congress_flow"})
