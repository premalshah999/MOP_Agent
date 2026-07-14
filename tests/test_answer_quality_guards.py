from __future__ import annotations

import json

from app.core.formatting import validate_key_numbers_against_rows
from app.core.peer_context import compute_peer_context, render_peer_context
from app.evals.faithfulness import judge_faithfulness
from app.llm import client


def test_incomplete_answer_fails_blocking_judge() -> None:
    client.set_stub(
        lambda messages, json_mode, purpose: json.dumps(
            {"faithful": True, "complete": False, "reason": "Virginia was omitted."}
        )
    )
    try:
        verdict = judge_faithfulness(
            "Compare Maryland and Virginia grants.",
            "Maryland received $100.",
            [{"state": "Maryland", "grants": 100}, {"state": "Virginia", "grants": 90}],
        )
    finally:
        client.clear_stub()
    assert verdict["data_faithful"] is True
    assert verdict["complete"] is False
    assert verdict["faithful"] is False


def test_peer_context_preserves_ratio_precision_and_rank_direction() -> None:
    text = render_peer_context(
        {
            "focus_state": "Maryland",
            "measure": "Debt_Ratio",
            "rank": 11,
            "total_states": 52,
            "value": 0.824,
            "national_median": 0.7873,
            "prior_year": 2022,
            "prior_value": 0.8012,
            "yoy_change_pct": 2.8,
        }
    )
    assert "Maryland highest-first rank" in text
    assert "0.7873" in text
    assert "was 0.8012" in text


def test_peer_context_is_not_attached_to_multi_entity_rows() -> None:
    assert compute_peer_context(
        table="acs_state",
        focus_state="Maryland",
        year=2023,
        routing_columns=["Below poverty"],
        rows=[
            {"state": "Maryland", "Below poverty": 9.1},
            {"state": "Virginia", "Below poverty": 9.9},
        ],
    ) is None


def test_unverified_rank_key_number_is_dropped() -> None:
    kept, dropped = validate_key_numbers_against_rows(
        [{"label": "National rank", "value": 11, "unit": "of 52"}],
        [{"state": "Virginia", "poverty_rate": 9.8}],
    )
    assert kept == []
    assert dropped == ["National rank"]


def test_endpoint_trend_delta_is_verified() -> None:
    item = {"label": "Change 2009 to 2021", "value": 0.07, "unit": "index points"}
    kept, dropped = validate_key_numbers_against_rows(
        [item],
        [{"Year": 2009, "index": 0.61}, {"Year": 2021, "index": 0.68}],
    )
    assert kept == [item]
    assert dropped == []
