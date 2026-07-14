from __future__ import annotations

from app.core.evidence_renderer import render_verified_rows


def test_scalar_money_fallback_is_polished_and_structured() -> None:
    result = render_verified_rows(
        [{"state": "Maryland", "total_outflow": 25_114_674_528.13}]
    )
    assert result["answer"] == "Verified total outflow for Maryland: **$25.11B**."
    assert result["key_numbers"] == [
        {
            "label": "Total outflow",
            "value": 25_114_674_528.13,
            "unit": "USD",
        }
    ]


def test_analyst_verified_scalar_is_not_labeled_as_a_fallback() -> None:
    result = render_verified_rows(
        [{"source": "Maryland", "total_outflow": 25_114_674_528.13}],
        fallback=False,
    )
    assert result["answer"] == "Verified total outflow for Maryland: **$25.11B**."
    assert "Analyst-verified query" in result["caveats"][0]
    assert "fallback" not in result["caveats"][0].lower()


def test_multirow_fallback_still_copies_a_table() -> None:
    result = render_verified_rows(
        [{"state": "Maryland", "value": 2}, {"state": "Virginia", "value": 1}]
    )
    assert "| state | value |" in result["answer"]
