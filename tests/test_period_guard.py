from app.core.analysis_plan import build_analysis_contract
from app.core.period_guard import (
    canonical_period_notes,
    mixed_period_note,
    period_claim_issues,
)
from app.core.planner import _unavailable_period_reason


def test_period_guard_rejects_cross_dataset_year_carryover() -> None:
    issues = period_claim_issues(
        "| State | Financial Literacy (2021) | Debt Ratio (2021) |",
        ["Debt ratio is from the same 2021 snapshot (not FY2023)."],
        ["finra_state", "gov_state"],
        ["financial_literacy", "Debt_Ratio"],
    )
    assert issues
    assert any("Debt Ratio" in issue and "2021" in issue for issue in issues)


def test_period_guard_accepts_explicit_mixed_periods() -> None:
    issues = period_claim_issues(
        "| State | Financial Literacy (2021) | Debt Ratio (FY2023) |",
        ["Financial literacy is from 2021; debt ratio is from FY2023."],
        ["finra_state", "gov_state"],
        ["financial_literacy", "Debt_Ratio"],
    )
    assert issues == []
    assert canonical_period_notes(["finra_state", "gov_state"])


def test_mixed_period_note_is_explicit_for_cross_dataset_answers() -> None:
    note = mixed_period_note(
        ["finra_state", "gov_state"],
        {"finra_state": 2021, "gov_state": "catalog snapshot"},
    )
    assert "survey year 2021" in note
    assert "FY2023" in note
    assert "not same-year observations" in note


def test_mixed_period_note_omits_false_warning_for_same_year() -> None:
    note = mixed_period_note(
        ["contract_congress", "congress_flow"],
        {"contract_congress": 2024, "congress_flow": 2024},
    )
    assert note == ""


def test_explicit_unavailable_periods_fail_before_sql() -> None:
    reason = _unavailable_period_reason(
        "What was Maryland's financial literacy in 2024?", ["finra_state"]
    )
    assert "does not contain 2024" in reason
    assert "2021" in reason
    assert (
        _unavailable_period_reason("What was the 2020-2024 grants summary?", ["contract_state"])
        == ""
    )
    assert "no year column" in _unavailable_period_reason(
        "Maryland subaward inflow in 2024", ["state_flow"]
    )


def test_mixed_explicit_years_are_assigned_per_table() -> None:
    question = (
        "Across states, what is the correlation between 2023 median household "
        "income and 2021 financial literacy?"
    )
    strategy = "acs_state: Year=2023; finra_state: Year=2021"
    assert _unavailable_period_reason(question, ["acs_state", "finra_state"], strategy) == ""
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_state", "finra_state"],
            "columns": ["Median household income", "financial_literacy"],
            "geography_level": "state",
            "operation": "correlation",
            "year_strategy": strategy,
        },
    )
    assert contract.period_by_table == {"acs_state": 2023, "finra_state": 2021}
    assert contract.effective_period == {"acs_state": 2023, "finra_state": 2021}
