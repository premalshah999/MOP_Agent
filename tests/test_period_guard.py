from app.core.period_guard import canonical_period_notes, mixed_period_note, period_claim_issues


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
