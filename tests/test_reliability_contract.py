from __future__ import annotations

import json

from app.core.analysis_contract import build_analysis_contract
from app.core.contextualize import prior_history
from app.core.verified_queries import match as match_verified_query
from app.core.verified_queries import reload_repo
from app.evals.faithfulness import judge_faithfulness
from app.llm import client
from app.semantic import value_resolver
from app.sql.semantic_validator import (
    normalize_generated_sql,
    semantic_sql_problems,
    stabilize_verified_ranking_sql,
)


def test_generic_state_language_does_not_invent_department_of_state(monkeypatch):
    monkeypatch.setattr(
        value_resolver,
        "distinct_values",
        lambda table, column, limit=2000: (
            "Department of State",
            "Department of Defense",
        ),
    )
    assert value_resolver.resolve_filter_value(
        "spending_state_agency",
        "agency_name",
        "What is the state average grant funding?",
    ) is None


def test_generic_state_language_does_not_invent_geography(monkeypatch):
    monkeypatch.setattr(
        value_resolver,
        "distinct_values",
        lambda table, column, limit=2000: ("VERMONT", "VIRGINIA", "MARYLAND"),
    )
    assert value_resolver.resolve_filter_value(
        "contract_state", "state", "Correlation between state grants and poverty"
    ) is None


def test_multiple_entities_and_typo_are_preserved(monkeypatch):
    monkeypatch.setattr(
        value_resolver,
        "distinct_values",
        lambda table, column, limit=2000: ("MARYLAND", "VIRGINIA", "VERMONT"),
    )
    matches = value_resolver.resolve_filter_values(
        "contract_state", "state", "Compare Marylnd and Virginia"
    )
    assert [value for value, _ in matches] == ["VIRGINIA", "MARYLAND"]


def test_current_question_is_removed_only_from_trailing_history():
    history = [
        {"role": "user", "content": "Earlier question"},
        {"role": "assistant", "content": "Earlier answer", "contract": {"metric": "grants"}},
        {"role": "user", "content": "Same question"},
    ]
    cleaned = prior_history(history, "  same   question ")
    assert [item["content"] for item in cleaned] == ["Earlier question", "Earlier answer"]


def test_semantic_validator_rejects_invented_agency_filter():
    contract = build_analysis_contract(
        "What is the state average of grants?",
        {
            "tables": ["spending_state_agency"],
            "columns": ["grant"],
            "geography_level": "state",
            "operation": "aggregate",
        },
    )
    sql = (
        'SELECT state, SUM("Grants") AS grants FROM mart_spending_state_agency '
        "WHERE year = '2024' AND agency_name = 'Department of State' GROUP BY state"
    )
    problems = semantic_sql_problems(sql, "What is the state average of grants?", contract, {})
    assert any("not grounded" in problem for problem in problems)


def test_semantic_validator_requires_default_year():
    contract = build_analysis_contract(
        "How much grant funding did Maryland receive?",
        {
            "tables": ["contract_state"],
            "columns": ["grant"],
            "geography_level": "state",
            "operation": "lookup",
        },
    )
    resolved = {"contract_state": {"state": {"value": "MARYLAND", "values": ["MARYLAND"]}}}
    sql = 'SELECT state, SUM("Grants") FROM mart_contract_state WHERE state=\'MARYLAND\' GROUP BY state'
    problems = semantic_sql_problems(sql, "How much grant funding did Maryland receive?", contract, resolved)
    assert any("must constrain" in problem for problem in problems)


def test_semantic_validator_rejects_wrong_measure():
    contract = build_analysis_contract(
        "How many grant dollars did Maryland receive?",
        {
            "tables": ["contract_state"],
            "columns": ["Grants"],
            "geography_level": "state",
            "operation": "lookup",
        },
    )
    resolved = {"contract_state": {"state": {"value": "MARYLAND", "values": ["MARYLAND"]}}}
    sql = (
        'SELECT state, "Contracts" FROM mart_contract_state '
        "WHERE state='MARYLAND' AND year='2024'"
    )
    problems = semantic_sql_problems(
        sql, "How many grant dollars did Maryland receive?", contract, resolved
    )
    assert any("required metric" in problem for problem in problems)


def test_valid_correlation_contract_passes():
    contract = build_analysis_contract(
        "What is the correlation between state grants and poverty?",
        {
            "tables": ["contract_state", "acs_state"],
            "columns": ["grant", "Poverty rate"],
            "geography_level": "state",
            "operation": "correlation",
        },
    )
    sql = '''
        SELECT CORR(a.grant, b."Poverty rate") AS correlation
        FROM mart_contract_state a
        JOIN mart_acs_state b ON LOWER(a.state) = LOWER(b.state)
        WHERE a.year = '2024' AND b."Year" = 2023
    '''
    assert semantic_sql_problems(
        sql, "What is the correlation between state grants and poverty?", contract, {}
    ) == []
    normalized = normalize_generated_sql(
        sql.replace("AS correlation", "AS grant_poverty_correlation"), contract
    )
    assert "AS correlation" in normalized
    assert "grant_poverty_correlation" not in normalized


def test_multi_year_aggregate_uses_catalog_period_row():
    contract = build_analysis_contract(
        "total grants to Texas over the 2020-2024 period",
        {
            "tables": ["contract_state"],
            "columns": ["Grants"],
            "geography_level": "state",
            "operation": "aggregate",
        },
    )
    resolved = {"contract_state": {"state": {"value": "TEXAS", "values": ["TEXAS"]}}}
    wrong = (
        'SELECT SUM("Grants") FROM mart_contract_state '
        "WHERE state='TEXAS' AND year='2024'"
    )
    right = wrong.replace("year='2024'", "year='2020-2024'")
    assert contract.requested_period == "2020-2024"
    assert any("2020-2024" in p for p in semantic_sql_problems(
        wrong, "total grants to Texas over the 2020-2024 period", contract, resolved
    ))
    assert semantic_sql_problems(
        right, "total grants to Texas over the 2020-2024 period", contract, resolved
    ) == []


def test_at_least_is_not_a_lowest_ranking():
    contract = build_analysis_contract(
        "what percent of Virginia adults have at least a bachelor's degree",
        {
            "tables": ["acs_state"],
            "columns": ["Education >= Bachelor's"],
            "geography_level": "state",
            "operation": "ranking",
            "sort_direction": "asc",
        },
    )
    assert contract.operation == "lookup"
    assert contract.sort_direction == "none"
    assert contract.top_k is None


def test_scalar_aggregate_ignores_model_only_sort_direction():
    base = {
        "tables": ["state_flow"],
        "columns": ["subaward_amount_year"],
        "operation": "aggregate",
    }
    none_contract = build_analysis_contract(
        "subcontract inflow to Maryland",
        {**base, "sort_direction": "none"},
    )
    desc_contract = build_analysis_contract(
        "subcontract inflow to Maryland",
        {**base, "sort_direction": "desc"},
    )
    assert none_contract.sort_direction == "none"
    assert desc_contract.sort_direction == "none"


def test_maryland_flow_totals_have_exact_verified_queries():
    reload_repo()
    inflow = match_verified_query("How much subcontract funding flows into Maryland?")
    outflow = match_verified_query("How much subcontract funding flows out of Maryland?")
    assert inflow and inflow["id"] == "vq042" and inflow["_mode"] == "exact"
    assert outflow and outflow["id"] == "vq043" and outflow["_mode"] == "exact"
    assert inflow["direct_render"] is True
    assert outflow["direct_render"] is True
    assert "intra-state" in inflow["caveats"][0]
    assert "intra-state" in outflow["caveats"][0]


def test_implicit_superlative_gets_bounded_result_shape():
    single = build_analysis_contract(
        "which California county carries the most bonds",
        {"tables": ["gov_county"], "columns": ["Bonds,_Loans_&_Notes"], "operation": "ranking"},
    )
    plural = build_analysis_contract(
        "most financially literate congressional districts",
        {"tables": ["finra_congress"], "columns": ["financial_literacy"], "operation": "ranking"},
    )
    assert single.top_k == 1
    assert plural.top_k == 10


def test_ranking_requires_requested_limit_direction_and_tie_breaker():
    contract = build_analysis_contract(
        "Bottom 5 states by grants",
        {
            "tables": ["contract_state"],
            "columns": ["grant"],
            "geography_level": "state",
            "operation": "ranking",
        },
    )
    wrong = (
        'SELECT state, "Grants" FROM mart_contract_state WHERE year=\'2024\' '
        'ORDER BY "Grants" DESC LIMIT 10'
    )
    problems = semantic_sql_problems(wrong, "Bottom 5 states by grants", contract, {})
    assert any("LIMIT" in problem for problem in problems)
    assert any("tie-breaker" in problem for problem in problems)
    assert any("sort direction" in problem for problem in problems)


def test_verified_ranking_gets_stable_label_tie_breaker():
    contract = build_analysis_contract(
        "top 10 counties in Maryland by grants",
        {
            "tables": ["contract_county"],
            "columns": ["Grants"],
            "geography_level": "county",
            "operation": "ranking",
        },
    )
    original = (
        'SELECT county, "Grants" FROM mart_contract_county '
        "WHERE LOWER(state)='maryland' AND year='2024' "
        'ORDER BY "Grants" DESC LIMIT 10'
    )
    stable = stabilize_verified_ranking_sql(original, contract)
    assert "county ASC" in stable
    assert semantic_sql_problems(
        stable, "top 10 counties in Maryland by grants", contract,
        {"contract_county": {"state": {"value": "MARYLAND", "values": ["MARYLAND"]}}},
    ) == []


def test_faithfulness_string_false_is_not_truthy():
    client.set_stub(lambda messages, json_mode, purpose: json.dumps({"faithful": "false", "reason": "wrong"}))
    try:
        verdict = judge_faithfulness("q", "answer", [{"value": 1}], "SELECT 1")
    finally:
        client.clear_stub()
    assert verdict["faithful"] is False
    assert verdict["available"] is True


def test_faithfulness_unavailable_fails_closed():
    def broken(messages, json_mode, purpose):
        raise client.LLMError("down")

    client.set_stub(broken)
    try:
        verdict = judge_faithfulness("q", "answer", [{"value": 1}], "SELECT 1")
    finally:
        client.clear_stub()
    assert verdict["faithful"] is False
    assert verdict["available"] is False
