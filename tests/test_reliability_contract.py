from __future__ import annotations

import json

import pytest

from app.core.analysis_plan import (
    build_analysis_contract,
    resolve_periods_by_table,
    semantic_plan_from_routing,
)
from app.core.conversation import prior_history
from app.core.query_engine import generate_and_execute
from app.llm import client
from app.quality.faithfulness import judge_faithfulness
from app.semantic import value_resolver
from app.sql.semantic_validator import (
    normalize_generated_sql,
    result_shape_problems,
    semantic_sql_problems,
)
from app.sql.validator import validate_sql


def test_generic_state_language_does_not_invent_department_of_state(monkeypatch):
    monkeypatch.setattr(
        value_resolver,
        "distinct_values",
        lambda table, column, limit=2000: (
            "Department of State",
            "Department of Defense",
        ),
    )
    assert (
        value_resolver.resolve_filter_value(
            "spending_state_agency",
            "agency_name",
            "What is the state average grant funding?",
        )
        is None
    )


def test_generic_state_language_does_not_invent_geography(monkeypatch):
    monkeypatch.setattr(
        value_resolver,
        "distinct_values",
        lambda table, column, limit=2000: ("VERMONT", "VIRGINIA", "MARYLAND"),
    )
    assert (
        value_resolver.resolve_filter_value(
            "contract_state", "state", "Correlation between state grants and poverty"
        )
        is None
    )


def test_planned_agency_filter_resolves_spelling_variant_and_prefix_typo(monkeypatch):
    monkeypatch.setattr(
        value_resolver,
        "distinct_values",
        lambda table, column, limit=2000: (
            ("Department of Defense", "Department of State") if column == "agency" else ()
        ),
    )

    resolved = value_resolver.resolve_entities(
        "spending_state_agency",
        "epartment of defence biggest deals by state",
        allowed_columns=["agency"],
    )

    assert resolved["agency"]["values"] == ["Department of Defense"]


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


def test_exact_state_does_not_admit_broader_fuzzy_sibling(monkeypatch):
    monkeypatch.setattr(
        value_resolver,
        "distinct_values",
        lambda table, column, limit=2000: ("virginia", "west virginia"),
    )
    matches = value_resolver.resolve_filter_values(
        "acs_state", "state", "What was Virginia's poverty rate in 2023?"
    )
    assert matches == [("virginia", 1.0)]


def test_long_state_name_does_not_also_resolve_embedded_state(monkeypatch):
    monkeypatch.setattr(
        value_resolver,
        "distinct_values",
        lambda table, column, limit=2000: ("virginia", "west virginia"),
    )
    only_west = value_resolver.resolve_filter_values(
        "acs_state", "state", "What was West Virginia's poverty rate?"
    )
    both = value_resolver.resolve_filter_values(
        "acs_state", "state", "Compare Virginia and West Virginia"
    )
    assert only_west == [("west virginia", 1.0)]
    assert [value for value, _ in both] == ["west virginia", "virginia"]


def test_state_postal_alias_does_not_admit_longer_state(monkeypatch):
    monkeypatch.setattr(
        value_resolver,
        "distinct_values",
        lambda table, column, limit=2000: ("maryland", "virginia", "west virginia"),
    )
    matches = value_resolver.resolve_filter_values(
        "contract_state", "state", "Compare MD and VA on grants"
    )
    assert [value for value, _ in matches] == ["maryland", "virginia"]


def test_exact_district_code_does_not_expand_to_every_state_district(monkeypatch):
    monkeypatch.setattr(
        value_resolver,
        "distinct_values",
        lambda table, column, limit=2000: tuple(
            f"Maryland CD-{number:02d}" for number in range(1, 9)
        ),
    )

    matches = value_resolver.resolve_filter_values(
        "congress_flow", "rcpt_cd_name", "Which destinations receive funding from MD-08?"
    )

    assert matches == [("Maryland CD-08", 0.99)]


def test_table_scoped_year_does_not_leak_into_fixed_snapshot():
    periods = resolve_periods_by_table(
        ["finra_state", "gov_state"],
        [2021],
        None,
        "finra_state: Year=2021; gov_state: catalog snapshot",
        operation="correlation",
    )

    assert periods == {"finra_state": 2021, "gov_state": "catalog snapshot"}


def test_state_scope_does_not_invent_same_named_or_typo_county(monkeypatch):
    def values(table, column, limit=2000):
        if column == "state":
            return ("wyoming", "west virginia")
        if column == "county":
            return ("wyoming", "tate", "albany")
        return ()

    monkeypatch.setattr(value_resolver, "distinct_values", values)
    resolved = value_resolver.resolve_entities(
        "acs_county", "Rank the Wyoming counties by Black and Asian population"
    )
    assert resolved["state"]["values"] == ["wyoming"]
    assert "county" not in resolved


def test_measure_word_does_not_fuzzy_resolve_to_county(monkeypatch):
    monkeypatch.setattr(
        value_resolver,
        "distinct_values",
        lambda table, column, limit=2000: ("towner",) if column == "county" else (),
    )
    resolved = value_resolver.resolve_entities(
        "acs_county",
        "Which counties have more renter occupied than owner occupied housing?",
    )
    assert "county" not in resolved


def test_metric_named_county_requires_explicit_county_word(monkeypatch):
    monkeypatch.setattr(
        value_resolver,
        "distinct_values",
        lambda table, column, limit=2000: ("grant",) if column == "county" else (),
    )
    generic = value_resolver.resolve_entities("contract_county", "Show federal grants by county.")
    explicit = value_resolver.resolve_entities(
        "contract_county", "Show federal grants in Grant County."
    )
    assert "county" not in generic
    assert explicit["county"]["values"] == ["grant"]


def test_explicit_same_named_county_is_preserved(monkeypatch):
    def values(table, column, limit=2000):
        if column == "state":
            return ("wyoming", "west virginia")
        if column == "county":
            return ("wyoming", "tate")
        return ()

    monkeypatch.setattr(value_resolver, "distinct_values", values)
    resolved = value_resolver.resolve_entities("acs_county", "Show Wyoming County, West Virginia")
    assert resolved["state"]["values"] == ["west virginia"]
    assert resolved["county"]["values"] == ["wyoming"]


def test_read_only_union_query_is_allowed():
    validate_sql(
        'WITH base AS (SELECT "Median household income" AS income, '
        '"Education >= High School" AS education FROM mart_acs_county '
        "WHERE Year = 2023) "
        "SELECT 'income vs education' AS comparison, CORR(income, education) AS correlation FROM base "
        "UNION ALL SELECT 'constant check', AVG(income) FROM base"
    )


def test_net_flow_requires_both_sides_and_subtraction():
    contract = build_analysis_contract(
        "What is Maryland's net subcontract flow?",
        {
            "tables": ["state_flow"],
            "columns": ["subaward_amount_year"],
            "geography_level": "state",
            "operation": "aggregate",
            "flow_direction": "none",
            "year_strategy": "no year filter",
        },
    )
    assert contract.flow_direction == "none"
    resolved = {
        "state_flow": {
            "rcpt_state_name": {"value": "Maryland", "values": ["Maryland"]},
            "subawardee_state_name": {"value": "Maryland", "values": ["Maryland"]},
        }
    }
    bad = (
        "SELECT SUM(subaward_amount_year) AS net_flow FROM mart_state_flow "
        "WHERE subawardee_state_name = 'Maryland'"
    )
    good = (
        "SELECT SUM(CASE WHEN subawardee_state_name = 'Maryland' THEN subaward_amount_year ELSE 0 END) "
        "- SUM(CASE WHEN rcpt_state_name = 'Maryland' THEN subaward_amount_year ELSE 0 END) AS net_flow "
        "FROM mart_state_flow"
    )
    assert any(
        "destination inflow minus origin outflow" in problem
        for problem in semantic_sql_problems(
            bad, "What is Maryland's net subcontract flow?", contract, resolved
        )
    )
    assert (
        semantic_sql_problems(good, "What is Maryland's net subcontract flow?", contract, resolved)
        == []
    )


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
    sql = "SELECT state, SUM(\"Grants\") FROM mart_contract_state WHERE state='MARYLAND' GROUP BY state"
    problems = semantic_sql_problems(
        sql, "How much grant funding did Maryland receive?", contract, resolved
    )
    assert any("must constrain" in problem for problem in problems)


def test_intermediate_query_requires_period_only_for_tables_it_reads():
    question = (
        "Which Maryland county has the highest poverty rate, and how does its "
        "debt ratio compare to the state median?"
    )
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_county", "gov_county", "gov_state"],
            "columns": ["Below poverty", "Debt_Ratio"],
            "geography_level": "county",
            "operation": "comparison",
            "year_strategy": ("acs_county: default 2023; gov_county and gov_state: no year filter"),
        },
    )
    resolved = {
        table: {"state": {"value": "maryland", "values": ["maryland"]}} for table in contract.tables
    }
    gov_sql = "SELECT county, \"Debt_Ratio\" FROM mart_gov_county WHERE state = 'maryland'"
    gov_problems = semantic_sql_problems(gov_sql, question, contract, resolved, enforce_shape=False)
    assert not any("acs_county must constrain" in problem for problem in gov_problems)

    derived_entity_sql = (
        'SELECT county, "Debt_Ratio" FROM mart_gov_county '
        "WHERE state = 'maryland' AND county = 'somerset'"
    )
    exploratory_problems = semantic_sql_problems(
        derived_entity_sql, question, contract, resolved, enforce_shape=False
    )
    assert not any("not grounded" in problem for problem in exploratory_problems)

    acs_without_period = (
        "SELECT county, \"Below poverty\" FROM mart_acs_county WHERE state = 'maryland'"
    )
    acs_problems = semantic_sql_problems(
        acs_without_period, question, contract, resolved, enforce_shape=False
    )
    assert any("acs_county must constrain" in problem for problem in acs_problems)


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


def test_acs_validator_rejects_wrong_education_denominator():
    question = "How many people have a bachelor's degree?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_state"],
            "columns": ["Education >= Bachelor's"],
            "geography_level": "state",
            "operation": "aggregate",
        },
    )
    sql = (
        'SELECT SUM("Total population" * "Education >= Bachelor\'s" / 100.0) AS people '
        "FROM mart_acs_state WHERE Year = 2023"
    )
    problems = semantic_sql_problems(sql, question, contract, {})
    assert any("no matching count denominator" in problem for problem in problems)


def test_acs_validator_rejects_multiplying_marginal_percentages():
    question = "How many Hispanic people have a bachelor's degree?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_state"],
            "columns": ["Hispanic", "Education >= Bachelor's"],
            "geography_level": "state",
            "operation": "aggregate",
        },
    )
    sql = (
        'SELECT SUM("Total population" * "Hispanic" * '
        '"Education >= Bachelor\'s" / 10000.0) AS people '
        "FROM mart_acs_state WHERE Year = 2023"
    )
    problems = semantic_sql_problems(sql, question, contract, {})
    assert any("marginal estimates" in problem for problem in problems)


def test_acs_validator_allows_total_population_share_count():
    question = "How many Hispanic people are in each state?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_state"],
            "columns": ["Hispanic"],
            "geography_level": "state",
            "operation": "breakdown",
        },
    )
    sql = (
        'SELECT state, "Total population" * "Hispanic" / 100.0 AS people '
        "FROM mart_acs_state WHERE Year = 2023"
    )
    assert semantic_sql_problems(sql, question, contract, {}) == []


def test_typed_acs_person_count_requires_documented_denominator_formula():
    question = "where is the maximum asian population by count"
    base_plan = {
        "operation": "ranking",
        "statistic": "derived",
        "result_unit": "persons",
        "formula": {
            "operator": "multiply",
            "operands": ["Total population", "Asian"],
            "scale": 0.01,
            "output_label": "Asian population count",
        },
        "observation_grain": "state",
        "result_scope": "single",
        "sort_direction": "desc",
        "top_k": 1,
        "flow_direction": "none",
        "include_component_measures": False,
        "output_dimensions": ["state"],
    }
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_state"],
            "columns": ["Total population", "Asian"],
            "geography_level": "state",
            "semantic_plan": base_plan,
        },
    )
    invalid = (
        'SELECT state, "Asian" AS people FROM mart_acs_state '
        "WHERE Year = 2023 ORDER BY people DESC LIMIT 1"
    )
    valid = (
        'SELECT state, "Total population" * "Asian" / 100.0 AS people '
        "FROM mart_acs_state WHERE Year = 2023 ORDER BY people DESC LIMIT 1"
    )
    assert any(
        "required metric" in problem or "formula" in problem
        for problem in semantic_sql_problems(invalid, question, contract, {})
    )
    assert semantic_sql_problems(valid, question, contract, {}) == []


def test_flow_validator_requires_named_inflow_geography_on_destination_side():
    question = "How much subaward funding flows into Maryland?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["state_flow"],
            "columns": ["subaward_amount_year"],
            "geography_level": "state",
            "operation": "aggregate",
            "flow_direction": "inflow",
        },
    )
    resolved = {
        "state_flow": {
            "rcpt_state_name": {"value": "Maryland", "values": ["Maryland"]},
            "subawardee_state_name": {"value": "Maryland", "values": ["Maryland"]},
        }
    }
    wrong = (
        "SELECT subawardee_state_name, SUM(subaward_amount_year) AS inflow "
        "FROM mart_state_flow WHERE LOWER(rcpt_state_name) = 'maryland' "
        "GROUP BY subawardee_state_name"
    )
    problems = semantic_sql_problems(wrong, question, contract, resolved)
    assert any("destination side" in problem for problem in problems)

    right = (
        "SELECT SUM(subaward_amount_year) AS inflow FROM mart_state_flow "
        "WHERE LOWER(subawardee_state_name) = 'maryland'"
    )
    assert semantic_sql_problems(right, question, contract, resolved) == []


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
    sql = """
        SELECT CORR(a.grant, b."Poverty rate") AS correlation,
               COUNT(*) FILTER (WHERE a.grant IS NOT NULL AND b."Poverty rate" IS NOT NULL) AS sample_size
        FROM mart_contract_state a
        JOIN mart_acs_state b ON LOWER(a.state) = LOWER(b.state)
        WHERE a.year = '2024' AND b."Year" = 2023
    """
    assert (
        semantic_sql_problems(
            sql, "What is the correlation between state grants and poverty?", contract, {}
        )
        == []
    )
    normalized = normalize_generated_sql(
        sql.replace("AS correlation", "AS grant_poverty_correlation"), contract
    )
    assert "AS correlation" in normalized
    assert "grant_poverty_correlation" not in normalized


def test_correlation_matrix_requires_pair_counts_or_complete_cases():
    question = (
        "Within Colorado, correlate high school and bachelor's attainment "
        "with median household income."
    )
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_county"],
            "columns": [
                "Education >= High School",
                "Education >= Bachelor's",
                "Median household income",
            ],
            "geography_level": "county",
            "operation": "correlation",
        },
    )
    resolved = {"acs_county": {"state": {"value": "colorado", "values": ["colorado"]}}}
    select = """
        WITH base AS (
          SELECT "Education >= High School" AS hs,
                 "Education >= Bachelor's" AS bachelors,
                 "Median household income" AS income
          FROM mart_acs_county
          WHERE LOWER(state) = 'colorado' AND Year = 2023 {complete_cases}
        )
        SELECT CORR(hs, income) AS hs_income,
               CORR(bachelors, income) AS bachelors_income,
               COUNT(*) FILTER (WHERE hs IS NOT NULL AND income IS NOT NULL) AS sample_size
        FROM base
    """
    bad = select.format(complete_cases="")
    good = select.format(
        complete_cases=(
            'AND "Education >= High School" IS NOT NULL '
            'AND "Education >= Bachelor\'s" IS NOT NULL '
            'AND "Median household income" IS NOT NULL'
        )
    )
    good_with_alias_filter = """
        WITH base AS (
          SELECT "Education >= High School" AS hs,
                 "Education >= Bachelor's" AS bachelors,
                 "Median household income" AS income
          FROM mart_acs_county
          WHERE LOWER(state) = 'colorado' AND Year = 2023
        ), complete_cases AS (
          SELECT * FROM base
          WHERE hs IS NOT NULL AND bachelors IS NOT NULL AND income IS NOT NULL
        )
        SELECT CORR(hs, income) AS hs_income,
               CORR(bachelors, income) AS bachelors_income,
               COUNT(*) AS sample_size
        FROM complete_cases
    """
    assert any(
        "correlation matrix" in problem
        for problem in semantic_sql_problems(bad, question, contract, resolved)
    )
    assert semantic_sql_problems(good, question, contract, resolved) == []
    assert semantic_sql_problems(good_with_alias_filter, question, contract, resolved) == []


def test_malformed_sql_normalization_is_repaired_not_raised():
    question = "Within Colorado, correlate bachelor's attainment with income."
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_county"],
            "columns": ["Education >= Bachelor's", "Median household income"],
            "geography_level": "county",
            "operation": "correlation",
        },
    )
    bad = r"""SELECT 'Bachelor\'s vs income' AS comparison,
        CORR("Education >= Bachelor's", "Median household income") AS correlation,
        COUNT(*) AS sample_size
        FROM mart_acs_county WHERE LOWER(state) = 'colorado' AND Year = 2023"""
    good = """SELECT
        CORR("Education >= Bachelor's", "Median household income") AS correlation,
        COUNT(*) FILTER (
          WHERE "Education >= Bachelor's" IS NOT NULL
            AND "Median household income" IS NOT NULL
        ) AS sample_size
        FROM mart_acs_county WHERE LOWER(state) = 'colorado' AND Year = 2023"""
    responses = iter((bad, good))

    def stub(messages, json_mode, purpose):
        assert purpose == "stage4_sql"
        return json.dumps({"sql": next(responses), "explanation": "test"})

    client.set_stub(stub)
    try:
        result = generate_and_execute(
            question,
            "grounding",
            tables=["acs_county"],
            contract=contract,
            resolved={"acs_county": {"state": {"value": "colorado", "values": ["colorado"]}}},
        )
    finally:
        client.clear_stub()
    assert result["error"] is None
    assert len(result["attempts"]) == 2
    assert result["attempts"][0]["error"].startswith("validation:")
    assert result["rows"][0]["sample_size"] == 64


def test_wrong_executed_result_shape_is_repaired_before_answering() -> None:
    question = "How many unique counties are in the ACS county dataset in 2023?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_county"],
            "columns": [],
            "geography_level": "county",
            "year_strategy": "acs_county: 2023",
            "semantic_plan": {
                "operation": "aggregate",
                "statistic": "count_distinct",
                "result_unit": "count",
                "observation_grain": "county",
                "result_scope": "single",
                "output_dimensions": [],
            },
        },
    )
    bad = """
        SELECT COUNT(DISTINCT fips) AS unique_counties
        FROM mart_acs_county WHERE Year = 2023
        UNION ALL
        SELECT COUNT(DISTINCT fips) AS unique_counties
        FROM mart_acs_county WHERE Year = 2023
    """
    good = """
        SELECT COUNT(DISTINCT fips) AS unique_counties
        FROM mart_acs_county WHERE Year = 2023
    """
    responses = iter((bad, good))

    def stub(messages, json_mode, purpose):
        assert purpose == "stage4_sql"
        return json.dumps({"sql": next(responses), "explanation": "test"})

    client.set_stub(stub)
    try:
        result = generate_and_execute(
            question,
            "grounding",
            tables=["acs_county"],
            contract=contract,
            resolved={},
        )
    finally:
        client.clear_stub()
    assert result["error"] is None
    assert len(result["attempts"]) == 2
    assert result["attempts"][0]["error"].startswith("validation: result shape:")
    assert result["rows"] == [{"unique_counties": 3222}]


def test_cross_dataset_join_rejects_incompatible_default_year_equality():
    question = "Compare Maryland and Virginia on financial literacy, poverty rate, and debt ratio."
    contract = build_analysis_contract(
        question,
        {
            "tables": ["finra_state", "acs_state", "gov_state"],
            "columns": ["financial_literacy", "Below poverty", "Debt_Ratio"],
            "geography_level": "state",
            "operation": "comparison",
        },
    )
    invalid = """
        SELECT f.state, f.financial_literacy, a."Below poverty", g.Debt_Ratio
        FROM mart_finra_state f
        JOIN mart_acs_state a
          ON LOWER(f.state) = LOWER(a.state) AND f.Year = a.Year
        JOIN mart_gov_state g ON LOWER(f.state) = LOWER(g.state)
        WHERE LOWER(f.state) IN ('maryland', 'virginia')
          AND f.Year = 2021 AND a.Year = 2023
    """
    valid = invalid.replace(" AND f.Year = a.Year", "")
    assert any(
        "required catalog periods differ" in problem
        for problem in semantic_sql_problems(invalid, question, contract, {})
    )
    assert semantic_sql_problems(valid, question, contract, {}) == []


def test_explicit_mixed_period_cross_dataset_join_passes() -> None:
    question = (
        "Across states, what is the correlation between 2023 median household "
        "income and 2021 financial literacy?"
    )
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_state", "finra_state"],
            "columns": ["Median household income", "financial_literacy"],
            "geography_level": "state",
            "operation": "correlation",
            "year_strategy": "acs_state: Year=2023; finra_state: Year=2021",
        },
    )
    sql = """
        SELECT CORR(a."Median household income", f.financial_literacy) AS correlation,
               COUNT(*) FILTER (WHERE a."Median household income" IS NOT NULL
                                  AND f.financial_literacy IS NOT NULL) AS sample_size
        FROM mart_acs_state a
        JOIN mart_finra_state f ON LOWER(a.state) = LOWER(f.state)
        WHERE a.Year = 2023 AND f.Year = 2021
    """
    assert semantic_sql_problems(sql, question, contract, {}) == []


def test_congress_contract_flow_join_requires_canonical_numeric_key() -> None:
    question = "Compare 2024 federal contracts and Sub-contract Out across districts."
    contract = build_analysis_contract(
        question,
        {
            "tables": ["contract_congress", "congress_flow"],
            "columns": ["Contracts", "subaward_amount"],
            "geography_level": "congress",
            "operation": "comparison",
            "flow_direction": "outflow",
            "year_strategy": "contract_congress: 2024; congress_flow: 2024",
        },
    )
    invalid = """
        SELECT c.cd_118, c."Contracts", SUM(f.subaward_amount) AS outflow
        FROM mart_contract_congress c
        JOIN mart_congress_flow f ON c.cd_118 = f.rcpt_cd_name
        WHERE c.year = '2024' AND f.act_dt_fis_yr = 2024
        GROUP BY c.cd_118, c."Contracts"
    """
    valid = """
        SELECT c.cd_118, c."Contracts", COALESCE(SUM(f.subaward_amount), 0) AS outflow
        FROM mart_contract_congress c
        LEFT JOIN mart_congress_flow f
          ON CAST(CONCAT(CAST(c.state_fips AS INTEGER), RIGHT(c.cd_118, 2)) AS INTEGER)
             = f.prime_awardee_stcd118
         AND f.act_dt_fis_yr = 2024
        WHERE c.year = '2024'
        GROUP BY c.cd_118, c."Contracts"
    """
    assert any(
        "canonicalize" in problem
        for problem in semantic_sql_problems(invalid, question, contract, {})
    )
    assert semantic_sql_problems(valid, question, contract, {}) == []


def test_contract_flow_ratio_coalesces_zero_event_counties() -> None:
    question = (
        "Express Sub-contract Out as a percentage of Federal Contracts for all "
        "counties of Nevada in 2024."
    )
    contract = build_analysis_contract(
        question,
        {
            "tables": ["contract_county", "county_flow"],
            "columns": ["Contracts", "subaward_amount"],
            "geography_level": "county",
            "operation": "breakdown",
            "flow_direction": "outflow",
            "year_strategy": "contract_county: 2024; county_flow: 2024",
            "semantic_plan": {
                "operation": "breakdown",
                "statistic": "derived",
                "formula": {
                    "operator": "divide",
                    "operands": ["subaward_amount", "Contracts"],
                    "scale": 100.0,
                    "output_label": "sub_out_pct",
                },
                "observation_grain": "county",
                "result_scope": "full",
                "sort_direction": "none",
                "top_k": None,
                "flow_direction": "outflow",
                "include_component_measures": False,
                "output_dimensions": ["state", "county"],
            },
        },
    )
    base = """
        SELECT c.state, c.county, c."Contracts",
               {flow} AS sub_contract_out,
               100.0 * {flow} / NULLIF(c."Contracts", 0) AS pct
        FROM mart_contract_county c
        LEFT JOIN mart_county_flow f
          ON c.county_fips = f.rcpt_cty AND f.act_dt_fis_yr = 2024
        WHERE c.year = '2024' AND LOWER(c.state) = 'nevada'
        GROUP BY c.state, c.county, c."Contracts"
    """
    invalid = base.format(flow="SUM(f.subaward_amount)")
    valid_with_components = base.format(flow="COALESCE(SUM(f.subaward_amount), 0)")
    valid = """
        WITH ratios AS (
            SELECT c.state, c.county,
                   100.0 * COALESCE(SUM(f.subaward_amount), 0)
                     / NULLIF(c."Contracts", 0) AS sub_out_pct
            FROM mart_contract_county c
            LEFT JOIN mart_county_flow f
              ON c.county_fips = f.rcpt_cty AND f.act_dt_fis_yr = 2024
            WHERE c.year = '2024' AND LOWER(c.state) = 'nevada'
            GROUP BY c.state, c.county, c."Contracts"
        )
        SELECT state, county, sub_out_pct FROM ratios
    """
    assert any(
        "COALESCE" in problem for problem in semantic_sql_problems(invalid, question, contract, {})
    )
    assert any(
        "component output" in problem
        for problem in semantic_sql_problems(valid_with_components, question, contract, {})
    )
    assert semantic_sql_problems(valid, question, contract, {}) == []
    with_unrequested_id = valid.replace(
        "SELECT state, county, sub_out_pct FROM ratios",
        "SELECT state, county, 32001 AS county_fips, sub_out_pct FROM ratios",
    )
    assert any(
        "county_fips" in problem
        for problem in semantic_sql_problems(with_unrequested_id, question, contract, {})
    )

    duplicated_denominator = """
        SELECT c.state, c.county,
               100.0 * COALESCE(SUM(f.subaward_amount), 0)
                 / NULLIF(SUM(c."Contracts"), 0) AS sub_out_pct
        FROM mart_contract_county c
        LEFT JOIN mart_county_flow f
          ON c.county_fips = f.rcpt_cty AND f.act_dt_fis_yr = 2024
        WHERE c.year = '2024' AND LOWER(c.state) = 'nevada'
        GROUP BY c.state, c.county
    """
    assert any(
        "denominator is duplicated" in problem
        for problem in semantic_sql_problems(duplicated_denominator, question, contract, {})
    )

    wrong_flow_side = valid.replace("f.rcpt_cty", "f.subawardee_cty")
    assert any(
        "opposite flow direction" in problem
        for problem in semantic_sql_problems(wrong_flow_side, question, contract, {})
    )


def test_contract_flow_correlation_includes_zero_event_geographies_and_sample_size() -> None:
    question = (
        "Across congressional districts, what is the correlation between 2024 "
        "federal contracts and 2024 Sub-contract Out?"
    )
    contract = build_analysis_contract(
        question,
        {
            "tables": ["contract_congress", "congress_flow"],
            "columns": ["Contracts", "subaward_amount"],
            "geography_level": "congress",
            "operation": "correlation",
            "flow_direction": "outflow",
            "year_strategy": "contract_congress: 2024; congress_flow: 2024",
        },
    )
    valid = """
        WITH outflow AS (
          SELECT prime_awardee_stcd118 AS code, SUM(subaward_amount) AS amount
          FROM mart_congress_flow WHERE act_dt_fis_yr = 2024 GROUP BY 1
        ), paired AS (
          SELECT c."Contracts" AS contracts, COALESCE(o.amount, 0) AS outflow
          FROM mart_contract_congress c LEFT JOIN outflow o
            ON CAST(CONCAT(CAST(c.state_fips AS INTEGER), RIGHT(c.cd_118, 2)) AS INTEGER) = o.code
          WHERE c.year = '2024'
        )
        SELECT CORR(contracts, outflow) AS correlation,
               COUNT(*) FILTER (WHERE contracts IS NOT NULL AND outflow IS NOT NULL) AS sample_size
        FROM paired
    """
    assert semantic_sql_problems(valid, question, contract, {}) == []
    without_zeroes = valid.replace("COALESCE(o.amount, 0)", "o.amount")
    assert any(
        "COALESCE" in problem
        for problem in semantic_sql_problems(without_zeroes, question, contract, {})
    )


def test_reasoning_inspection_query_is_not_forced_to_include_final_flow_side() -> None:
    question = "Rank congressional districts by subaward inflow per resident."
    contract = build_analysis_contract(
        question,
        {
            "tables": ["congress_flow", "acs_congress"],
            "columns": ["subaward_amount", "Total population"],
            "geography_level": "congress",
            "operation": "ranking",
            "flow_direction": "inflow",
            "year_strategy": "congress_flow: 2024; acs_congress: 2023",
        },
    )
    inspection = (
        'SELECT cd_118, "Total population" FROM mart_acs_congress WHERE Year = 2023 LIMIT 5'
    )

    assert semantic_sql_problems(inspection, question, contract, {}, enforce_shape=False) == []
    assert any(
        "subawardee side" in problem
        for problem in semantic_sql_problems(inspection, question, contract, {}, enforce_shape=True)
    )


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
    wrong = "SELECT SUM(\"Grants\") FROM mart_contract_state WHERE state='TEXAS' AND year='2024'"
    right = wrong.replace("year='2024'", "year='2020-2024'")
    assert contract.requested_period == "2020-2024"
    assert any(
        "2020-2024" in p
        for p in semantic_sql_problems(
            wrong, "total grants to Texas over the 2020-2024 period", contract, resolved
        )
    )
    assert (
        semantic_sql_problems(
            right, "total grants to Texas over the 2020-2024 period", contract, resolved
        )
        == []
    )


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


def test_implicit_superlative_gets_bounded_result_shape():
    single = build_analysis_contract(
        "which California county carries the most bonds",
        {
            "tables": ["gov_county"],
            "columns": ["Bonds,_Loans_&_Notes"],
            "operation": "ranking",
            "top_k": 1,
        },
    )
    plural = build_analysis_contract(
        "most financially literate congressional districts",
        {
            "tables": ["finra_congress"],
            "columns": ["financial_literacy"],
            "operation": "ranking",
            "top_k": 10,
        },
    )
    assert single.top_k == 1
    assert plural.top_k == 10


def test_top_one_ranking_has_one_canonical_scope() -> None:
    """Provider wording must not make identical top-one plans look different."""
    base = {
        "tables": ["gov_county"],
        "columns": ["Net_Position"],
        "geography_level": "county",
        "operation": "ranking",
        "sort_direction": "asc",
        "top_k": 1,
    }
    scopes = []
    for proposed_scope in ("single", "top_n", "unspecified"):
        contract = build_analysis_contract(
            "Which county is poorest by net position?",
            {
                **base,
                "semantic_plan": {
                    "operation": "ranking",
                    "statistic": "value",
                    "result_unit": "usd",
                    "formula": {"operator": "identity", "operands": ["Net_Position"]},
                    "observation_grain": "county",
                    "result_scope": proposed_scope,
                    "sort_direction": "asc",
                    "top_k": 1,
                    "output_dimensions": ["state", "county"],
                },
            },
        )
        scopes.append(contract.result_scope)
    assert scopes == ["single", "single", "single"]


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
        "SELECT state, \"Grants\" FROM mart_contract_state WHERE year='2024' "
        'ORDER BY "Grants" DESC LIMIT 10'
    )
    problems = semantic_sql_problems(wrong, "Bottom 5 states by grants", contract, {})
    assert any("LIMIT" in problem for problem in problems)
    assert any("tie-breaker" in problem for problem in problems)
    assert any("sort direction" in problem for problem in problems)


@pytest.mark.parametrize(
    ("question", "proposed", "expected"),
    [
        ("which county is poorest by net position", "asc", "asc"),
        ("which state has the highest negative assets minus liabilities", "asc", "asc"),
        ("which county has the largest liabilities minus assets", "desc", "desc"),
        ("which state has the highest positive assets minus liabilities", "desc", "desc"),
        ("rank states from poorest to least poor", "asc", "desc"),
    ],
)
def test_polarity_rankings_preserve_the_audited_semantic_direction(question, proposed, expected):
    contract = build_analysis_contract(
        question,
        {
            "tables": ["gov_county" if "county" in question else "gov_state"],
            "columns": ["Total_Assets", "Total_Liabilities"],
            "geography_level": "county" if "county" in question else "state",
            "operation": "ranking",
            "sort_direction": proposed,
            "top_k": 1,
        },
    )
    assert contract.sort_direction == expected


def test_unqualified_assets_liabilities_difference_uses_net_position() -> None:
    question = (
        "Which state has the highest negative difference in total assets and total liabilities?"
    )
    contract = build_analysis_contract(
        question,
        {
            "tables": ["gov_state"],
            "columns": ["Total_Assets", "Total_Liabilities"],
            "geography_level": "state",
            "operation": "ranking",
            "sort_direction": "asc",
            "top_k": 1,
        },
    )
    assert contract.metric_columns == ["Net_Position"]
    right = "SELECT state, Net_Position FROM mart_gov_state ORDER BY Net_Position ASC LIMIT 1"
    reversed_gap = (
        "SELECT state, Total_Liabilities - Total_Assets AS gap "
        "FROM mart_gov_state ORDER BY gap ASC LIMIT 1"
    )
    assert semantic_sql_problems(right, question, contract, {}) == []
    problems = semantic_sql_problems(reversed_gap, question, contract, {})
    assert any("does not use required metric" in problem for problem in problems)


def test_explicit_rank_scope_rejects_silent_top_ten_limit() -> None:
    question = "Rank the Wyoming counties by Black and Asian population."
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_county"],
            "columns": ["Black", "Asian"],
            "geography_level": "county",
            "operation": "ranking",
            "sort_direction": "desc",
            "top_k": None,
            "semantic_plan": {
                "operation": "ranking",
                "statistic": "value",
                "result_unit": "percent",
                "formula": {"operator": "none", "operands": []},
                "observation_grain": "county",
                "result_scope": "full",
                "sort_direction": "desc",
                "sort_columns": ["Black", "Asian"],
                "top_k": None,
                "output_dimensions": ["county", "state"],
            },
        },
    )
    base = (
        'SELECT state, county, "Black", "Asian" FROM mart_acs_county '
        "WHERE LOWER(state) = 'wyoming' AND Year = 2023 "
        'ORDER BY "Black" DESC, "Asian" DESC, county ASC'
    )
    assert semantic_sql_problems(base, question, contract, {}) == []
    problems = semantic_sql_problems(base + " LIMIT 10", question, contract, {})
    assert any("full ranking scope" in problem for problem in problems)

    tied = base.rsplit(", county ASC", 1)[0]
    problems = semantic_sql_problems(tied, question, contract, {})
    assert any("tie-breaker" in problem for problem in problems)


def test_typed_county_results_require_state_companion_identity() -> None:
    question = "Which county has the highest debt ratio?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["gov_county"],
            "columns": ["Debt_Ratio"],
            "geography_level": "county",
            "semantic_plan": {
                "operation": "ranking",
                "statistic": "value",
                "result_unit": "ratio",
                "formula": {"operator": "identity", "operands": ["Debt_Ratio"]},
                "observation_grain": "county",
                "result_scope": "single",
                "sort_direction": "desc",
                "top_k": 1,
                "flow_direction": "none",
                "include_component_measures": False,
                "output_dimensions": ["county", "state"],
            },
        },
    )
    incomplete = "SELECT county, Debt_Ratio FROM mart_gov_county ORDER BY Debt_Ratio DESC LIMIT 1"
    complete = (
        "SELECT state, county, Debt_Ratio FROM mart_gov_county ORDER BY Debt_Ratio DESC LIMIT 1"
    )
    assert any(
        "state" in problem for problem in semantic_sql_problems(incomplete, question, contract, {})
    )
    assert semantic_sql_problems(complete, question, contract, {}) == []


def test_typed_measure_predicate_is_required_in_sql() -> None:
    question = "Which counties have a higher renter population than owner population?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_county"],
            "columns": ["Renter occupied", "Owner occupied"],
            "geography_level": "county",
            "semantic_plan": {
                "operation": "comparison",
                "statistic": "derived",
                "result_unit": "percent",
                "formula": {
                    "operator": "subtract",
                    "operands": ["Renter occupied", "Owner occupied"],
                    "scale": 1.0,
                },
                "predicate": {
                    "operator": "gt",
                    "operands": ["Renter occupied", "Owner occupied"],
                },
                "observation_grain": "county",
                "result_scope": "full",
                "sort_direction": "none",
                "top_k": None,
                "flow_direction": "none",
                "include_component_measures": False,
                "output_dimensions": ["state", "county"],
            },
        },
    )
    base = (
        'SELECT state, county, "Renter occupied" - "Owner occupied" AS gap '
        "FROM mart_acs_county WHERE Year = 2023{predicate}"
    )
    assert any(
        "row predicate" in problem
        for problem in semantic_sql_problems(base.format(predicate=""), question, contract, {})
    )
    assert (
        semantic_sql_problems(
            base.format(predicate=' AND "Renter occupied" > "Owner occupied"'),
            question,
            contract,
            {},
        )
        == []
    )


def test_typed_literal_predicate_is_required_in_sql() -> None:
    question = "Which states have poverty above 20 percent?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_state"],
            "columns": ["Below poverty"],
            "geography_level": "state",
            "semantic_plan": {
                "operation": "comparison",
                "statistic": "value",
                "result_unit": "percent",
                "predicate": {
                    "operator": "gt",
                    "operands": ["Below poverty"],
                    "comparison_value": 20,
                },
                "observation_grain": "state",
                "result_scope": "full",
                "output_dimensions": ["state"],
            },
        },
    )
    missing = 'SELECT state, "Below poverty" FROM mart_acs_state WHERE Year = 2023'
    present = missing + ' AND "Below poverty" > 20'

    assert any(
        "row predicate" in problem
        for problem in semantic_sql_problems(missing, question, contract, {})
    )
    assert semantic_sql_problems(present, question, contract, {}) == []


def test_typed_entity_predicate_accepts_grounded_canonical_value() -> None:
    question = "How many grant dollars did Maryland receive in 2024?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["contract_state"],
            "columns": ["Grants"],
            "geography_level": "state",
            "semantic_plan": {
                "operation": "lookup",
                "statistic": "value",
                "result_unit": "usd",
                "formula": {"operator": "identity", "operands": ["Grants"]},
                "predicate": {
                    "operator": "eq",
                    "operands": ["state"],
                    "comparison_value": "Maryland",
                },
                "observation_grain": "state",
                "result_scope": "single",
                "output_dimensions": ["state"],
            },
        },
    )
    sql = (
        'SELECT state, "Grants" AS grant_dollars FROM mart_contract_state '
        "WHERE state = 'MARYLAND' AND year = '2024'"
    )
    resolved = {
        "contract_state": {
            "state": {"value": "MARYLAND", "values": ["MARYLAND"]},
        }
    }

    assert semantic_sql_problems(sql, question, contract, resolved) == []


def test_typed_entity_predicate_rejects_unrelated_grounded_value() -> None:
    question = "How many grant dollars did Maryland receive in 2024?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["contract_state"],
            "columns": ["Grants"],
            "geography_level": "state",
            "semantic_plan": {
                "operation": "lookup",
                "statistic": "value",
                "result_unit": "usd",
                "formula": {"operator": "identity", "operands": ["Grants"]},
                "predicate": {
                    "operator": "eq",
                    "operands": ["state"],
                    "comparison_value": "Maryland",
                },
                "observation_grain": "state",
                "result_scope": "single",
                "output_dimensions": ["state"],
            },
        },
    )
    wrong_sql = (
        'SELECT state, "Grants" AS grant_dollars FROM mart_contract_state '
        "WHERE state = 'VIRGINIA' AND year = '2024'"
    )
    resolved = {
        "contract_state": {
            "state": {"value": "MARYLAND", "values": ["MARYLAND"]},
        }
    }

    assert any(
        "row predicate" in problem
        for problem in semantic_sql_problems(wrong_sql, question, contract, resolved)
    )


def test_identity_formula_does_not_erase_companion_measure() -> None:
    contract = build_analysis_contract(
        "Rank county outflow and show liabilities beside it.",
        {
            "tables": ["county_flow", "gov_county"],
            "columns": ["subaward_amount", "Total_Liabilities_per_capita"],
            "geography_level": "county",
            "semantic_plan": {
                "operation": "ranking",
                "statistic": "sum",
                "result_unit": "usd",
                "formula": {
                    "operator": "identity",
                    "operands": ["subaward_amount"],
                },
                "observation_grain": "county",
                "result_scope": "full",
                "sort_direction": "desc",
                "sort_columns": ["subaward_amount"],
                "include_component_measures": True,
                "output_dimensions": ["rcpt_cty_name", "rcpt_state"],
            },
        },
    )

    assert contract.metric_columns == ["subaward_amount", "Total_Liabilities_per_capita"]


def test_flow_state_output_uses_directional_planned_dimension() -> None:
    question = "Compare Maryland and Virginia subcontract outflow."
    contract = build_analysis_contract(
        question,
        {
            "tables": ["state_flow"],
            "columns": ["subaward_amount_year"],
            "geography_level": "state",
            "semantic_plan": {
                "operation": "comparison",
                "statistic": "sum",
                "result_unit": "usd",
                "formula": {"operator": "none", "operands": []},
                "predicate": {"operator": "none", "operands": []},
                "observation_grain": "state",
                "result_scope": "grouped",
                "sort_direction": "none",
                "top_k": None,
                "flow_direction": "outflow",
                "include_component_measures": False,
                "output_dimensions": ["rcpt_state_name"],
            },
        },
    )
    sql = (
        "SELECT rcpt_state_name, SUM(subaward_amount_year) AS outflow "
        "FROM mart_state_flow WHERE rcpt_state_name IN ('Maryland', 'Virginia') "
        "GROUP BY rcpt_state_name"
    )
    resolved = {
        "state_flow": {
            "rcpt_state_name": {
                "value": "Maryland",
                "values": ["Maryland", "Virginia"],
            }
        }
    }
    assert semantic_sql_problems(sql, question, contract, resolved) == []


def test_flow_ranking_without_output_plan_accepts_readable_congress_label() -> None:
    question = "Which congressional district receives the most subaward inflow?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["congress_flow"],
            "columns": ["subaward_amount"],
            "geography_level": "congress",
            "operation": "ranking",
            "flow_direction": "inflow",
            "sort_direction": "desc",
            "top_k": 1,
            "year_strategy": "2024",
        },
    )
    readable = (
        "SELECT subawardee_cd_name, SUM(subaward_amount) AS subaward_inflow "
        "FROM mart_congress_flow WHERE act_dt_fis_yr = 2024 "
        "GROUP BY subawardee_cd_name ORDER BY subaward_inflow DESC LIMIT 1"
    )
    technical_id_only = readable.replace("subawardee_cd_name", "subawardee_stcd118")

    assert semantic_sql_problems(readable, question, contract, {}) == []
    assert any(
        "readable geography dimension" in problem
        for problem in semantic_sql_problems(technical_id_only, question, contract, {})
    )


def test_named_state_scope_is_required_on_congressional_results() -> None:
    question = "Maryland congressional districts by free cash flow"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["gov_congress"],
            "columns": ["Free_Cash_Flow"],
            "geography_level": "congress",
            "semantic_plan": {
                "operation": "ranking",
                "statistic": "value",
                "result_unit": "usd",
                "formula": {"operator": "identity", "operands": ["Free_Cash_Flow"]},
                "observation_grain": "congress",
                "result_scope": "full",
                "sort_direction": "desc",
                "output_dimensions": ["cd_118"],
            },
        },
    )
    resolved = {
        "gov_congress": {
            "cd_118": {
                "value": "MD-01",
                "values": ["MD-01", "MD-02", "MD-03", "MD-04"],
            }
        }
    }
    national = (
        "SELECT cd_118, Free_Cash_Flow FROM mart_gov_congress "
        "ORDER BY Free_Cash_Flow DESC, cd_118 ASC"
    )
    scoped = national.replace("ORDER BY", "WHERE cd_118 LIKE 'MD-%' ORDER BY")

    assert any(
        "named cd_118 scope" in problem
        for problem in semantic_sql_problems(national, question, contract, resolved)
    )
    assert semantic_sql_problems(scoped, question, contract, resolved) == []


def test_semantic_validator_rejects_predicate_across_incompatible_units() -> None:
    question = "Which counties have poverty above their debt ratio?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_county", "gov_county"],
            "columns": ["Below poverty", "Debt_Ratio"],
            "geography_level": "county",
            "semantic_plan": {
                "operation": "comparison",
                "statistic": "value",
                "result_unit": "value",
                "formula": {"operator": "none", "operands": []},
                "predicate": {
                    "operator": "gt",
                    "operands": ["Below poverty", "Debt_Ratio"],
                },
                "observation_grain": "county",
                "result_scope": "full",
                "output_dimensions": ["county", "state"],
            },
        },
    )
    sql = (
        'SELECT a.county, a.state, a."Below poverty", g.Debt_Ratio '
        "FROM mart_acs_county a JOIN mart_gov_county g ON a.fips = g.fips "
        'WHERE a.Year = 2023 AND a."Below poverty" > g.Debt_Ratio'
    )

    assert any(
        "incompatible units" in problem
        for problem in semantic_sql_problems(sql, question, contract, {})
    )


def test_reasoning_intermediate_sql_need_not_implement_the_final_formula() -> None:
    question = "What is the gap between the highest and lowest county Grants?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["contract_county"],
            "columns": ["Grants"],
            "geography_level": "county",
            "semantic_plan": {
                "operation": "comparison",
                "statistic": "derived",
                "result_unit": "usd",
                "formula": {
                    "operator": "subtract",
                    "operands": ["Grants", "Grants"],
                },
                "observation_grain": "county",
                "result_scope": "single",
            },
        },
    )
    intermediate_sql = (
        'SELECT county, "Grants" FROM mart_contract_county '
        "WHERE year = '2024' ORDER BY \"Grants\" DESC LIMIT 1"
    )

    strict = semantic_sql_problems(intermediate_sql, question, contract, {})
    exploratory = semantic_sql_problems(
        intermediate_sql,
        question,
        contract,
        {},
        enforce_shape=False,
    )
    assert any("semantic plan formula" in problem for problem in strict)
    assert not any("semantic plan formula" in problem for problem in exploratory)


def test_reasoning_peer_query_may_temporarily_expand_beyond_named_entity() -> None:
    question = "How does Texas Grants compare to its peers?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["contract_state"],
            "columns": ["Grants"],
            "geography_level": "state",
            "semantic_plan": {
                "operation": "comparison",
                "statistic": "value",
                "result_unit": "usd",
                "formula": {"operator": "identity", "operands": ["Grants"]},
                "observation_grain": "state",
                "result_scope": "grouped",
                "output_dimensions": ["state"],
            },
        },
    )
    resolved = {
        "contract_state": {
            "state": {"value": "TEXAS", "values": ["TEXAS"]},
        }
    }
    peer_sql = (
        'SELECT state, "Grants" FROM mart_contract_state '
        "WHERE year = '2024' ORDER BY \"Grants\" DESC"
    )

    strict = semantic_sql_problems(peer_sql, question, contract, resolved)
    exploratory = semantic_sql_problems(
        peer_sql,
        question,
        contract,
        resolved,
        enforce_shape=False,
    )
    assert any("named state scope" in problem for problem in strict)
    assert not any("named state scope" in problem for problem in exploratory)


def test_ungrouped_correlation_plan_is_one_atomic_result() -> None:
    plan = semantic_plan_from_routing(
        {
            "operation": "correlation",
            "semantic_plan": {
                "operation": "correlation",
                "statistic": "correlation",
                "result_unit": "correlation",
                "observation_grain": "county",
                "result_scope": "grouped",
                "output_dimensions": [],
            },
        }
    )

    assert plan.result_scope == "single"


def test_explicitly_grouped_correlation_keeps_its_output_scope() -> None:
    plan = semantic_plan_from_routing(
        {
            "operation": "correlation",
            "semantic_plan": {
                "operation": "correlation",
                "statistic": "correlation",
                "result_unit": "correlation",
                "observation_grain": "county",
                "result_scope": "grouped",
                "output_dimensions": ["state"],
            },
        }
    )

    assert plan.result_scope == "grouped"


def test_single_top_rank_may_group_by_its_output_dimension() -> None:
    question = "which congressional district receives the most subaward inflow"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["congress_flow"],
            "columns": ["subaward_amount"],
            "geography_level": "congress",
            "year_strategy": "2024",
            "semantic_plan": {
                "operation": "ranking",
                "statistic": "sum",
                "result_unit": "usd",
                "observation_grain": "congress",
                "result_scope": "single",
                "sort_direction": "desc",
                "top_k": 1,
                "flow_direction": "inflow",
                "output_dimensions": ["subawardee_cd_name"],
            },
        },
    )
    sql = (
        "SELECT subawardee_cd_name, SUM(subaward_amount) AS total_inflow "
        "FROM mart_congress_flow WHERE act_dt_fis_yr = 2024 "
        "GROUP BY subawardee_cd_name "
        "ORDER BY total_inflow DESC, subawardee_cd_name LIMIT 1"
    )

    assert semantic_sql_problems(sql, question, contract, {}) == []


def test_correlation_statistic_canonicalizes_inconsistent_operation() -> None:
    contract = build_analysis_contract(
        "Are income and literacy correlated?",
        {
            "tables": ["acs_state", "finra_state"],
            "columns": ["Median household income", "financial_literacy"],
            "geography_level": "state",
            "semantic_plan": {
                "operation": "aggregate",
                "statistic": "correlation",
                "result_unit": "correlation",
                "observation_grain": "state",
                "result_scope": "single",
            },
        },
    )
    assert contract.operation == "correlation"


def test_typed_correlation_is_not_reparsed_as_median_aggregate() -> None:
    contract = build_analysis_contract(
        "Are wealthier states by median income generally more financially literate?",
        {
            "tables": ["acs_state", "finra_state"],
            "columns": ["Median household income", "financial_literacy"],
            "geography_level": "state",
            "semantic_plan": {
                "operation": "correlation",
                "statistic": "correlation",
                "result_unit": "correlation",
                "observation_grain": "state",
                "result_scope": "single",
            },
        },
    )
    assert contract.operation == "correlation"


def test_explicit_fy2023_government_snapshot_does_not_require_year_filter():
    question = "How much did Prince George County borrow in 2023?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["gov_county"],
            "columns": ["Bonds,_Loans_&_Notes"],
            "geography_level": "county",
            "operation": "lookup",
            "year_strategy": "gov_county: Fiscal Year 2023 snapshot",
        },
    )
    assert contract.period_by_table == {"gov_county": "catalog snapshot"}
    sql = (
        'SELECT state, county, "Bonds,_Loans_&_Notes" FROM mart_gov_county '
        "WHERE LOWER(county) = 'prince george'"
    )
    assert (
        semantic_sql_problems(
            sql,
            question,
            contract,
            {"gov_county": {"county": {"value": "prince george", "values": ["prince george"]}}},
        )
        == []
    )


def test_finra_share_cannot_be_expanded_to_resident_count_with_acs_population():
    question = "How many satisfied residents are in Prince George County?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["finra_county", "acs_county"],
            "columns": ["satisfied", "Total population"],
            "geography_level": "county",
            "operation": "aggregate",
            "year_strategy": "finra_county: 2021; acs_county: 2023",
        },
    )
    sql = (
        'SELECT f.satisfied * a."Total population" AS satisfied_residents '
        "FROM mart_finra_county f JOIN mart_acs_county a ON f.fips = a.fips "
        "WHERE f.Year = 2021 AND a.Year = 2023"
    )
    problems = semantic_sql_problems(sql, question, contract, {})
    assert any("FINRA shares and indices" in problem for problem in problems)


def test_explicit_liabilities_minus_assets_preserves_expression_sign():
    question = "Which New York county has the largest liabilities minus assets?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["gov_county"],
            "columns": ["Total_Liabilities", "Total_Assets"],
            "geography_level": "county",
            "operation": "ranking",
            "sort_direction": "desc",
            "top_k": 1,
        },
    )
    assert contract.sort_direction == "desc"
    wrong = (
        "SELECT state, county, Net_Position FROM mart_gov_county "
        "WHERE LOWER(state) = 'new york' ORDER BY Net_Position ASC LIMIT 1"
    )
    right = (
        "SELECT state, county, Total_Liabilities - Total_Assets AS gap "
        "FROM mart_gov_county WHERE LOWER(state) = 'new york' "
        "ORDER BY gap DESC LIMIT 1"
    )
    assert any(
        "semantic plan formula" in problem
        for problem in semantic_sql_problems(wrong, question, contract, {})
    )
    assert semantic_sql_problems(right, question, contract, {}) == []

    verifier_style_contract = build_analysis_contract(
        question,
        {
            "tables": ["gov_county"],
            "columns": ["Net_Position"],
            "geography_level": "county",
            "operation": "ranking",
            "sort_direction": "asc",
            "top_k": 1,
        },
    )
    assert verifier_style_contract.metric_columns == ["Total_Liabilities", "Total_Assets"]
    assert verifier_style_contract.sort_direction == "desc"
    assert semantic_sql_problems(right, question, verifier_style_contract, {}) == []


def test_typed_semantic_plan_overrides_conflicting_legacy_route_fields() -> None:
    """Downstream stages consume one meaning instead of re-reading prose."""
    contract = build_analysis_contract(
        "Show the New York county with the greatest fiscal shortfall.",
        {
            "tables": ["gov_county"],
            # Simulate a stale/incorrect outer route from an earlier planner.
            "columns": ["Net_Position"],
            "geography_level": "county",
            "operation": "comparison",
            "sort_direction": "asc",
            "top_k": 10,
            "semantic_plan": {
                "operation": "ranking",
                "statistic": "derived",
                "formula": {
                    "operator": "subtract",
                    "operands": ["Total_Liabilities", "Total_Assets"],
                    "scale": 1.0,
                    "output_label": "liabilities_minus_assets",
                },
                "observation_grain": "county",
                "result_scope": "single",
                "sort_direction": "desc",
                "top_k": 1,
                "flow_direction": "none",
                "include_component_measures": False,
                "output_dimensions": ["state", "county"],
            },
        },
    )
    assert contract.operation == "ranking"
    assert contract.metric_columns == ["Total_Liabilities", "Total_Assets"]
    assert contract.formula.operator == "subtract"
    assert contract.formula.operands == ["Total_Liabilities", "Total_Assets"]
    assert contract.sort_direction == "desc"
    assert contract.top_k == 1
    assert contract.result_scope == "single"


def test_typed_full_scope_is_paraphrase_independent() -> None:
    routing = {
        "tables": ["acs_county"],
        "columns": ["Black", "Asian"],
        "geography_level": "county",
        "operation": "ranking",
        "semantic_plan": {
            "operation": "ranking",
            "statistic": "value",
            "formula": {"operator": "none", "operands": []},
            "observation_grain": "county",
            "result_scope": "full",
            "sort_direction": "desc",
            "top_k": None,
            "flow_direction": "none",
            "include_component_measures": True,
            "output_dimensions": ["state", "county"],
        },
    }
    contracts = [
        build_analysis_contract(question, routing)
        for question in (
            "Rank the Wyoming counties by Black and Asian population.",
            "Put every Wyoming county in order using Black share, then Asian share.",
            "Give me the complete Wyoming county ordering for those two measures.",
        )
    ]
    assert all(contract.result_scope == "full" for contract in contracts)
    sql = (
        'SELECT state, county, "Black", "Asian" FROM mart_acs_county '
        "WHERE LOWER(state) = 'wyoming' AND Year = 2023 "
        'ORDER BY "Black" DESC, "Asian" DESC, county ASC LIMIT 10'
    )
    for question, contract in zip(
        (
            "Rank the Wyoming counties by Black and Asian population.",
            "Put every Wyoming county in order using Black share, then Asian share.",
            "Give me the complete Wyoming county ordering for those two measures.",
        ),
        contracts,
        strict=True,
    ):
        assert any(
            "full ranking scope" in problem
            for problem in semantic_sql_problems(sql, question, contract, {})
        )


def test_typed_plan_normalizes_internal_scope_operation_consistency() -> None:
    contract = build_analysis_contract(
        "For every Nevada county, what share of contracts goes out as subcontracts?",
        {
            "tables": ["contract_county", "county_flow"],
            "columns": ["Contracts", "subaward_amount"],
            "geography_level": "county",
            "operation": "lookup",
            "semantic_plan": {
                "operation": "lookup",
                "statistic": "derived",
                "formula": {
                    "operator": "divide",
                    "operands": ["subaward_amount", "Contracts"],
                    "scale": 100,
                },
                "observation_grain": "county",
                "result_scope": "full",
                "sort_direction": "none",
                "top_k": None,
                "flow_direction": "outflow",
                "include_component_measures": False,
                "output_dimensions": ["state", "county"],
            },
        },
    )
    assert contract.operation == "breakdown"
    assert contract.result_scope == "full"


def test_faithfulness_string_false_is_not_truthy():
    client.set_stub(
        lambda messages, json_mode, purpose: json.dumps({"faithful": "false", "reason": "wrong"})
    )
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


def test_typed_flow_plan_is_not_reinterpreted_by_a_second_regex_brain() -> None:
    base = {
        "tables": ["contract_county", "county_flow"],
        "columns": ["subaward_amount", "Contracts"],
        "geography_level": "county",
        "semantic_plan": {
            "operation": "ranking",
            "statistic": "derived",
            "result_unit": "percent",
            "formula": {
                "operator": "divide",
                "operands": ["subaward_amount", "Contracts"],
                "scale": 100,
            },
            "observation_grain": "county",
            "result_scope": "full",
            "sort_direction": "desc",
            "flow_direction": "inflow",
            "output_dimensions": ["state", "county"],
        },
    }
    contract = build_analysis_contract(
        "Express Sub-contract Out as a percentage of Federal Contracts for all counties of Nevada in 2024.",
        base,
    )
    assert contract.flow_direction == "inflow"


def test_focused_flow_direction_follows_the_bound_geography_side() -> None:
    contract = build_analysis_contract(
        "Which states receive the most subawards from Maryland?",
        {
            "tables": ["state_flow"],
            "columns": ["subaward_amount_year"],
            "filter_columns": ["rcpt_state_name"],
            "geography_level": "state",
            "semantic_plan": {
                "operation": "ranking",
                "statistic": "sum",
                "result_unit": "usd",
                "observation_grain": "state",
                "result_scope": "top_n",
                "sort_direction": "desc",
                "flow_direction": "inflow",
                "output_dimensions": ["subawardee_state_name"],
            },
        },
    )

    assert contract.flow_direction == "outflow"


def test_typed_multi_part_operation_is_not_overwritten_by_superlative_words() -> None:
    contract = build_analysis_contract(
        (
            "Which Maryland county has the highest poverty rate, and how does "
            "its debt ratio compare to the state median?"
        ),
        {
            "tables": ["acs_county", "gov_county"],
            "columns": ["Below poverty", "Debt_Ratio"],
            "geography_level": "county",
            "semantic_plan": {
                "operation": "comparison",
                "statistic": "value",
                "result_unit": "ratio",
                "observation_grain": "county",
                "result_scope": "single",
                "sort_direction": "desc",
                "top_k": 1,
                "output_dimensions": ["county", "state"],
            },
        },
    )
    assert contract.operation == "comparison"


def test_explicit_full_ranking_language_remains_a_ranking() -> None:
    contract = build_analysis_contract(
        "Rank all Wyoming counties by Black population.",
        {
            "tables": ["acs_county"],
            "columns": ["Black"],
            "geography_level": "county",
            "semantic_plan": {
                "operation": "ranking",
                "statistic": "value",
                "result_unit": "percent",
                "formula": {"operator": "identity", "operands": ["Black"]},
                "observation_grain": "county",
                "result_scope": "full",
                "sort_direction": "desc",
                "flow_direction": "none",
                "output_dimensions": ["state", "county"],
            },
        },
    )
    assert contract.operation == "ranking"
    assert contract.sort_direction == "desc"
    assert contract.top_k is None


def test_named_scalar_output_requires_planned_geography_projection() -> None:
    question = "How many grant dollars did Maryland receive?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["contract_state"],
            "columns": ["Grants"],
            "geography_level": "state",
            "semantic_plan": {
                "operation": "lookup",
                "statistic": "value",
                "result_unit": "usd",
                "formula": {"operator": "identity", "operands": ["Grants"]},
                "observation_grain": "state",
                "result_scope": "single",
                "output_dimensions": ["state"],
            },
        },
    )
    resolved = {
        "contract_state": {
            "state": {"value": "MARYLAND", "values": ["MARYLAND"]},
        }
    }
    scalar_only = (
        "SELECT \"Grants\" FROM mart_contract_state WHERE state='MARYLAND' AND year='2024'"
    )
    labeled = scalar_only.replace('SELECT "Grants"', 'SELECT state, "Grants"')
    assert any(
        "readable dimension" in problem
        for problem in semantic_sql_problems(scalar_only, question, contract, resolved)
    )
    assert semantic_sql_problems(labeled, question, contract, resolved) == []


def test_cross_table_flow_join_must_use_directional_key_in_join_condition() -> None:
    question = (
        "Express Sub-contract Out as a percentage of Federal Contracts for all "
        "counties of Nevada in 2024."
    )
    contract = build_analysis_contract(
        question,
        {
            "tables": ["contract_county", "county_flow"],
            "columns": ["subaward_amount", "Contracts"],
            "geography_level": "county",
            "year_strategy": "contract_county: 2024; county_flow: 2024",
            "semantic_plan": {
                "operation": "breakdown",
                "statistic": "derived",
                "result_unit": "percent",
                "formula": {
                    "operator": "divide",
                    "operands": ["subaward_amount", "Contracts"],
                    "scale": 100,
                },
                "observation_grain": "county",
                "result_scope": "full",
                "flow_direction": "outflow",
                "output_dimensions": ["state", "county"],
            },
        },
    )
    wrong = """
        WITH flows AS (
            SELECT rcpt_cty, subawardee_cty, SUM(subaward_amount) AS amount
            FROM mart_county_flow WHERE act_dt_fis_yr = 2024
            GROUP BY rcpt_cty, subawardee_cty
        )
        SELECT c.state, c.county,
               100.0 * COALESCE(SUM(f.amount), 0) / NULLIF(c."Contracts", 0) AS pct
        FROM mart_contract_county c
        LEFT JOIN flows f ON c.county_fips = f.subawardee_cty
        WHERE c.year = '2024' AND LOWER(c.state) = 'nevada'
        GROUP BY c.state, c.county, c."Contracts"
    """
    assert any(
        "outflow side" in problem
        for problem in semantic_sql_problems(wrong, question, contract, {})
    )


def test_single_correlation_cannot_group_by_each_observation() -> None:
    question = "Within Colorado, correlate bachelor's attainment with income."
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_county"],
            "columns": ["Education >= Bachelor's", "Median household income"],
            "geography_level": "county",
            "year_strategy": "acs_county: 2023",
            "semantic_plan": {
                "operation": "correlation",
                "statistic": "correlation",
                "result_unit": "correlation",
                "observation_grain": "county",
                "result_scope": "single",
                "output_dimensions": ["state"],
            },
        },
    )
    bad = """
        SELECT state, county,
               CORR("Education >= Bachelor's", "Median household income") AS correlation,
               COUNT(*) AS sample_size
        FROM mart_acs_county
        WHERE LOWER(state) = 'colorado' AND Year = 2023
        GROUP BY state, county
    """
    good = """
        SELECT 'Colorado' AS state,
               CORR("Education >= Bachelor's", "Median household income") AS correlation,
               COUNT(*) FILTER (
                 WHERE "Education >= Bachelor's" IS NOT NULL
                   AND "Median household income" IS NOT NULL
               ) AS sample_size
        FROM mart_acs_county
        WHERE LOWER(state) = 'colorado' AND Year = 2023
    """
    assert any(
        "single-result analysis" in problem
        for problem in semantic_sql_problems(bad, question, contract, {})
    )
    assert semantic_sql_problems(good, question, contract, {}) == []


def test_distinct_geography_count_requires_one_real_distinct_aggregate() -> None:
    question = "How many unique counties are in the ACS county dataset in 2023?"
    contract = build_analysis_contract(
        question,
        {
            "tables": ["acs_county"],
            "columns": [],
            "geography_level": "county",
            "year_strategy": "acs_county: 2023",
            "semantic_plan": {
                "operation": "aggregate",
                "statistic": "count_distinct",
                "result_unit": "count",
                "observation_grain": "county",
                "result_scope": "single",
                "output_dimensions": [],
            },
        },
    )
    grouped = """
        SELECT state, county, COUNT(*) AS n
        FROM mart_acs_county WHERE Year = 2023
        GROUP BY state, county
    """
    scalar = """
        SELECT COUNT(DISTINCT fips) AS unique_counties
        FROM mart_acs_county WHERE Year = 2023
    """
    grouped_problems = semantic_sql_problems(grouped, question, contract, {})
    assert any("COUNT(DISTINCT" in problem for problem in grouped_problems)
    assert any("single-result analysis" in problem for problem in grouped_problems)
    assert semantic_sql_problems(scalar, question, contract, {}) == []
    assert result_shape_problems([{"unique_counties": 3222}], contract) == []
    assert result_shape_problems([{"n": 1}, {"n": 1}], contract)
