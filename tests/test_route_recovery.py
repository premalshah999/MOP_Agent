from __future__ import annotations

import json

from app.core.plan_verifier import (
    _reconcile_with_schema,
    _schema_capable_cross_table_route,
    should_verify,
)
from app.core.planner import classify_and_route
from app.llm import client
from app.semantic.registry import semantic_catalog_for_verification


def _base_decision(**updates):
    value = {
        "intent": "UNANSWERABLE",
        "requires_sql": False,
        "needs_clarification": False,
        "clarification_question": "",
        "reason": "term not found",
        "tables": [],
        "metric_columns": [],
        "filter_columns": [],
        "geography_level": "state",
        "operation": "aggregate",
        "flow_direction": "none",
        "sort_direction": "none",
        "top_k": None,
        "year_strategy": "",
        "join_plan": "",
        "confidence": "high",
    }
    value.update(updates)
    return value


def test_simple_high_confidence_typed_plan_does_not_add_a_second_llm_brain():
    decision = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["contract_state"],
        metric_columns=["Grants"],
        confidence="high",
        semantic_plan={
            "operation": "aggregate",
            "statistic": "sum",
            "formula": {"operator": "none", "operands": []},
            "observation_grain": "state",
            "result_scope": "single",
            "sort_direction": "none",
            "top_k": None,
            "flow_direction": "none",
            "include_component_measures": False,
            "output_dimensions": ["state"],
        },
    )
    assert should_verify(decision) is False


def test_complex_typed_plan_keeps_independent_verification():
    decision = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["contract_county", "county_flow"],
        metric_columns=["subaward_amount", "Contracts"],
        confidence="high",
        semantic_plan={
            "operation": "breakdown",
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
    )
    assert should_verify(decision) is True


def test_high_risk_statistical_families_keep_independent_verification():
    decision = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["acs_state"],
        metric_columns=["Asian"],
        confidence="high",
        semantic_plan={
            "operation": "ranking",
            "statistic": "value",
            "result_unit": "percent",
            "formula": {"operator": "identity", "operands": ["Asian"]},
            "observation_grain": "state",
            "result_scope": "single",
            "sort_direction": "desc",
            "top_k": 1,
            "flow_direction": "none",
            "include_component_measures": False,
            "output_dimensions": ["state"],
        },
    )
    assert should_verify(decision) is True


def test_national_flow_ranking_uses_the_directional_geography_side():
    decision = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["congress_flow"],
        metric_columns=["subaward_amount"],
        geography_level="congress",
        operation="ranking",
        flow_direction="inflow",
        sort_direction="desc",
        top_k=1,
        semantic_plan={
            "operation": "ranking",
            "statistic": "sum",
            "result_unit": "usd",
            "formula": {"operator": "none", "operands": []},
            "predicate": {"operator": "none", "operands": []},
            "observation_grain": "congress",
            "result_scope": "single",
            "sort_direction": "desc",
            "top_k": 1,
            "flow_direction": "inflow",
            "include_component_measures": False,
            # Simulate a provider choosing the opposite side's readable label.
            "output_dimensions": ["rcpt_cd_name"],
        },
    )

    def stub(messages, json_mode, purpose):
        assert purpose == "stage12_intent_route"
        return json.dumps(decision)

    client.set_stub(stub)
    try:
        route = classify_and_route(
            "Which congressional district receives the most subaward inflow?"
        )
    finally:
        client.clear_stub()

    assert route["semantic_plan"]["output_dimensions"] == ["subawardee_cd_name"]


def test_verifier_cannot_multiply_marginal_acs_percentages():
    initial = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["acs_county"],
        metric_columns=["Black", "Asian"],
        filter_columns=["state"],
        geography_level="county",
        semantic_plan={
            "operation": "ranking",
            "statistic": "value",
            "result_unit": "percent",
            "formula": {"operator": "none", "operands": []},
            "observation_grain": "county",
            "result_scope": "full",
            "sort_direction": "desc",
            "top_k": None,
            "flow_direction": "none",
            "include_component_measures": True,
            "output_dimensions": ["county", "state"],
        },
    )
    invalid_audit = dict(initial)
    invalid_audit["metric_columns"] = ["Total population", "Black", "Asian"]
    invalid_audit["semantic_plan"] = {
        **initial["semantic_plan"],
        "statistic": "derived",
        "result_unit": "persons",
        "formula": {
            "operator": "multiply",
            "operands": ["Total population", "Black", "Asian"],
            "scale": 0.01,
        },
    }
    reconciled, reason = _reconcile_with_schema(
        "Rank the Wyoming counties by Black and Asian population.",
        initial,
        invalid_audit,
    )
    assert reconciled == initial
    assert reason == "blocked multiplication of marginal ACS percentages"


def test_verifier_cannot_silently_change_explicit_result_unit():
    initial = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["acs_county"],
        metric_columns=["Black", "Asian"],
        confidence="high",
        semantic_plan={
            "operation": "ranking",
            "statistic": "value",
            "result_unit": "percent",
            "formula": {"operator": "none", "operands": []},
            "observation_grain": "county",
            "result_scope": "full",
            "sort_direction": "desc",
            "top_k": None,
            "flow_direction": "none",
            "include_component_measures": True,
            "output_dimensions": ["county", "state"],
        },
    )
    audited = dict(initial)
    audited["semantic_plan"] = {
        **initial["semantic_plan"],
        "statistic": "derived",
        "result_unit": "persons",
        "formula": {
            "operator": "multiply",
            "operands": ["Total population", "Black"],
            "scale": 0.01,
        },
    }
    audited["metric_columns"] = ["Total population", "Black", "Asian"]
    reconciled, reason = _reconcile_with_schema(
        "Rank the Wyoming counties by Black and Asian population.",
        initial,
        audited,
    )
    assert reconciled == initial
    assert reason == "preserved authoritative typed result unit"


def test_verifier_cannot_clarify_without_a_schema_or_plan_disagreement():
    semantic_plan = {
        "operation": "comparison",
        "statistic": "derived",
        "result_unit": "usd",
        "formula": {
            "operator": "subtract",
            "operands": ["Grants", "Grants"],
            "scale": 1.0,
            "output_label": "grant gap",
        },
        "predicate": {"operator": "none", "operands": []},
        "observation_grain": "county",
        "result_scope": "single",
        "sort_direction": "none",
        "top_k": None,
        "flow_direction": "none",
        "include_component_measures": False,
        "output_dimensions": [],
    }
    initial = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["contract_county"],
        metric_columns=["Grants"],
        geography_level="county",
        operation="comparison",
        semantic_plan=semantic_plan,
    )
    audited = {
        **initial,
        "intent": "CLARIFY",
        "requires_sql": False,
        "needs_clarification": True,
        "clarification_question": "Which calculation convention should be used?",
    }

    reconciled, reason = _reconcile_with_schema(
        "What is the gap between the highest and lowest county Grants?",
        initial,
        audited,
    )

    assert reconciled == initial
    assert reason == "preserved complete analytical plan without a schema disagreement"


def test_verifier_can_clarify_when_the_typed_plan_is_incomplete():
    initial = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["contract_county"],
        metric_columns=["Grants"],
        geography_level="county",
        operation="comparison",
        semantic_plan={
            "operation": "comparison",
            "statistic": "unspecified",
            "result_unit": "usd",
            "observation_grain": "county",
            "result_scope": "unspecified",
        },
    )
    audited = {
        **initial,
        "intent": "CLARIFY",
        "requires_sql": False,
        "needs_clarification": True,
        "clarification_question": "Which comparison do you mean?",
    }

    reconciled, reason = _reconcile_with_schema(
        "Compare county Grants.",
        initial,
        audited,
    )

    assert reconciled == audited
    assert reason == ""


def test_verifier_cannot_introduce_a_predicate_across_incompatible_units():
    initial = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["acs_county", "gov_county"],
        metric_columns=["Below poverty", "Debt_Ratio"],
        geography_level="county",
        operation="ranking",
        semantic_plan={
            "operation": "ranking",
            "statistic": "value",
            "result_unit": "value",
            "formula": {"operator": "none", "operands": []},
            "predicate": {"operator": "none", "operands": []},
            "observation_grain": "county",
            "result_scope": "top_n",
            "sort_direction": "desc",
            "top_k": 10,
            "flow_direction": "none",
            "include_component_measures": True,
            "output_dimensions": ["county", "state"],
        },
    )
    audited = {
        **initial,
        "operation": "comparison",
        "semantic_plan": {
            **initial["semantic_plan"],
            "operation": "comparison",
            "predicate": {
                "operator": "gt",
                "operands": ["Below poverty", "Debt_Ratio"],
            },
            "result_scope": "full",
            "sort_direction": "none",
            "top_k": None,
        },
    }

    reconciled, reason = _reconcile_with_schema(
        "Which counties are high on poverty and government debt ratio?",
        initial,
        audited,
    )

    assert reconciled == initial
    assert reason == "blocked row predicate across incompatible metric units"


def test_runtime_canonicalizes_net_position_and_county_identity():
    decision = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["gov_county"],
        metric_columns=["Total_Assets", "Total_Liabilities"],
        filter_columns=[],
        geography_level="county",
        operation="ranking",
        sort_direction="asc",
        top_k=1,
        semantic_plan={
            "operation": "ranking",
            "statistic": "derived",
            "result_unit": "usd",
            "formula": {
                "operator": "subtract",
                "operands": ["Total_Assets", "Total_Liabilities"],
                "scale": 1.0,
            },
            "observation_grain": "county",
            "result_scope": "single",
            "sort_direction": "asc",
            "top_k": 1,
            "flow_direction": "none",
            "include_component_measures": False,
            "output_dimensions": ["county"],
        },
    )

    def stub(messages, json_mode, purpose):
        assert purpose in {"stage12_intent_route", "route_verification"}
        return json.dumps(decision)

    client.set_stub(stub)
    try:
        route = classify_and_route("Which county is poorest by assets minus liabilities?")
    finally:
        client.clear_stub()
    assert route["columns"] == ["Net_Position"]
    assert route["semantic_plan"]["formula"]["operator"] == "identity"
    assert route["semantic_plan"]["output_dimensions"] == ["county", "state"]


def test_runtime_canonicalizes_unqualified_reversed_net_position_formula():
    decision = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["gov_state"],
        metric_columns=["Total_Liabilities", "Total_Assets"],
        filter_columns=[],
        geography_level="state",
        operation="ranking",
        sort_direction="desc",
        top_k=1,
        semantic_plan={
            "operation": "ranking",
            "statistic": "derived",
            "result_unit": "usd",
            "formula": {
                "operator": "subtract",
                "operands": ["Total_Liabilities", "Total_Assets"],
                "scale": 1.0,
            },
            "observation_grain": "state",
            "result_scope": "single",
            "sort_direction": "desc",
            "top_k": 1,
            "flow_direction": "none",
            "include_component_measures": False,
            "output_dimensions": ["state"],
        },
    )

    client.set_stub(lambda messages, json_mode, purpose: json.dumps(decision))
    try:
        route = classify_and_route(
            "Which state has the highest negative difference in total assets and liabilities?"
        )
    finally:
        client.clear_stub()

    assert route["columns"] == ["Net_Position"]
    assert route["semantic_plan"]["formula"]["operator"] == "identity"
    assert route["semantic_plan"]["sort_direction"] == "asc"


def test_runtime_preserves_explicit_liabilities_minus_assets_formula():
    decision = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["gov_state"],
        metric_columns=["Total_Liabilities", "Total_Assets"],
        filter_columns=[],
        geography_level="state",
        operation="ranking",
        sort_direction="desc",
        top_k=1,
        semantic_plan={
            "operation": "ranking",
            "statistic": "derived",
            "result_unit": "usd",
            "formula": {
                "operator": "subtract",
                "operands": ["Total_Liabilities", "Total_Assets"],
                "scale": 1.0,
            },
            "observation_grain": "state",
            "result_scope": "single",
            "sort_direction": "desc",
            "top_k": 1,
            "flow_direction": "none",
            "include_component_measures": False,
            "output_dimensions": ["state"],
        },
    )

    client.set_stub(lambda messages, json_mode, purpose: json.dumps(decision))
    try:
        route = classify_and_route("Which state has the largest liabilities minus assets?")
    finally:
        client.clear_stub()

    assert route["columns"] == ["Total_Liabilities", "Total_Assets"]
    assert route["semantic_plan"]["formula"]["operator"] == "subtract"
    assert route["semantic_plan"]["sort_direction"] == "desc"


def test_description_rich_catalog_contains_source_vocabulary():
    catalog = semantic_catalog_for_verification().casefold()
    assert "benefit transfers" in catalog
    assert "financial wellbeing" in catalog
    assert "cost of public services" in catalog
    assert "government net worth" in catalog


def test_route_verifier_recovers_false_missing_measure():
    calls: list[str] = []

    def stub(messages, json_mode, purpose):
        calls.append(purpose)
        if purpose == "stage12_intent_route":
            return json.dumps(_base_decision())
        if purpose == "route_verification":
            return json.dumps(
                _base_decision(
                    intent="ANALYTICAL",
                    requires_sql=True,
                    reason="Benefit transfers are documented as Direct Payments.",
                    tables=["contract_state"],
                    metric_columns=["Direct Payments"],
                    operation="aggregate",
                    year_strategy="2024",
                )
            )
        raise AssertionError(purpose)

    client.set_stub(stub)
    try:
        route = classify_and_route("How much benefit transfer funding went to Maryland?")
    finally:
        client.clear_stub()
    assert route["intent"] == "ANALYTICAL"
    assert route["tables"] == ["contract_state"]
    assert route["columns"] == ["Direct Payments"]
    assert route["route_verification"]["changed"] is True
    assert calls == ["stage12_intent_route", "route_verification"]


def test_route_verifier_preserves_exact_dataset_absence():
    unsupported = _base_decision(
        reason="contract_county has no Employees column.",
        tables=["contract_county"],
    )

    def stub(messages, json_mode, purpose):
        if purpose == "stage12_intent_route":
            return json.dumps(unsupported)
        return json.dumps(
            _base_decision(
                intent="ANALYTICAL",
                requires_sql=True,
                tables=["contract_county"],
                metric_columns=["Federal Residents"],
                reason="incorrectly substitute a nearby field",
            )
        )

    client.set_stub(stub)
    try:
        route = classify_and_route("How many employees are in contract_county?")
    finally:
        client.clear_stub()
    assert route["intent"] == "UNANSWERABLE"
    assert route["tables"] == []
    assert route["route_verification"]["changed"] is False
    assert "metric substitution" in route["route_verification"]["reconciliation"]


def test_route_verifier_allows_documented_cross_table_contract_derivation():
    unsupported = _base_decision(
        reason="contract_county has no physical Sub-contract Out column.",
        tables=["contract_county"],
        geography_level="county",
    )

    def stub(messages, json_mode, purpose):
        if purpose == "stage12_intent_route":
            return json.dumps(unsupported)
        return json.dumps(
            _base_decision(
                intent="ANALYTICAL",
                requires_sql=True,
                tables=["contract_county", "county_flow"],
                metric_columns=["Contracts", "subaward_amount"],
                geography_level="county",
                operation="ranking",
                year_strategy="contract_county: 2024; county_flow: 2024",
                reason="The source dictionary defines this cross-table ratio.",
            )
        )

    client.set_stub(stub)
    try:
        route = classify_and_route(
            "In contract_county, show Sub-contract Out as a percentage of "
            "Federal Contracts for Nevada counties."
        )
    finally:
        client.clear_stub()
    assert route["intent"] == "ANALYTICAL"
    assert route["tables"] == ["contract_county", "county_flow"]
    assert route["columns"] == ["Contracts", "subaward_amount"]


def test_route_verifier_blocks_impossible_agency_geography_intersection():
    def stub(messages, json_mode, purpose):
        if purpose == "stage12_intent_route":
            return json.dumps(
                _base_decision(
                    intent="ANALYTICAL",
                    requires_sql=True,
                    tables=["contract_county"],
                    metric_columns=["Contracts"],
                    geography_level="county",
                    operation="ranking",
                    top_k=10,
                )
            )
        return json.dumps(
            _base_decision(
                intent="ANALYTICAL",
                requires_sql=True,
                tables=["county_flow"],
                metric_columns=["subaward_amount"],
                geography_level="county",
                operation="ranking",
                top_k=10,
            )
        )

    client.set_stub(stub)
    try:
        route = classify_and_route(
            "Top 10 counties that received contracts from the Department of Defense."
        )
    finally:
        client.clear_stub()
    assert route["intent"] == "UNANSWERABLE"
    assert "agency dimension" in route["reason"]
    assert "impossible" in route["route_verification"]["reconciliation"]


def test_schema_reconciliation_preserves_valid_cross_table_agency_route():
    initial = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["spending_state_agency", "acs_state"],
        metric_columns=["Contracts", "Total population"],
        geography_level="state",
        operation="ranking",
        semantic_plan={
            "operation": "ranking",
            "statistic": "derived",
            "result_unit": "ratio",
            "formula": {
                "operator": "divide",
                "operands": ["Contracts", "Total population"],
                "scale": 1000,
            },
            "observation_grain": "state",
            "result_scope": "full",
            "sort_direction": "desc",
            "output_dimensions": ["state"],
        },
    )
    audited = _base_decision(
        reason="No single table contains agency and population.",
        geography_level="state",
    )

    reconciled, reason = _reconcile_with_schema(
        "Using ACS total population, rank states by Department of Defense contracts per 1,000 people.",
        initial,
        audited,
    )

    assert reconciled["intent"] == "ANALYTICAL"
    assert reconciled["tables"] == ["spending_state_agency", "acs_state"]
    assert reason == "preserved schema-capable cross-table agency route"


def test_schema_capability_recognizes_documented_county_flow_fips_join():
    route = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["county_flow", "gov_county"],
        metric_columns=["subaward_amount", "Total_Liabilities_per_capita"],
        geography_level="county",
    )

    assert _schema_capable_cross_table_route(route) is True


def test_schema_capability_rejects_mismatched_geography_route():
    route = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["contract_state", "gov_county"],
        metric_columns=["Contracts", "Total_Liabilities_per_capita"],
        geography_level="county",
    )

    assert _schema_capable_cross_table_route(route) is False


def test_planner_keeps_named_nonflow_dimensions_as_filters():
    decision = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["spending_state_agency"],
        metric_columns=["Grants", "Direct Payments"],
        filter_columns=["state", "agency"],
        geography_level="state",
        operation="breakdown",
        confidence="high",
        semantic_plan={
            "operation": "breakdown",
            "statistic": "sum",
            "result_unit": "usd",
            "formula": {
                "operator": "add",
                "operands": ["Grants", "Direct Payments"],
                "scale": 1,
            },
            "observation_grain": "state",
            "result_scope": "single",
            "output_dimensions": ["state", "agency"],
        },
    )

    client.set_stub(lambda messages, json_mode, purpose: json.dumps(decision))
    try:
        route = classify_and_route(
            "In Maryland, break out HHS grants and direct payments and give their combined total."
        )
    finally:
        client.clear_stub()

    assert route["filter_columns"] == ["state", "agency"]


def test_planner_separates_flow_filter_side_from_output_side():
    decision = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["congress_flow"],
        metric_columns=["subaward_amount"],
        filter_columns=["rcpt_cd_name", "subawardee_cd_name"],
        geography_level="congress",
        operation="ranking",
        flow_direction="outflow",
        confidence="high",
        semantic_plan={
            "operation": "ranking",
            "statistic": "sum",
            "result_unit": "usd",
            "observation_grain": "congress",
            "result_scope": "top_n",
            "sort_direction": "desc",
            "sort_columns": ["subaward_amount"],
            "top_k": 10,
            "flow_direction": "outflow",
            "output_dimensions": ["subawardee_cd_name"],
        },
    )

    client.set_stub(lambda messages, json_mode, purpose: json.dumps(decision))
    try:
        route = classify_and_route(
            "Which 10 districts received the most 2024 funding originating in MD-08?"
        )
    finally:
        client.clear_stub()

    assert route["filter_columns"] == ["rcpt_cd_name"]


def test_within_state_correlation_uses_county_observations():
    state_route = _base_decision(
        intent="ANALYTICAL",
        requires_sql=True,
        tables=["acs_state"],
        metric_columns=[
            "Education >= High School",
            "Education >= Bachelor's",
            "Education >= Graduate",
            "Median household income",
            "Income >$50K",
            "Income >$100K",
            "Income >$200K",
        ],
        filter_columns=["state"],
        geography_level="state",
        operation="correlation",
        year_strategy="2023",
    )

    def stub(messages, json_mode, purpose):
        if purpose in {"stage12_intent_route", "route_verification"}:
            return json.dumps(state_route)
        raise AssertionError(purpose)

    client.set_stub(stub)
    try:
        route = classify_and_route(
            "For the state of Colorado, correlate all loaded education measures "
            "with median household income and the loaded income bands."
        )
    finally:
        client.clear_stub()
    assert route["intent"] == "ANALYTICAL"
    assert route["tables"] == ["acs_county"]
    assert route["geography_level"] == "county"
    assert route["route_verification"]["reconciliation"] == (
        "used county observations for within-state correlation"
    )
