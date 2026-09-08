from __future__ import annotations

import json

from app.api.datasets import dataset_catalog
from app.core import meta_answer
from app.core.planner import classify_and_route
from app.llm import client
from app.semantic.audit import build_semantic_coverage_audit
from app.semantic.registry import catalog_for_prompt, get_dataset, table_schema_block


def test_router_preserves_catalog_grounding_for_meta_questions() -> None:
    client.set_stub(
        lambda messages, json_mode, purpose: json.dumps(
            {
                "intent": "META",
                "requires_sql": False,
                "needs_clarification": False,
                "clarification_question": "",
                "reason": "schema availability question",
                "tables": ["contract_state"],
                "metric_columns": ["Employees"],
                "filter_columns": [],
                "geography_level": "state",
                "operation": "lookup",
                "flow_direction": "none",
                "sort_direction": "none",
                "top_k": None,
                "year_strategy": "",
                "join_plan": "",
                "assumptions": [],
                "confidence": "high",
            }
        )
    )
    try:
        route = classify_and_route("Does contract_state contain Employees?")
    finally:
        client.clear_stub()
    assert route["intent"] == "META"
    assert route["tables"] == []
    assert route["catalog_tables"] == ["contract_state"]
    assert route["catalog_columns"] == ["Employees"]


def test_meta_answer_repairs_a_schema_contradiction() -> None:
    calls: list[str] = []

    def stub(messages, json_mode, purpose):
        calls.append(purpose)
        if purpose == "meta":
            return "The contract_state dataset does not contain Employees."
        if purpose == "meta_repair":
            return "The `contract_state` dataset contains an `Employees` measure. It represents federal civilian employees working in each state."
        if purpose == "meta_faithfulness" and calls.count("meta_faithfulness") == 1:
            return json.dumps(
                {
                    "faithful": False,
                    "complete": False,
                    "reason": "Employees is explicitly present in the supplied schema.",
                }
            )
        if purpose == "meta_faithfulness":
            return json.dumps({"faithful": True, "complete": True, "reason": ""})
        raise AssertionError(f"unexpected purpose: {purpose}")

    client.set_stub(stub)
    try:
        response = meta_answer.respond(
            "Does contract_state contain Employees?",
            "META",
            {"catalog_tables": ["contract_state"], "catalog_columns": ["Employees"]},
        )
    finally:
        client.clear_stub()
    assert "contains" in response["answer"]
    assert "working in each state" in response["answer"]
    assert response["confidence"] == "high"
    assert calls == ["meta", "meta_faithfulness", "meta_repair", "meta_faithfulness"]
    assert response["context_memory"]["metrics"] == ["Employees"]


def test_contract_employee_semantics_are_present_in_both_llm_prompts() -> None:
    route_catalog = catalog_for_prompt()
    sql_schema = table_schema_block("contract_state")
    assert "employee count" in route_catalog.casefold()
    assert "federal employment" in route_catalog.casefold()
    assert "Employees [FLOAT;measure; unit:persons; default-row-aggregation:sum]" in sql_schema
    assert "Federal civilian employees working in this state" in sql_schema


def test_catalog_prompt_can_be_scoped_after_route_is_audited() -> None:
    scoped = catalog_for_prompt({"finra_state"})
    assert "### finra_state" in scoped
    assert "### acs_state" not in scoped
    assert len(scoped) < len(catalog_for_prompt())


def test_employee_dictionary_example_does_not_say_states_receive_people() -> None:
    contract_state = next(
        table
        for family in dataset_catalog()
        for table in family["tables"]
        if table["tableName"] == "contract_state"
    )
    employees = next(
        variable for variable in contract_state["variables"] if variable["name"] == "Employees"
    )
    assert "received" not in employees["exampleQuestion"].casefold()
    assert "highest employees" in employees["exampleQuestion"].casefold()


def test_acs_dictionary_discloses_runtime_subset_and_cross_tab_limits() -> None:
    acs_state = next(
        table
        for family in dataset_catalog()
        for table in family["tables"]
        if table["tableName"] == "acs_state"
    )
    notes = " ".join(acs_state["notes"]).casefold()
    assert "only age 18-65" in notes
    assert "no sex/gender columns" in notes
    assert "separate marginal percentages" in notes
    assert "adults age 25+" in notes
    assert "unweighted summary" in notes
    assert "not a population-weighted u.s. national statistic" in notes


def test_acs_runtime_limitations_reach_model_grounding() -> None:
    route_catalog = catalog_for_prompt().casefold()
    sql_schema = table_schema_block("acs_state").casefold()
    for text in (route_catalog, sql_schema):
        assert "only age 18-65" in text
        assert "joint cross-tabulation" in text
        assert "does not contain the number of adults age 25+" in text


def test_gov_state_coverage_note_matches_physical_runtime() -> None:
    gov_state = next(
        table
        for family in dataset_catalog()
        for table in family["tables"]
        if table["tableName"] == "gov_state"
    )
    notes = " ".join(gov_state["notes"]).casefold()
    assert "district of columbia is a legitimate runtime row" in notes
    assert "connecticut" in notes and "not present" in notes


def test_every_runtime_dataset_is_semantically_certified_and_evaluated() -> None:
    audit = build_semantic_coverage_audit()
    summary = audit["summary"]
    assert summary["semantically_certified_dataset_count"] == 17
    assert summary["evaluation_covered_dataset_count"] == 17
    assert summary["critical_issue_count"] == 0


def test_inherited_metric_semantics_include_units_and_definitions() -> None:
    gov_county = get_dataset("gov_county")
    finra_county = get_dataset("finra_county")
    assert gov_county is not None and finra_county is not None
    per_capita = gov_county.metrics["Total_Assets_per_capita"]
    assert "divided by the represented population" in per_capita.description
    assert per_capita.unit == "USD per person"
    assert finra_county.metrics["financial_constraint"].unit == "index (0-1)"
    assert finra_county.metrics["satisfied"].unit == "proportion (0-1)"
