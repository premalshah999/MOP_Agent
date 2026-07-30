from __future__ import annotations

from app.api.datasets import dataset_catalog
from app.core import orchestrator
from app.core import meta_answer
from app.semantic.discovery import build_guidance, discover_metrics


def _tables() -> list[dict]:
    return [table for family in dataset_catalog() for table in family["tables"]]


def test_dataset_api_exposes_complete_documented_dictionary() -> None:
    tables = _tables()
    assert len(tables) == 17
    assert sum(len(table["variables"]) for table in tables) == 298
    for table in tables:
        assert table["source"]
        assert table["periodLabel"]
        assert table["variables"]
        assert "Unnamed: 0" not in {variable["name"] for variable in table["variables"]}
        for variable in table["variables"]:
            assert variable["role"] in {"measure", "dimension"}
            assert variable["label"]
            assert variable["description"]
            assert variable["exampleQuestion"]


def test_dictionary_inherits_column_docs_across_geographies() -> None:
    county = next(table for table in _tables() if table["tableName"] == "acs_county")
    poverty = next(variable for variable in county["variables"] if variable["name"] == "Below poverty")
    assert poverty["label"] == "Poverty rate"
    assert "poverty" in poverty["description"].casefold()
    assert poverty["unit"].casefold() == "percent"


def test_typo_discovery_resolves_to_canonical_metric() -> None:
    matches = discover_metrics("show financial literasy for Maryland")
    assert matches[0].concept.variable == "financial_literacy"
    assert matches[0].score >= 0.9
    guidance = build_guidance("show financial literasy for Maryland")
    assert "Financial literacy index" in guidance["answer"]
    assert guidance["suggestions"][0] == "What is Maryland's financial literacy index in 2021?"


def test_unknown_metric_never_claims_it_is_supported() -> None:
    guidance = build_guidance(
        "Which states have the highest crime rate?",
        intent="UNANSWERABLE",
    )
    assert guidance["resolution"] == "unsupported"
    assert "couldn't match" in guidance["answer"]
    assert guidance["suggestions"]
    assert all("crime" not in suggestion.casefold() for suggestion in guidance["suggestions"])
    assert all(candidate["dataset"] for candidate in guidance["context_memory"]["discovery_candidates"])


def test_dictionary_no_result_prompt_routes_to_concept_guidance() -> None:
    response = meta_answer.respond(
        'I couldn\'t find "unemployment" in this dataset. What is the closest measure I can use?',
        "META",
        {},
    )
    assert response["resolution"] == "unsupported"
    assert response["suggestions"]
    assert all("unemployment" not in suggestion.casefold() for suggestion in response["suggestions"])


def test_ambiguous_federal_funding_uses_real_channels_and_period() -> None:
    guidance = build_guidance("How much federal money goes to Maryland?")
    suggestions = guidance["suggestions"]
    assert any("federal contracts" in item.casefold() for item in suggestions)
    assert any("federal grants" in item.casefold() for item in suggestions)
    assert any("direct payments" in item.casefold() for item in suggestions)
    assert all("FY2024" in item for item in suggestions)


def test_flow_guidance_preserves_direction_and_entity() -> None:
    guidance = build_guidance("subcontrct outflow from Maryland")
    assert guidance["suggestions"][0] == "How much federal subaward funding flows out of Maryland?"


def test_orchestrator_attaches_grounded_options_and_memory(monkeypatch) -> None:
    monkeypatch.setattr(orchestrator, "contextualize", lambda question, history: question)
    monkeypatch.setattr(
        orchestrator,
        "classify_and_route",
        lambda question, history: {
            "intent": "UNANSWERABLE",
            "requires_sql": False,
            "needs_clarification": False,
            "clarification_question": "",
            "reason": "unsupported metric",
            "service_unavailable": False,
        },
    )
    result = orchestrator.answer_question("Which states have the highest crime rate?")
    assert result["resolution"] == "unsupported"
    assert result["suggested_followups"]
    assert result["contract"]["context_memory"]["discovery_candidates"]
