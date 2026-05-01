from __future__ import annotations

import os
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
os.environ.setdefault("SQLITE_DB_PATH", str(ROOT / "data" / "runtime" / "test_unit.sqlite3"))
os.environ.setdefault("DUCKDB_PATH", str(ROOT / "data" / "runtime" / "test_unit.duckdb"))

from app.core.intent_classifier import classify_intent  # noqa: E402
from app.core.orchestrator import answer_question  # noqa: E402
from app.core.query_planner import create_query_plan  # noqa: E402
from app.schemas.query_plan import QueryPlan  # noqa: E402
from app.semantic.audit import build_semantic_coverage_audit  # noqa: E402
from app.semantic.retriever import retrieve_semantic_context  # noqa: E402
from app.semantic.registry import get_dataset, load_registry  # noqa: E402
from app.sql.generator import generate_sql  # noqa: E402
from app.sql.validator import SqlValidationError, validate_sql  # noqa: E402


class SemanticRegistryTests(unittest.TestCase):
    def test_registry_loads_runtime_datasets_and_default_funding_metric(self) -> None:
        registry = load_registry()
        self.assertEqual(len(registry.datasets), 17)
        county = get_dataset("contract_county")
        self.assertIsNotNone(county)
        assert county is not None
        self.assertIn("total_federal_funding", county.metrics)
        self.assertIn("funding", county.metrics["total_federal_funding"].default_for)

    def test_registry_links_metric_variants_by_semantic_concept(self) -> None:
        acs = get_dataset("acs_state")
        gov = get_dataset("gov_county")
        contract = get_dataset("contract_state")
        assert acs is not None
        assert gov is not None
        assert contract is not None

        self.assertEqual(acs.metrics["asian_population_count"].semantic_concept, "asian_population")
        self.assertEqual(acs.metrics["asian_population_count"].related_variants["share"], "asian_share")
        self.assertEqual(acs.metrics["asian_share"].related_variants["count"], "asian_population_count")

        self.assertEqual(gov.metrics["total_assets"].related_variants["per_capita"], "total_assets_per_capita")
        self.assertEqual(gov.metrics["total_assets_per_capita"].related_variants["amount"], "total_assets")

        self.assertEqual(contract.metrics["employees"].related_variants["per_1000"], "employees_per_1000")
        self.assertEqual(contract.metrics["employees_per_1000"].related_variants["count"], "employees")

        self.assertEqual(contract.metrics["total_federal_funding"].related_variants["per_1000"], "total_federal_funding_per_1000")
        self.assertEqual(contract.metrics["total_federal_funding_per_1000"].related_variants["amount"], "total_federal_funding")

    def test_semantic_coverage_audit_reports_runtime_metadata_alignment(self) -> None:
        audit = build_semantic_coverage_audit()
        self.assertEqual(audit["summary"]["runtime_table_count"], 17)
        self.assertEqual(audit["summary"]["registered_dataset_count"], 17)
        self.assertEqual(audit["summary"]["critical_issue_count"], 0)
        self.assertEqual(
            set(audit["documented_not_loaded"]),
            {"contract_cd_agency", "contract_county_agency", "contract_state_agency"},
        )
        self.assertGreater(audit["summary"]["column_coverage_ratio"], 0.75)


class PlanningTests(unittest.TestCase):
    def test_funding_question_plans_county_ranking_without_clarification(self) -> None:
        question = "top 10 counties in maryland with maximum funding"
        intent = classify_intent(question)
        context = retrieve_semantic_context(question)
        plan = create_query_plan(question, intent, context)
        self.assertEqual(plan.intent, "AGGREGATION")
        self.assertEqual(plan.datasets, ["contract_county"])
        self.assertEqual(plan.metrics, ["total_federal_funding"])
        self.assertEqual(plan.queries[0].limit, 10)
        self.assertEqual(plan.filters[0].field, "state")
        self.assertEqual(plan.filters[0].value, "Maryland")

    def test_asset_county_questions_resolve_to_government_finance(self) -> None:
        question = "top asset county in california"
        intent = classify_intent(question)
        context = retrieve_semantic_context(question)
        plan = create_query_plan(question, intent, context)
        self.assertEqual(plan.intent, "AGGREGATION")
        self.assertEqual(plan.datasets, ["gov_county"])
        self.assertEqual(plan.metrics, ["total_assets"])
        self.assertEqual(plan.queries[0].operation, "ranking")
        self.assertEqual(plan.queries[0].limit, 10)
        self.assertEqual(plan.filters[0].field, "state")
        self.assertEqual(plan.filters[0].value, "California")

    def test_p_asset_shorthand_resolves_to_assets_per_capita_ranking(self) -> None:
        question = "p asset county in california"
        intent = classify_intent(question)
        context = retrieve_semantic_context(question)
        plan = create_query_plan(question, intent, context)
        self.assertEqual(plan.datasets, ["gov_county"])
        self.assertEqual(plan.metrics, ["total_assets_per_capita"])
        self.assertEqual(plan.queries[0].operation, "ranking")
        self.assertEqual(plan.queries[0].limit, 10)

    def test_semantic_variants_resolve_without_one_off_rules(self) -> None:
        cases = [
            ("counties in CA with biggest assets", "gov_county", "total_assets", "ranking", "California"),
            ("highest liabilities p/c in md counties", "gov_county", "liabilities_per_capita", "ranking", "Maryland"),
            ("top ten states by fin lit", "finra_state", "financial_literacy", "ranking", None),
            ("financial stress counties in tx", "finra_county", "financial_constraint", "ranking", "Texas"),
            ("top counties in Maryland by college degree", "acs_county", "bachelors_attainment", "ranking", "Maryland"),
            ("largest grant money counties in CA", "contract_county", "grants", "ranking", "California"),
            ("federal jobs per capita in VA", "contract_state", "employees_per_1000", "lookup", "Virginia"),
            ("Where is the maximum asian population?", "acs_state", "asian_population_count", "ranking", None),
            ("Where is the maximum asian population based on amount?", "acs_state", "asian_population_count", "ranking", None),
            ("top states by asian population share", "acs_state", "asian_share", "ranking", None),
            ("epartment of defence biggest deals", "spending_state_agency", "contracts", "ranking", None),
        ]
        for question, family, metric, operation, focus_state in cases:
            with self.subTest(question=question):
                result = answer_question(question)
                self.assertEqual(result["resolution"], "answered")
                self.assertEqual(result["contract"]["family"], family)
                self.assertEqual(result["contract"]["metric"], metric)
                self.assertEqual(result["contract"]["operation"], operation)
                self.assertEqual(result["contract"]["focus_state"], focus_state)

    def test_department_defense_deals_resolve_to_agency_contract_ranking(self) -> None:
        result = answer_question("epartment of defence biggest deals")
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "spending_state_agency")
        self.assertEqual(result["contract"]["metric"], "contracts")
        self.assertEqual(result["contract"]["operation"], "ranking")
        self.assertEqual(result["row_count"], 10)
        self.assertIn("Department of Defense", result["answer"])
        self.assertIn("individual deal", result["answer"].lower())

    def test_single_state_maximum_employment_is_lookup_not_top_one_ranking(self) -> None:
        result = answer_question("Maximum employment in Maryland")
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "contract_state")
        self.assertEqual(result["contract"]["metric"], "employees")
        self.assertEqual(result["contract"]["operation"], "lookup")
        self.assertEqual(result["contract"]["focus_state"], "Maryland")
        self.assertEqual(result["row_count"], 1)
        self.assertNotIn("top 1 states", result["answer"].lower())

    def test_follow_up_amount_correction_overrides_prior_share_metric(self) -> None:
        result = answer_question(
            "what you gave is ratio based distribution, I need an amount based distribution.",
            [
                {"role": "user", "content": "Where is the maximum asian population?"},
                {
                    "role": "assistant",
                    "content": "I interpreted your question as Asian population share.",
                    "contract": {"family": "acs_state", "metric": "asian_share"},
                },
            ],
        )
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "acs_state")
        self.assertEqual(result["contract"]["metric"], "asian_population_count")
        self.assertEqual(result["contract"]["operation"], "ranking")
        self.assertEqual(result["row_count"], 10)

    def test_follow_up_ratio_correction_overrides_prior_count_metric(self) -> None:
        history = [
            {"role": "user", "content": "Where is the maximum asian population?"},
            {
                "role": "assistant",
                "content": "I interpreted your question as Asian population count.",
            },
        ]
        for question in ("based on ratio?", "Based on percentage?"):
            with self.subTest(question=question):
                result = answer_question(question, history)
                self.assertEqual(result["resolution"], "answered")
                self.assertEqual(result["contract"]["family"], "acs_state")
                self.assertEqual(result["contract"]["metric"], "asian_share")
                self.assertEqual(result["contract"]["operation"], "ranking")
                self.assertEqual(result["row_count"], 10)

    def test_follow_up_population_count_preserves_prior_concept(self) -> None:
        result = answer_question(
            "based on population count?",
            [
                {"role": "user", "content": "Where is the maximum asian population based on percentage?"},
                {
                    "role": "assistant",
                    "content": "I interpreted your question as Asian population share.",
                    "contract": {"family": "acs_state", "metric": "asian_share", "operation": "ranking"},
                },
            ],
        )
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "acs_state")
        self.assertEqual(result["contract"]["metric"], "asian_population_count")
        self.assertEqual(result["contract"]["operation"], "ranking")
        self.assertEqual(result["row_count"], 10)

    def test_follow_up_variant_correction_preserves_prior_query_shape(self) -> None:
        employment = answer_question(
            "per 1000?",
            [
                {"role": "user", "content": "rank top 10 states based on employment"},
                {"role": "assistant", "content": "Here are the top states by Employees."},
            ],
        )
        self.assertEqual(employment["resolution"], "answered")
        self.assertEqual(employment["contract"]["family"], "contract_state")
        self.assertEqual(employment["contract"]["metric"], "employees_per_1000")
        self.assertEqual(employment["contract"]["operation"], "ranking")
        self.assertEqual(employment["row_count"], 10)

        assets = answer_question(
            "based on per capita?",
            [
                {"role": "user", "content": "top counties in California by assets"},
                {"role": "assistant", "content": "Here are the top California counties by Total assets."},
            ],
        )
        self.assertEqual(assets["resolution"], "answered")
        self.assertEqual(assets["contract"]["family"], "gov_county")
        self.assertEqual(assets["contract"]["metric"], "total_assets_per_capita")
        self.assertEqual(assets["contract"]["operation"], "ranking")
        self.assertEqual(assets["contract"]["focus_state"], "California")
        self.assertEqual(assets["row_count"], 10)

    def test_free_cash_flow_does_not_route_to_fund_flow(self) -> None:
        result = answer_question("Maryland congressional districts by free cash flow")
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "gov_congress")
        self.assertEqual(result["contract"]["metric"], "free_cash_flow")
        self.assertIn("LIKE UPPER('MD-%')", result["sql"])
        self.assertGreater(result["row_count"], 0)

    def test_position_question_ranks_full_peer_set_before_filtering_focus(self) -> None:
        result = answer_question("Where does Maryland rank nationally for debt ratio?")
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "gov_state")
        self.assertEqual(result["contract"]["metric"], "debt_ratio")
        self.assertEqual(result["contract"]["operation"], "position")
        self.assertEqual(result["contract"]["focus_state"], "Maryland")
        self.assertEqual(result["row_count"], 1)
        self.assertIn("COUNT(*) OVER () AS total_count", result["sql"])
        self.assertIn("Maryland ranks #", result["answer"])
        self.assertIn("Peer average", result["answer"])
        self.assertNotIn("WHERE LOWER(CAST(state AS VARCHAR)) = LOWER('Maryland')", result["sql"])

    def test_stand_question_uses_position_context_for_single_state(self) -> None:
        result = answer_question("Where does California stand on assets?")
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "gov_state")
        self.assertEqual(result["contract"]["metric"], "total_assets")
        self.assertEqual(result["contract"]["operation"], "position")
        self.assertEqual(result["contract"]["focus_state"], "California")
        self.assertIn("California ranks #", result["answer"])

    def test_county_employment_uses_documented_proxy(self) -> None:
        result = answer_question("rank top 10 counties in maryland based on employment")
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "contract_county")
        self.assertEqual(result["contract"]["metric"], "federal_residents")
        self.assertEqual(result["contract"]["focus_state"], "Maryland")
        self.assertEqual(result["row_count"], 10)
        self.assertIn("Federal residents", result["answer"])
        self.assertIn("proxy", result["answer"].lower())
        self.assertIsNotNone(result["sql"])

    def test_follow_up_county_correction_rebuilds_geography_and_keeps_metric_concept(self) -> None:
        history = [
            {"role": "user", "content": "Maximum employment in Maryland"},
            {
                "role": "assistant",
                "content": "Maryland has 143,910 federal employees.",
                "contract": {"family": "contract_state", "metric": "employees", "operation": "lookup"},
            },
        ]
        for question in ("I meant counties in maryland.", "countis not states"):
            with self.subTest(question=question):
                result = answer_question(question, history)
                self.assertEqual(result["resolution"], "answered")
                self.assertEqual(result["contract"]["family"], "contract_county")
                self.assertEqual(result["contract"]["metric"], "federal_residents")
                self.assertEqual(result["contract"]["operation"], "ranking")
                self.assertEqual(result["contract"]["focus_state"], "Maryland")
                self.assertEqual(result["row_count"], 10)
                self.assertIn("proxy", result["answer"].lower())

    def test_frustration_repair_does_not_execute_random_sql(self) -> None:
        result = answer_question(
            "are you crasy",
            [
                {"role": "user", "content": "countis not states"},
                {
                    "role": "assistant",
                    "content": "Maryland has 143,910 federal employees.",
                    "contract": {"family": "contract_state", "metric": "employees", "operation": "lookup"},
                },
            ],
        )
        self.assertEqual(result["resolution"], "answered_with_assumptions")
        self.assertIsNone(result["sql"])
        self.assertEqual(result["contract"]["contract_type"], "CONVERSATION_REPAIR")
        self.assertIn("over-carried", result["answer"])

    def test_employment_supported_at_state_and_agency_levels(self) -> None:
        states = answer_question("rank top 10 states based on employment")
        self.assertEqual(states["resolution"], "answered")
        self.assertEqual(states["contract"]["family"], "contract_state")
        self.assertEqual(states["contract"]["metric"], "employees")
        self.assertEqual(states["row_count"], 10)

        agencies = answer_question("rank agencies in Maryland by federal employees")
        self.assertEqual(agencies["resolution"], "answered")
        self.assertEqual(agencies["contract"]["family"], "spending_state_agency")
        self.assertEqual(agencies["contract"]["metric"], "employees")
        self.assertEqual(agencies["contract"]["focus_state"], "Maryland")
        self.assertEqual(agencies["row_count"], 10)

    def test_unsupported_metric_does_not_inherit_prior_context(self) -> None:
        result = answer_question(
            "top counties with maximum crime",
            [{"role": "user", "content": "top 10 counties by funding"}],
        )
        self.assertEqual(result["resolution"], "unsupported")
        self.assertIsNone(result["contract"]["metric"])
        self.assertIn("not available", result["answer"].lower())

    def test_follow_up_compare_inherits_metric_only_when_missing(self) -> None:
        result = answer_question("compare Maryland vs Virginia", [{"role": "user", "content": "grants in maryland"}])
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "contract_state")
        self.assertEqual(result["contract"]["metric"], "grants")
        self.assertEqual(result["row_count"], 2)

    def test_broad_federal_money_clarifies(self) -> None:
        result = answer_question("How much federal money goes to Maryland?")
        self.assertEqual(result["resolution"], "needs_clarification")
        self.assertIn("ambiguous", result["answer"].lower())
        self.assertIn("subcontract", result["answer"].lower())

    def test_normalized_broad_funding_uses_per_1000_variant_without_clarifying(self) -> None:
        result = answer_question("funding per 1000 in Maryland")
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "contract_state")
        self.assertEqual(result["contract"]["metric"], "total_federal_funding_per_1000")
        self.assertEqual(result["contract"]["operation"], "lookup")
        self.assertEqual(result["contract"]["focus_state"], "Maryland")
        self.assertEqual(result["row_count"], 1)

    def test_sql_generator_uses_only_mart_views(self) -> None:
        question = "top 10 counties in maryland with maximum funding"
        plan = create_query_plan(question, classify_intent(question), retrieve_semantic_context(question))
        sql = generate_sql(plan)[0]["sql"]
        self.assertIn("mart_contract_county", sql)
        self.assertIn("total_federal", plan.metrics[0])
        validate_sql(sql)


class SqlValidatorTests(unittest.TestCase):
    def test_blocks_mutation_and_file_access(self) -> None:
        with self.assertRaises(SqlValidationError):
            validate_sql("DROP TABLE mart_contract_county")
        with self.assertRaises(SqlValidationError):
            validate_sql("SELECT * FROM read_parquet('/tmp/x.parquet')")


class OrchestratorTests(unittest.TestCase):
    def test_answers_maryland_county_funding_ranking(self) -> None:
        result = answer_question("top 10 counties in maryland with maximum funding")
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "contract_county")
        self.assertEqual(result["contract"]["metric"], "total_federal_funding")
        self.assertEqual(result["row_count"], 10)
        self.assertIn("top 10 counties", result["answer"].lower())
        self.assertIsNotNone(result["sql"])
        self.assertEqual(result["quality"]["status"], "ok")

    def test_answers_california_county_assets(self) -> None:
        result = answer_question("top asset county in california")
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "gov_county")
        self.assertEqual(result["contract"]["metric"], "total_assets")
        self.assertEqual(result["contract"]["operation"], "ranking")
        self.assertEqual(result["contract"]["focus_state"], "California")
        self.assertEqual(result["row_count"], 10)
        self.assertIn("Total assets", result["answer"])

    def test_answers_p_asset_as_california_county_assets_per_capita(self) -> None:
        result = answer_question("p asset county in california")
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "gov_county")
        self.assertEqual(result["contract"]["metric"], "total_assets_per_capita")
        self.assertEqual(result["contract"]["operation"], "ranking")
        self.assertEqual(result["contract"]["focus_state"], "California")
        self.assertEqual(result["row_count"], 10)
        self.assertIn("per capita", result["answer"].lower())

    def test_identity_question_does_not_hit_sql(self) -> None:
        result = answer_question("who are you?")
        self.assertEqual(result["resolution"], "answered")
        self.assertIsNone(result["sql"])
        self.assertIn("analytics assistant", result["answer"].lower())

    def test_help_variants_do_not_hit_sql(self) -> None:
        for question in ("how can you help me?", "what kind of questions can I ask?", "what insights can you give me?"):
            result = answer_question(question)
            self.assertEqual(result["resolution"], "answered")
            self.assertIsNone(result["sql"])
            self.assertEqual(result["contract"]["contract_type"], "ASSISTANT_HELP")

    def test_dataset_discovery_and_metric_definition_are_non_sql(self) -> None:
        discovery = answer_question("what data do you have?")
        self.assertIsNone(discovery["sql"])
        self.assertEqual(discovery["contract"]["contract_type"], "DATASET_DISCOVERY")
        self.assertIn("approved data areas", discovery["answer"].lower())

        definition = answer_question("what does grants mean?")
        self.assertIsNone(definition["sql"])
        self.assertEqual(definition["contract"]["contract_type"], "METRIC_DEFINITION")
        self.assertIn("sql expression", definition["answer"].lower())

    def test_clarification_response_resolves_prior_ambiguity_with_scope(self) -> None:
        first = answer_question("How much federal money goes to Maryland?")
        history = [
            {"role": "user", "content": "How much federal money goes to Maryland?"},
            {"role": "assistant", "content": first["answer"]},
        ]
        result = answer_question("the first one", history)
        self.assertEqual(result["resolution"], "answered")
        self.assertEqual(result["contract"]["family"], "contract_state")
        self.assertEqual(result["contract"]["metric"], "total_federal_funding")
        self.assertEqual(result["contract"]["focus_state"], "Maryland")
        self.assertEqual(result["row_count"], 1)

    def test_agency_breakout_and_flow_paths(self) -> None:
        agency = answer_question("which agencies provide the most grants to Maryland")
        self.assertEqual(agency["resolution"], "answered")
        self.assertEqual(agency["contract"]["family"], "spending_state_agency")
        self.assertEqual(agency["contract"]["operation"], "breakdown")
        self.assertEqual(agency["row_count"], 10)

        flow = answer_question("subcontract inflow to Maryland")
        self.assertEqual(flow["resolution"], "answered")
        self.assertEqual(flow["contract"]["family"], "state_flow")
        self.assertEqual(flow["contract"]["operation"], "flow_ranking")
        self.assertGreater(flow["row_count"], 0)


if __name__ == "__main__":
    unittest.main()
