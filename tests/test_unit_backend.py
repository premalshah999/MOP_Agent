"""Unit tests for the rewritten MOP agent pipeline."""

from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

from app import safety  # noqa: E402
from app import sql_utils  # noqa: E402
from app import router  # noqa: E402
from app import formatter  # noqa: E402
from app import map_intent  # noqa: E402
from app import prompts  # noqa: E402
from app import planner  # noqa: E402
from app import plan_verifier  # noqa: E402
from app import query_frame  # noqa: E402
from app import semantic_registry  # noqa: E402
from app.metadata_answerer import answer_metadata_question  # noqa: E402


# ===================================================================
# SQL Utilities
# ===================================================================
class SqlUtilsTests(unittest.TestCase):
    def test_extract_sql_from_markdown_fence(self) -> None:
        text = "Here is the query:\n```sql\nSELECT * FROM gov_state;\n```"
        self.assertEqual(sql_utils.extract_sql(text), "SELECT * FROM gov_state;")

    def test_extract_sql_from_plain_text(self) -> None:
        text = "Sure, here you go: SELECT state FROM gov_state LIMIT 5;"
        result = sql_utils.extract_sql(text)
        self.assertTrue(result.startswith("SELECT"))

    def test_extract_sql_with_cte(self) -> None:
        text = "Let me write this: WITH cte AS (SELECT 1) SELECT * FROM cte;"
        result = sql_utils.extract_sql(text)
        self.assertTrue(result.startswith("WITH"))

    def test_extract_sql_empty(self) -> None:
        self.assertEqual(sql_utils.extract_sql(""), "")

    def test_auto_quote_columns_with_spaces(self) -> None:
        sql = "SELECT Contracts Per 1000 FROM contract_state"
        result = sql_utils.auto_quote_columns(sql)
        self.assertIn('"Contracts Per 1000"', result)

    def test_auto_fix_year_string(self) -> None:
        sql = "SELECT * FROM contract_state WHERE year = 2024"
        result = sql_utils.auto_fix_year_string(sql)
        self.assertIn("year = '2024'", result)

    def test_auto_fix_year_string_leaves_acs_alone(self) -> None:
        sql = "SELECT * FROM acs_state WHERE Year = 2023"
        result = sql_utils.auto_fix_year_string(sql)
        self.assertIn("Year = 2023", result)
        self.assertNotIn("'2023'", result)

    def test_is_ranking_question(self) -> None:
        self.assertTrue(sql_utils.is_ranking_question("Which states have the highest debt ratio?"))
        self.assertTrue(sql_utils.is_ranking_question("Top 10 counties by revenue"))
        self.assertFalse(sql_utils.is_ranking_question("What is the debt ratio for Maryland?"))

    def test_detect_explicit_k(self) -> None:
        self.assertEqual(sql_utils.detect_explicit_k("top 10 states"), 10)
        self.assertEqual(sql_utils.detect_explicit_k("bottom 5 counties"), 5)
        self.assertIsNone(sql_utils.detect_explicit_k("Which state has the highest?"))

    def test_ranking_top_k(self) -> None:
        self.assertEqual(sql_utils.ranking_top_k("top 10 states"), 10)
        self.assertEqual(sql_utils.ranking_top_k("highest debt ratio"), 15)  # default

    def test_apply_limit_adds_to_ordered_query(self) -> None:
        sql = "SELECT * FROM gov_state ORDER BY Debt_Ratio DESC"
        result = sql_utils.apply_limit(sql, 15)
        self.assertIn("LIMIT 15", result)

    def test_apply_limit_replaces_existing(self) -> None:
        sql = "SELECT * FROM gov_state ORDER BY Debt_Ratio DESC LIMIT 100"
        result = sql_utils.apply_limit(sql, 15)
        self.assertIn("LIMIT 15", result)
        self.assertNotIn("LIMIT 100", result)

    def test_prepare_sql_full_pipeline(self) -> None:
        raw = "```sql\nSELECT state, Contracts Per 1000 FROM contract_state WHERE year = 2024 ORDER BY Contracts Per 1000 DESC\n```"
        result = sql_utils.prepare_sql(raw, "Top 10 states by contracts per 1000")
        self.assertIn('"Contracts Per 1000"', result)
        self.assertIn("year = '2024'", result)
        self.assertIn("LIMIT", result)

    def test_prepare_sql_does_not_limit_leaderboard_bundle_queries(self) -> None:
        raw = (
            "WITH ranked AS (SELECT state, Debt_Ratio AS debt_ratio, 1 AS metric_rank, 50 AS total_states, 0.44 AS national_average FROM gov_state), "
            "focus AS (SELECT 'focus' AS row_kind, state, debt_ratio, metric_rank, total_states, national_average, 0 AS list_position FROM ranked), "
            "top_rows AS (SELECT 'top' AS row_kind, state, debt_ratio, metric_rank, total_states, national_average, metric_rank AS list_position FROM ranked), "
            "bottom_rows AS (SELECT 'bottom' AS row_kind, state, debt_ratio, metric_rank, total_states, national_average, metric_rank AS list_position FROM ranked) "
            "SELECT * FROM focus UNION ALL SELECT * FROM top_rows UNION ALL SELECT * FROM bottom_rows ORDER BY row_kind, list_position"
        )
        result = sql_utils.prepare_sql(
            raw,
            "Where does Maryland rank nationally for debt ratio, and what are the top 10 and bottom 10 states?",
        )
        self.assertNotIn(" LIMIT ", result.upper())


# ===================================================================
# Router
# ===================================================================
class RouterTests(unittest.TestCase):
    def test_detect_geo_level_state(self) -> None:
        self.assertEqual(router.detect_geo_level("Which states have the highest debt?"), "state")

    def test_detect_geo_level_county(self) -> None:
        self.assertEqual(router.detect_geo_level("Show me counties in Maryland"), "county")

    def test_detect_geo_level_congress(self) -> None:
        self.assertEqual(router.detect_geo_level("Congressional districts with lowest income"), "congress")

    def test_detect_geo_level_none(self) -> None:
        self.assertIsNone(router.detect_geo_level("What is debt ratio?"))

    def test_extract_state_name(self) -> None:
        self.assertEqual(router.extract_state_name("Revenue in California"), "california")
        self.assertEqual(router.extract_state_name("Maryland's debt ratio"), "maryland")
        self.assertIsNone(router.extract_state_name("Which is the highest?"))

    def test_extract_state_name_multiword(self) -> None:
        self.assertEqual(router.extract_state_name("Debt in New York"), "new york")
        self.assertEqual(router.extract_state_name("West Virginia counties"), "west virginia")

    def test_extract_year(self) -> None:
        self.assertEqual(router.extract_year("Federal spending in 2024"), "2024")
        self.assertIsNone(router.extract_year("Show me the top states"))

    def test_keyword_route_gov(self) -> None:
        tables = router._keyword_route("Which state has the highest debt ratio?")
        self.assertTrue(any("gov" in t for t in tables))

    def test_keyword_route_acs(self) -> None:
        tables = router._keyword_route("Poverty rates by county")
        self.assertTrue(any("acs" in t for t in tables))

    def test_keyword_route_contract(self) -> None:
        tables = router._keyword_route("Federal contracts by state in 2024")
        self.assertTrue(any("contract" in t for t in tables))

    def test_keyword_route_finra(self) -> None:
        tables = router._keyword_route("Financial literacy scores across states")
        self.assertTrue(any("finra" in t for t in tables))

    def test_keyword_route_flow(self) -> None:
        tables = router._keyword_route("Subaward flows from Maryland")
        self.assertTrue(any("flow" in t for t in tables))

    def test_keyword_route_agency(self) -> None:
        tables = router._keyword_route("Department of Defense spending by state")
        self.assertIn("spending_state_agency", tables)

    def test_keyword_route_free_cash_flow_stays_in_gov(self) -> None:
        tables = router._keyword_route("What state is highest on Free_Cash_Flow in Government Finances?")
        self.assertIn("gov_state", tables)
        self.assertFalse(any("flow" in t for t in tables))

    def test_keyword_route_default_fallback(self) -> None:
        tables = router._keyword_route("Tell me something interesting")
        self.assertTrue(len(tables) > 0)

    def test_build_schema_context_includes_columns(self) -> None:
        ctx = router.build_schema_context(["gov_state"])
        self.assertIn("TABLE: gov_state", ctx)
        self.assertIn("Debt_Ratio", ctx)

    def test_build_schema_context_multiple_tables(self) -> None:
        ctx = router.build_schema_context(["gov_state", "acs_state"])
        self.assertIn("TABLE: gov_state", ctx)
        self.assertIn("TABLE: acs_state", ctx)


class SemanticRegistryTests(unittest.TestCase):
    def test_semantic_catalog_marks_missing_runtime_agency_geos(self) -> None:
        catalog = semantic_registry.semantic_catalog()
        agency = catalog["agency"]
        self.assertIn("county", agency["missing_runtime_geographies"])
        self.assertIn("congress", agency["missing_runtime_geographies"])
        self.assertIn("state", agency["available_geographies"])

    def test_schema_fact_cd_118(self) -> None:
        fact = semantic_registry.schema_fact("cd_118")
        self.assertIsNotNone(fact)
        assert fact is not None
        self.assertIn("MD-05", fact)


class QueryFrameTests(unittest.TestCase):
    def test_query_frame_classifies_free_cash_flow_as_gov(self) -> None:
        frame = query_frame.infer_query_frame("What state is highest on Free_Cash_Flow in Government Finances?")
        self.assertEqual(frame.family, "gov")
        self.assertEqual(frame.metric_hint, "Free_Cash_Flow")

    def test_query_frame_extracts_congress_state_scope(self) -> None:
        frame = query_frame.infer_query_frame("Within Maryland congressional districts, which district is highest on Free_Cash_Flow?")
        self.assertEqual(frame.geo_level, "congress")
        self.assertEqual(frame.primary_state, "maryland")
        self.assertEqual(frame.state_postal, "MD")

    def test_query_frame_maps_income_above_200k_to_builtin_metric(self) -> None:
        frame = query_frame.infer_query_frame(
            "Which states have the largest share of households with income above $200K in 2023?"
        )
        self.assertEqual(frame.family, "acs")
        self.assertEqual(frame.metric_hint, "Income >$200K")
        self.assertEqual(frame.intent, "share")

    def test_query_frame_maps_black_ratio_to_builtin_metric(self) -> None:
        frame = query_frame.infer_query_frame("5 states with maximum black population by ratio.")
        self.assertEqual(frame.family, "acs")
        self.assertEqual(frame.metric_hint, "Black")
        self.assertEqual(frame.intent, "ranking")


class MapIntentTests(unittest.TestCase):
    def test_build_map_intent_for_state_ranking(self) -> None:
        df = pd.DataFrame(
            [
                {"state": "california", "Total_Liabilities": 154300485231},
                {"state": "florida", "Total_Liabilities": 76136644395},
                {"state": "maryland", "Total_Liabilities": 56874205850},
                {"state": "new york", "Total_Liabilities": 55336798755},
                {"state": "virginia", "Total_Liabilities": 41510908739},
            ]
        )
        intent = map_intent.build_map_intent(
            "Which state has the highest total liabilities?",
            df,
            ["gov_state"],
        )
        self.assertTrue(intent["enabled"])
        self.assertEqual(intent["dataset"], "gov_spending")
        self.assertEqual(intent["level"], "state")
        self.assertIn(intent["mapType"], {"atlas-single-metric", "top-n-highlight"})
        self.assertIn(intent.get("buttonLabel"), {"Open heat map", "Open map view"})
        self.assertEqual(intent.get("defaultView"), "heat")

    def test_build_map_intent_disabled_for_flow_answer(self) -> None:
        df = pd.DataFrame(
            [
                {"rcpt_state_name": "Virginia", "total_flow": 8_130_000_000},
                {"rcpt_state_name": "Texas", "total_flow": 5_130_000_000},
                {"rcpt_state_name": "Maryland", "total_flow": 4_000_000_000},
                {"rcpt_state_name": "California", "total_flow": 1_060_000_000},
            ]
        )
        intent = map_intent.build_map_intent(
            "Which states send the most subcontract inflow into Maryland?",
            df,
            ["state_flow"],
        )
        self.assertFalse(intent["enabled"])

    def test_build_map_intent_enables_single_row_state_lookup(self) -> None:
        df = pd.DataFrame(
            [{"state": "mississippi", "total_liabilities": 1_830_000_000, "metric_rank": 34, "total_states": 50}]
        )
        intent = map_intent.build_map_intent(
            "Where does Mississippi stand on total liabilities?",
            df,
            ["gov_state"],
        )
        self.assertTrue(intent["enabled"])
        self.assertEqual(intent["mapType"], "single-state-spotlight")
        self.assertEqual(intent["state"], "Mississippi")
        self.assertEqual(intent.get("buttonLabel"), "Open state map")

    def test_build_map_intent_enables_spending_total_spotlight(self) -> None:
        df = pd.DataFrame(
            [{"state": "maryland", "spending_total": 104_270_000_000, "Contracts": 46_230_000_000}]
        )
        intent = map_intent.build_map_intent(
            "How much federal money goes to Maryland?",
            df,
            ["contract_state"],
        )
        self.assertTrue(intent["enabled"])
        self.assertEqual(intent["dataset"], "contract_static")
        self.assertEqual(intent["metric"], "spending_total")
        self.assertEqual(intent["mapType"], "single-state-spotlight")

    def test_build_map_intent_enables_national_county_heat_map_without_state_filter(self) -> None:
        df = pd.DataFrame(
            [
                {"county": "Los Angeles", "state": "california", "Total population": 10_000_000},
                {"county": "Cook", "state": "illinois", "Total population": 5_000_000},
                {"county": "Harris", "state": "texas", "Total population": 4_700_000},
            ]
        )
        intent = map_intent.build_map_intent(
            "Which counties have the highest total population in 2023?",
            df,
            ["acs_county"],
        )
        self.assertTrue(intent["enabled"])
        self.assertEqual(intent["level"], "county")
        self.assertEqual(intent["mapType"], "top-n-highlight")

    def test_build_map_intent_includes_comparison_ids(self) -> None:
        df = pd.DataFrame(
            [
                {"state": "maryland", "Contracts": 46_230_238_790},
                {"state": "virginia", "Contracts": 87_510_000_000},
                {"state": "california", "Contracts": 63_100_000_000},
                {"state": "texas", "Contracts": 44_800_000_000},
            ]
        )
        intent = map_intent.build_map_intent(
            "Compare Maryland and Virginia on contracts in 2024.",
            df,
            ["contract_state"],
        )
        self.assertTrue(intent["enabled"])
        self.assertEqual(intent["mapType"], "atlas-comparison")
        self.assertEqual(intent["comparisonIds"], ["MD", "VA"])
        self.assertEqual(intent["comparisonLabels"], ["Maryland", "Virginia"])
        self.assertEqual(intent.get("buttonLabel"), "Open comparison map")
        self.assertEqual(intent.get("defaultView"), "comparison")


class MetadataAnswererTests(unittest.TestCase):
    def test_metadata_answer_breakdown_state_only(self) -> None:
        answer = answer_metadata_question("Is Federal Spending Breakdown available below the state level?")
        self.assertIsNotNone(answer)
        assert answer is not None
        self.assertIn("state-only", answer.lower())

    def test_metadata_answer_cd_118(self) -> None:
        answer = answer_metadata_question("What does cd_118 mean in these tables?")
        self.assertIsNotNone(answer)
        assert answer is not None
        self.assertIn("congressional district", answer.lower())

    def test_metadata_answer_non_numeric_years(self) -> None:
        answer = answer_metadata_question("Are all year fields numeric in the project?")
        self.assertIsNotNone(answer)
        assert answer is not None
        self.assertIn("2020-2024", answer)
        self.assertIn("Fiscal Year 2023", answer)

    def test_metadata_answer_relative_exposure(self) -> None:
        answer = answer_metadata_question("If a user asks for the largest relative exposure in Contracts, what field should be used?")
        self.assertIsNotNone(answer)
        assert answer is not None
        self.assertIn("Contracts Per 1000", answer)

    def test_metadata_answer_unsupported_gov_year(self) -> None:
        answer = answer_metadata_question("What was Maryland liabilities in 2021?")
        self.assertIsNotNone(answer)
        assert answer is not None
        self.assertIn("Fiscal Year 2023", answer)

    def test_metadata_answer_ambiguous_funding_source(self) -> None:
        answer = answer_metadata_question("What is the biggest funding source in Maryland?")
        self.assertIsNotNone(answer)
        assert answer is not None
        self.assertIn("ambiguous", answer.lower())

    def test_metadata_answer_causal_question(self) -> None:
        answer = answer_metadata_question("Do direct payments cause lower poverty?")
        self.assertIsNotNone(answer)
        assert answer is not None
        self.assertIn("avoid causal claims", answer.lower())

    def test_metadata_answer_dependent_on_federal_money_is_ambiguous(self) -> None:
        answer = answer_metadata_question("Which state is most dependent on federal money?")
        self.assertIsNotNone(answer)
        assert answer is not None
        self.assertIn("ambiguous", answer.lower())
        self.assertIn("per 1000", answer.lower())

    def test_metadata_answer_custom_score_requires_definition(self) -> None:
        answer = answer_metadata_question(
            "Can you rank Maryland districts by a custom score that combines grants (2024), financial literacy (2021), and bachelor's attainment (2023)?"
        )
        self.assertIsNotNone(answer)
        assert answer is not None
        self.assertIn("custom", answer.lower())
        self.assertIn("weights", answer.lower())

    def test_metadata_answer_clarifies_compare_without_metric(self) -> None:
        answer = answer_metadata_question("Compare Maryland and Virginia.")
        self.assertIsNotNone(answer)
        assert answer is not None
        self.assertIn("need the metric first", answer.lower())
        self.assertIn("Government Finances", answer)

    def test_metadata_answer_clarifies_state_overview_without_metric(self) -> None:
        answer = answer_metadata_question("Tell me about Maryland.")
        self.assertIsNotNone(answer)
        assert answer is not None
        self.assertIn("needs a dimension", answer.lower())
        self.assertIn("Federal Spending", answer)

    def test_metadata_answer_capabilities_question(self) -> None:
        answer = answer_metadata_question("What can you do?")
        self.assertIsNotNone(answer)
        assert answer is not None
        self.assertIn("Government Finances", answer)
        self.assertIn("Federal Spending", answer)
        self.assertIn("Fund Flow", answer)


class PlanVerifierTests(unittest.TestCase):
    def test_verifier_requires_normalized_metric_for_relative_questions(self) -> None:
        result = plan_verifier.verify_execution_candidate(
            "Which state has the largest relative exposure in Contracts?",
            "SELECT state, Contracts FROM contract_state ORDER BY Contracts DESC LIMIT 15",
            ["contract_state"],
        )

        self.assertFalse(result.ok)
        self.assertIsNotNone(result.error)
        assert result.error is not None
        self.assertIn("normalized", result.error.lower())

    def test_verifier_requires_md_scope_for_maryland_congress_queries(self) -> None:
        result = plan_verifier.verify_execution_candidate(
            "Within Maryland congressional districts, which district is highest on Free_Cash_Flow?",
            "SELECT cd_118, Free_Cash_Flow FROM gov_congress ORDER BY Free_Cash_Flow DESC LIMIT 15",
            ["gov_congress"],
        )

        self.assertFalse(result.ok)
        self.assertIsNotNone(result.error)
        assert result.error is not None
        self.assertIn("MD-scoped", result.error)

    def test_verifier_returns_direct_answer_for_missing_runtime_agency_geo(self) -> None:
        with patch("app.plan_verifier.runtime_table_loaded", return_value=False):
            result = plan_verifier.verify_execution_candidate(
                "Within Maryland counties in 2024, which counties have the highest Department of Defense contracts?",
                "SELECT county, Contracts FROM contract_county_agency WHERE lower(state) = 'maryland' AND year = '2024' ORDER BY Contracts DESC LIMIT 15",
                ["contract_county_agency"],
            )

        self.assertFalse(result.ok)
        self.assertIsNotNone(result.answer)
        assert result.answer is not None
        self.assertIn("not available", result.answer.lower())

    def test_verifier_allows_builtin_acs_share_metric_without_custom_denominator(self) -> None:
        result = plan_verifier.verify_execution_candidate(
            "Which states have the largest share of households with income above $200K in 2023?",
            'SELECT state, "Income >$200K" FROM acs_state WHERE Year = 2023 ORDER BY "Income >$200K" DESC LIMIT 15',
            ["acs_state"],
        )

        self.assertTrue(result.ok)
        self.assertIsNone(result.error)


# ===================================================================
# Prompts
# ===================================================================
class PromptsTests(unittest.TestCase):
    def test_get_relevant_examples_returns_nonempty(self) -> None:
        examples = prompts.get_relevant_examples(["gov_state"])
        self.assertIn("EXAMPLES", examples)
        self.assertIn("Q:", examples)
        self.assertIn("A:", examples)

    def test_get_relevant_examples_includes_cross_dataset(self) -> None:
        examples = prompts.get_relevant_examples(["gov_state"])
        # Cross-dataset examples are always included
        self.assertIn("corr(", examples)

    def test_get_relevant_examples_includes_table_specific(self) -> None:
        examples = prompts.get_relevant_examples(["finra_state"])
        self.assertIn("financial_literacy", examples)

    def test_lookup_definition_found(self) -> None:
        result = prompts.lookup_definition("What is the debt ratio?")
        self.assertIsNotNone(result)
        self.assertIn("liabilities", result.lower())

    def test_lookup_definition_not_found(self) -> None:
        result = prompts.lookup_definition("How is the weather?")
        self.assertIsNone(result)

    def test_sql_system_prompt_has_placeholders(self) -> None:
        self.assertIn("{schema_context}", prompts.SQL_SYSTEM_PROMPT)
        self.assertIn("{examples}", prompts.SQL_SYSTEM_PROMPT)

    def test_sql_system_prompt_has_critical_rules(self) -> None:
        self.assertIn("LOWER(", prompts.SQL_SYSTEM_PROMPT)
        self.assertIn("year = '2024'", prompts.SQL_SYSTEM_PROMPT)
        self.assertIn("gov_*", prompts.SQL_SYSTEM_PROMPT)


# ===================================================================
# Formatter
# ===================================================================
class FormatterTests(unittest.TestCase):
    def test_compute_statistics_basic(self) -> None:
        df = pd.DataFrame({
            "state": ["maryland", "virginia", "california"],
            "Debt_Ratio": [0.55, 0.42, 0.65],
        })
        stats = formatter.compute_statistics(df)
        self.assertEqual(stats["row_count"], 3)
        self.assertEqual(stats["label_column"], "state")
        self.assertIn("Debt_Ratio", stats["metrics"])
        m = stats["metrics"]["Debt_Ratio"]
        self.assertAlmostEqual(m["min"], 0.42)
        self.assertAlmostEqual(m["max"], 0.65)
        self.assertEqual(m["min_entity"], "virginia")
        self.assertEqual(m["max_entity"], "california")

    def test_compute_statistics_with_correlation(self) -> None:
        df = pd.DataFrame({
            "state": ["a", "b", "c", "d", "e"],
            "x": [1, 2, 3, 4, 5],
            "y": [2, 4, 6, 8, 10],
        })
        stats = formatter.compute_statistics(df)
        self.assertIn("correlation", stats)
        self.assertAlmostEqual(stats["correlation"]["r"], 1.0, places=3)

    def test_compute_statistics_single_row(self) -> None:
        df = pd.DataFrame({"state": ["maryland"], "Revenue_per_capita": [5000.0]})
        stats = formatter.compute_statistics(df)
        self.assertEqual(stats["row_count"], 1)

    def test_build_evidence_text(self) -> None:
        df = pd.DataFrame({
            "state": ["a", "b", "c"],
            "val": [10.0, 20.0, 30.0],
        })
        stats = formatter.compute_statistics(df)
        text = formatter.build_evidence_text(df, stats)
        self.assertIn("3 rows", text)
        self.assertIn("Top 3:", text)
        self.assertIn("Bottom 3:", text)

    def test_format_result_empty_df(self) -> None:
        df = pd.DataFrame()
        result = formatter.format_result("test?", df)
        self.assertEqual(result, "No data found matching your query.")

    def test_format_result_prefers_grounded_summary(self) -> None:
        df = pd.DataFrame({
            "agency": ["HHS", "Defense", "SSA"],
            "contracts": [12.0, 20.0, 5.0],
            "grants": [18.0, 5.0, 10.0],
            "resident_wage": [6.2, 6.8, 14.1],
            "spending_total": [36.2, 31.8, 29.1],
        })
        result = formatter.format_result("Which agencies account for the most spending in Maryland?", df)
        self.assertIn("HHS", result)
        self.assertIn("default federal spending", result)
        self.assertIn("**", result)
        self.assertIn("**Top 5:**", result)
        self.assertIn("**Leader profile:**", result)
        self.assertIn("**Context:**", result)
        self.assertIn("**Interpretation:**", result)
        self.assertIn("**You could ask next:**", result)

    def test_format_result_single_row_spending_includes_definition_and_composition(self) -> None:
        df = pd.DataFrame({
            "state": ["MARYLAND"],
            "contracts": [46_230_238_790.42],
            "grants": [30_579_948_445.74],
            "resident_wage": [27_461_823_181.99],
            "spending_total": [104_272_010_418.15],
        })
        result = formatter.format_result("How much federal money goes to Maryland?", df, sql="SELECT ... FROM contract_state WHERE year = '2024'")
        self.assertIn("**Definition:**", result)
        self.assertIn("**Breakdown:**", result)
        self.assertIn("**Composition:**", result)
        self.assertIn("**Scope:**", result)

    def test_format_result_uses_flow_pair_labels(self) -> None:
        df = pd.DataFrame({
            "rcpt_state_name": ["Virginia", "Texas", "Ohio"],
            "subawardee_state_name": ["Virginia", "California", "New Mexico"],
            "total_flow": [32_593_894_707.72, 19_205_492_501.90, 13_389_580_913.82],
        })
        result = formatter.format_result("biggest federal fund flow?", df)
        self.assertIn("Virginia -> Virginia", result)
        self.assertIn("Texas -> California", result)

    def test_format_result_handles_focus_top_bottom_leaderboard_bundle(self) -> None:
        df = pd.DataFrame({
            "row_kind": ["focus", "nearby", "nearby", "top", "top", "bottom", "bottom"],
            "state": ["maryland", "virginia", "california", "illinois", "connecticut", "wyoming", "vermont"],
            "Debt_Ratio": [0.65, 0.67, 0.63, 0.89, 0.85, 0.12, 0.10],
            "metric_rank": [12, 11, 13, 1, 2, 49, 50],
            "total_states": [50, 50, 50, 50, 50, 50, 50],
            "national_average": [0.41, 0.41, 0.41, 0.41, 0.41, 0.41, 0.41],
            "list_position": [0, 1, 2, 1, 2, 1, 2],
        })
        result = formatter.format_result(
            "Where does Maryland rank nationally for debt ratio, and what are the top 2 and bottom 2 states?",
            df,
            sql="SELECT * FROM gov_state",
        )
        self.assertIn("Maryland", result)
        self.assertIn("12th", result)
        self.assertIn("50", result)
        self.assertIn("**Around Maryland:**", result)
        self.assertIn("**Top 2:**", result)
        self.assertIn("**Bottom 2:**", result)

    def test_format_result_adds_normalized_definition_note(self) -> None:
        df = pd.DataFrame({
            "state": ["maryland", "virginia", "alaska"],
            "Contracts Per 1000": [1200.0, 980.0, 910.0],
        })
        result = formatter.format_result(
            "Which state has the largest relative exposure in Contracts?",
            df,
            sql="SELECT state, \"Contracts Per 1000\" FROM contract_state WHERE year = '2024' ORDER BY 2 DESC LIMIT 15",
        )
        self.assertIn("normalized", result.lower())
        self.assertIn("relative exposure", result.lower())

    def test_format_result_adds_processed_congress_scope_note(self) -> None:
        df = pd.DataFrame({
            "cd_118": ["MD-05", "MD-03", "MD-04"],
            "Free_Cash_Flow": [-548_230_000.0, -614_360_000.0, -717_240_000.0],
        })
        result = formatter.format_result(
            "Within Maryland congressional districts, which district is highest on Free_Cash_Flow?",
            df,
            sql="SELECT cd_118, Free_Cash_Flow FROM gov_congress WHERE UPPER(cd_118) LIKE 'MD-%' ORDER BY Free_Cash_Flow DESC LIMIT 15",
        )
        self.assertIn("processed congress-level government finance", result.lower())

    def test_fallback_answer_no_api_key(self) -> None:
        df = pd.DataFrame({
            "state": ["maryland", "virginia"],
            "Debt_Ratio": [0.55, 0.42],
        })
        stats = formatter.compute_statistics(df)
        answer = formatter._fallback_answer(df, stats)
        self.assertIn("Debt_Ratio", answer)
        self.assertIn("maryland", answer.lower() or "virginia" in answer.lower())


# ===================================================================
# Safety
# ===================================================================
class SafetyTests(unittest.TestCase):
    def test_read_only_sql_is_safe(self) -> None:
        self.assertTrue(safety.is_safe("SELECT state FROM gov_state LIMIT 5;"))

    def test_mutating_sql_is_blocked(self) -> None:
        self.assertFalse(safety.is_safe("DROP TABLE gov_state;"))

    def test_insert_is_blocked(self) -> None:
        self.assertFalse(safety.is_safe("INSERT INTO gov_state VALUES ('test');"))

    def test_with_cte_is_safe(self) -> None:
        self.assertTrue(safety.is_safe("WITH cte AS (SELECT 1) SELECT * FROM cte;"))


# ===================================================================
# Agent integration (mocked LLM)
# ===================================================================
class AgentIntegrationTests(unittest.TestCase):
    """Agent integration tests — require duckdb, skip if unavailable."""

    @classmethod
    def setUpClass(cls) -> None:
        try:
            import duckdb  # noqa: F401
            cls.skip_reason = None
        except ImportError:
            cls.skip_reason = "duckdb not installed"

    def setUp(self) -> None:
        if self.skip_reason:
            self.skipTest(self.skip_reason)

    def test_conceptual_question_returns_definition(self) -> None:
        from app import agent

        with patch.dict(os.environ, {"DEEPSEEK_API_KEY": ""}, clear=False):
            result = agent.ask_agent("What is debt ratio?", [])

        self.assertIn("answer", result)
        self.assertIn("liabilities", result["answer"].lower())
        self.assertIsNone(result.get("sql") or result["sql"])

    def test_ask_agent_data_query_flow(self) -> None:
        from app import agent

        fake_df = pd.DataFrame({
            "state": ["connecticut", "new jersey"],
            "Debt_Ratio": [0.89, 0.85],
        })

        with patch.dict(os.environ, {"DEEPSEEK_API_KEY": "test-key"}, clear=False):
            with patch("app.agent.classify", return_value="DATA_QUERY"):
                with patch("app.agent.route_tables", return_value=["gov_state"]):
                    with patch("app.agent._generate_sql", return_value="SELECT state, Debt_Ratio FROM gov_state ORDER BY Debt_Ratio DESC LIMIT 15;"):
                        with patch("app.agent._execute_with_repair", return_value=(fake_df, "SELECT ...", None)):
                            with patch("app.agent.format_result", return_value="Connecticut leads."):
                                result = agent.ask_agent("Top states by debt ratio", [])

        self.assertEqual(result["answer"], "Connecticut leads.")
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(len(result["data"]), 2)

    def test_ask_agent_error_is_user_friendly(self) -> None:
        from app import agent

        with patch.dict(os.environ, {"DEEPSEEK_API_KEY": "test-key"}, clear=False):
            with patch("app.agent.classify", return_value="DATA_QUERY"):
                with patch("app.agent.plan_query", return_value=None):
                    with patch("app.agent.route_tables", return_value=["gov_state"]):
                        with patch("app.agent._generate_sql", return_value="SELECT 1 UNION ALL SELECT 1, 2"):
                            with patch(
                                "app.agent._execute_with_repair",
                                return_value=(None, "SELECT ...", "Set operations can only apply to expressions with same column count"),
                            ):
                                result = agent.ask_agent("Compare Maryland and Virginia on debt ratio.", [])

        self.assertIn("error", result)
        # Error should be user-friendly, not raw SQL error
        self.assertNotIn("Set operations", result["error"])

    def test_ask_agent_uses_deterministic_planner_without_api_key(self) -> None:
        from app import agent

        with patch.dict(os.environ, {"DEEPSEEK_API_KEY": ""}, clear=False):
            result = agent.ask_agent("Which agencies account for the most spending in Maryland?", [])

        self.assertIn("sql", result)
        self.assertIn("spending_state_agency", result["sql"])
        self.assertGreater(result["row_count"], 0)

    def test_ask_agent_answers_metadata_question_without_sql(self) -> None:
        from app import agent

        with patch.dict(os.environ, {"DEEPSEEK_API_KEY": ""}, clear=False):
            result = agent.ask_agent("Are all year fields numeric in the project?", [])

        self.assertIn("answer", result)
        self.assertIsNone(result["sql"])
        self.assertIn("2020-2024", result["answer"])

    def test_ask_agent_metadata_guard_runs_before_conceptual_fallback(self) -> None:
        from app import agent

        with patch.dict(os.environ, {"DEEPSEEK_API_KEY": "test-key"}, clear=False):
            with patch("app.agent.classify", return_value="CONCEPTUAL"):
                with patch("app.agent.llm_complete", side_effect=AssertionError("LLM should not run")):
                    result = agent.ask_agent(
                        "If the user asks for “the biggest funding source” without specifying whether they mean contracts, grants, direct payments, or a composite, what should the chatbot do?",
                        [],
                    )

        self.assertIn("answer", result)
        self.assertIn("ambiguous", result["answer"].lower())
        self.assertIsNone(result["sql"])

    def test_ask_agent_uses_verifier_caveat_before_query_execution(self) -> None:
        from app import agent

        question = "Within Maryland counties in 2024, which counties have the highest Department of Defense contracts?"
        sql = (
            "SELECT county, Contracts FROM contract_county_agency "
            "WHERE lower(state) = 'maryland' AND year = '2024' "
            "ORDER BY Contracts DESC LIMIT 15"
        )

        with patch.dict(os.environ, {"DEEPSEEK_API_KEY": "test-key"}, clear=False):
            with patch("app.agent.classify", return_value="DATA_QUERY"):
                with patch("app.agent.plan_query", return_value=None):
                    with patch("app.agent.route_tables", return_value=["contract_county_agency"]):
                        with patch("app.agent.build_schema_context", return_value="schema"):
                            with patch("app.agent._generate_sql", return_value=sql):
                                with patch("app.plan_verifier.runtime_table_loaded", return_value=False):
                                    with patch("app.agent.execute_query", side_effect=AssertionError("execute_query should not run")):
                                        result = agent.ask_agent(question, [])

        self.assertIn("answer", result)
        self.assertIn("not available", result["answer"].lower())
        self.assertEqual(result["row_count"], 0)

    def test_explicit_metric_position_question_is_not_forced_into_followup(self) -> None:
        from app import agent

        agent._last_query.clear()
        intent = agent._classify_intent(
            "Where does Mississippi stand on total liabilities?",
            [{"role": "user", "content": "Which state has the highest total liabilities?"}],
        )
        self.assertNotEqual(intent, "FOLLOWUP")

    def test_ask_agent_returns_rank_for_mississippi_position_lookup(self) -> None:
        from app import agent

        agent._last_query.clear()
        history = [
            {"role": "user", "content": "Which state has the highest total liabilities?"},
            {"role": "assistant", "content": "California has the highest total liabilities."},
        ]
        result = agent.ask_agent("Where does Mississippi stand on total liabilities?", history)
        self.assertIn("Mississippi", result["answer"])
        self.assertIn("ranks", result["answer"])
        self.assertIn("34th", result["answer"])

    def test_ask_agent_clarifies_compare_without_metric(self) -> None:
        from app import agent

        with patch.dict(os.environ, {"DEEPSEEK_API_KEY": "test-key"}, clear=False):
            result = agent.ask_agent("Compare Maryland and Virginia.", [])

        self.assertIn("answer", result)
        self.assertIn("need the metric first", result["answer"].lower())
        self.assertIsNone(result["sql"])

    def test_ask_agent_clarifies_state_overview_without_metric(self) -> None:
        from app import agent

        with patch.dict(os.environ, {"DEEPSEEK_API_KEY": "test-key"}, clear=False):
            result = agent.ask_agent("Tell me about Maryland.", [])

        self.assertIn("answer", result)
        self.assertIn("needs a dimension", result["answer"].lower())
        self.assertIsNone(result["sql"])


class ChartGeneratorTests(unittest.TestCase):
    """Test Vega-Lite chart auto-generation."""

    def test_bar_chart_for_ranking(self) -> None:
        from app.chart_generator import generate_chart_spec
        df = pd.DataFrame({
            "state": ["connecticut", "new jersey", "illinois", "massachusetts", "hawaii"],
            "Debt_Ratio": [0.89, 0.85, 0.82, 0.78, 0.75],
        })
        spec = generate_chart_spec(df, "top 5 states by debt ratio")
        self.assertIsNotNone(spec)
        self.assertEqual(spec["mark"]["type"], "bar")
        self.assertIn("data", spec)
        self.assertEqual(len(spec["data"]["values"]), 5)

    def test_no_chart_for_single_row(self) -> None:
        from app.chart_generator import generate_chart_spec
        df = pd.DataFrame({"state": ["maryland"], "Debt_Ratio": [0.65]})
        spec = generate_chart_spec(df, "Maryland debt ratio")
        self.assertIsNone(spec)

    def test_scatter_chart_for_correlation(self) -> None:
        from app.chart_generator import generate_chart_spec
        df = pd.DataFrame({
            "state": ["a", "b", "c", "d", "e"],
            "literacy": [3.1, 2.9, 3.5, 2.7, 3.3],
            "debt": [0.5, 0.6, 0.3, 0.7, 0.4],
        })
        spec = generate_chart_spec(df, "correlation between literacy and debt")
        self.assertIsNotNone(spec)
        self.assertEqual(spec["mark"]["type"], "point")

    def test_line_chart_for_time_series(self) -> None:
        from app.chart_generator import generate_chart_spec
        df = pd.DataFrame({
            "year": [2019, 2020, 2021, 2022, 2023],
            "population": [100, 105, 110, 115, 120],
        })
        spec = generate_chart_spec(df, "population trend")
        self.assertIsNotNone(spec)
        self.assertEqual(spec["mark"]["type"], "line")

    def test_no_chart_for_empty_df(self) -> None:
        from app.chart_generator import generate_chart_spec
        df = pd.DataFrame(columns=["state", "value"])
        spec = generate_chart_spec(df, "anything")
        self.assertIsNone(spec)


class ResultValidationTests(unittest.TestCase):
    """Test result validation logic."""

    def test_empty_result_returns_hint(self) -> None:
        from app.agent import _validate_result
        df = pd.DataFrame(columns=["state", "value"])
        hint = _validate_result(df, "federal contracts in maryland")
        self.assertIsNotNone(hint)
        self.assertIn("0 rows", hint)
        self.assertIn("year as VARCHAR", hint)

    def test_valid_result_returns_none(self) -> None:
        from app.agent import _validate_result
        df = pd.DataFrame({"state": ["maryland"], "value": [100]})
        hint = _validate_result(df, "show maryland")
        self.assertIsNone(hint)

    def test_excessive_rows_returns_hint(self) -> None:
        from app.agent import _validate_result
        df = pd.DataFrame({"x": range(200_000)})
        hint = _validate_result(df, "some query")
        self.assertIsNotNone(hint)
        self.assertIn("excessive", hint)


class ClassifierTests(unittest.TestCase):
    """Test intent classification edge cases."""

    def test_short_pronoun_question_is_followup(self) -> None:
        from app.classifier import _fallback_classifier
        self.assertEqual(_fallback_classifier("what is it?"), "FOLLOWUP")
        self.assertEqual(_fallback_classifier("which one is it?"), "FOLLOWUP")
        self.assertEqual(_fallback_classifier("what flow is that?"), "FOLLOWUP")
        self.assertEqual(_fallback_classifier("What department was this flow?"), "FOLLOWUP")

    def test_definitional_question_is_conceptual(self) -> None:
        from app.classifier import _fallback_classifier
        self.assertEqual(_fallback_classifier("explain debt ratio"), "CONCEPTUAL")
        self.assertEqual(_fallback_classifier("define current ratio"), "CONCEPTUAL")

    def test_data_question_is_data_query(self) -> None:
        from app.classifier import _fallback_classifier
        self.assertEqual(_fallback_classifier("states with highest pension liability"), "DATA_QUERY")


class FollowupResolverTests(unittest.TestCase):
    """Test follow-up resolution with detail-seeking detection."""

    def test_detail_seeking_adds_instruction(self) -> None:
        from app import agent
        agent._last_query["question"] = "largest fund flow"
        agent._last_query["sql"] = "SELECT MAX(amount) FROM fund_flow;"
        agent._last_query["answer_snippet"] = "The largest flow is $5.2B"

        result = agent._resolve_followup("what flow is it?", [])
        self.assertIn("SPECIFIC record", result)
        self.assertIn("SELECT ALL relevant columns", result)

    def test_challenge_adds_anti_sycophancy(self) -> None:
        from app import agent
        agent._last_query["question"] = "top state"
        agent._last_query["sql"] = "SELECT state FROM gov_state LIMIT 1;"

        result = agent._resolve_followup("are you sure about that?", [])
        self.assertIn("challenging the previous result", result)

    def test_classify_intent_short_pronoun_with_history(self) -> None:
        from app import agent
        agent._last_query["sql"] = "SELECT * FROM gov_state;"
        intent = agent._classify_intent("what is it?", [{"role": "user", "content": "hi"}])
        self.assertEqual(intent, "FOLLOWUP")

    def test_drilldown_adds_breakdown_instruction(self) -> None:
        from app import agent
        agent._last_query["question"] = "biggest fund flow"
        agent._last_query["sql"] = "SELECT rcpt_state_name, subawardee_state_name, SUM(subaward_amount_year) AS total FROM state_flow GROUP BY 1,2 ORDER BY total DESC LIMIT 15;"
        agent._last_query["answer_snippet"] = "The single largest fund flow is Virginia to Virginia at $32.59B"

        result = agent._resolve_followup("What department was this flow?", [])
        self.assertIn("BREAKDOWN", result)
        self.assertIn("GROUP BY", result)

    def test_this_pronoun_is_followup(self) -> None:
        from app import agent
        agent._last_query["sql"] = "SELECT * FROM state_flow;"
        intent = agent._classify_intent("What department was this flow?", [{"role": "user", "content": "biggest flow"}])
        self.assertEqual(intent, "FOLLOWUP")

    def test_short_state_context_question_is_followup(self) -> None:
        from app import agent
        agent._last_query["sql"] = "SELECT state, Total_Liabilities FROM gov_state ORDER BY Total_Liabilities DESC LIMIT 15;"
        intent = agent._classify_intent(
            "where does Mississipi stand?",
            [{"role": "user", "content": "Which state has the highest total liabilities?"}],
        )
        self.assertEqual(intent, "FOLLOWUP")

    def test_short_state_followup_inherits_previous_metric(self) -> None:
        from app import agent
        agent._last_query["question"] = "Which state has the highest total liabilities?"
        result = agent._resolve_followup("where does Mississipi stand?", [])
        self.assertIn("Which state has the highest total liabilities?", result)
        self.assertIn("Mississippi", result)


class JsonInMessageTests(unittest.TestCase):
    """Test JSON-in-message SQL extraction from structured responses."""

    def test_extract_sql_from_json_response(self) -> None:
        from app.agent import _generate_sql_structured
        from app.sql_utils import extract_sql
        import json

        # Simulate a JSON response like the new prompt format
        json_response = '{"reasoning": "Using gov_state for debt ratio ranking", "sql": "SELECT state, Debt_Ratio FROM gov_state ORDER BY Debt_Ratio DESC LIMIT 10;"}'
        parsed = json.loads(json_response)
        sql = extract_sql(parsed["sql"])
        self.assertTrue(sql.startswith("SELECT"))
        self.assertIn("Debt_Ratio", sql)

    def test_extract_sql_from_json_with_cte(self) -> None:
        from app.sql_utils import extract_sql
        import json

        json_response = '{"reasoning": "CTE for window function", "sql": "WITH ranked AS (SELECT state, val, RANK() OVER(ORDER BY val DESC) AS rnk FROM t) SELECT * FROM ranked WHERE rnk <= 5;"}'
        parsed = json.loads(json_response)
        sql = extract_sql(parsed["sql"])
        self.assertTrue(sql.startswith("WITH"))

    def test_extract_sql_fallback_to_raw(self) -> None:
        from app.sql_utils import extract_sql
        # When the model returns raw SQL without JSON wrapper
        raw = "SELECT state FROM gov_state LIMIT 5;"
        sql = extract_sql(raw)
        self.assertEqual(sql, "SELECT state FROM gov_state LIMIT 5;")


class SchemaContextTests(unittest.TestCase):
    """Test enhanced schema context builder."""

    def test_schema_context_has_key_columns(self) -> None:
        from app.router import build_schema_context
        ctx = build_schema_context(["gov_state"])
        self.assertIn("Key columns:", ctx)
        self.assertIn("Data columns:", ctx)

    def test_schema_context_join_hints_for_multi_table(self) -> None:
        from app.router import build_schema_context
        ctx = build_schema_context(["acs_state", "gov_state"])
        self.assertIn("JOIN PATHS:", ctx)
        self.assertIn("LOWER(", ctx)

    def test_schema_context_no_join_hints_for_single_table(self) -> None:
        from app.router import build_schema_context
        ctx = build_schema_context(["gov_state"])
        self.assertNotIn("JOIN PATHS:", ctx)

    def test_schema_context_includes_canonical_spending_rules(self) -> None:
        from app.router import build_schema_context
        ctx = build_schema_context(["contract_state"])
        self.assertIn("CANONICAL CHATBOT RULES:", ctx)
        self.assertIn('Contracts + Grants + "Resident Wage"', ctx)
        self.assertIn("Direct Payments", ctx)
        self.assertIn('Use stored "Per 1000" and `_per_capita` fields directly', ctx)


class PromptImprovementTests(unittest.TestCase):
    """Test that the improved prompt contains critical new rules."""

    def test_prompt_has_table_decision_guide(self) -> None:
        self.assertIn("TABLE DECISION GUIDE", prompts.SQL_SYSTEM_PROMPT)

    def test_prompt_has_cte_template(self) -> None:
        self.assertIn("WITH ranked AS", prompts.SQL_SYSTEM_PROMPT)

    def test_prompt_has_flow_table_grain(self) -> None:
        self.assertIn("agency_name", prompts.SQL_SYSTEM_PROMPT)
        self.assertIn("one row per agency per NAICS", prompts.SQL_SYSTEM_PROMPT)

    def test_prompt_has_json_output_format(self) -> None:
        self.assertIn('"reasoning"', prompts.SQL_SYSTEM_PROMPT)
        self.assertIn('"sql"', prompts.SQL_SYSTEM_PROMPT)

    def test_prompt_has_congress_flow_trap(self) -> None:
        self.assertIn("prime_awardee_stcd118 is INTEGER", prompts.SQL_SYSTEM_PROMPT)

    def test_prompt_has_cross_year_alignment(self) -> None:
        self.assertIn("a.Year = 2023 AND c.year = '2024'", prompts.SQL_SYSTEM_PROMPT)

    def test_prompt_distinguishes_spending_state_year_column(self) -> None:
        self.assertIn("| spending_state | Year | VARCHAR | '2024' |", prompts.SQL_SYSTEM_PROMPT)

    def test_prompt_includes_dashboard_spending_rule(self) -> None:
        self.assertIn('use spending_total = Contracts + Grants + "Resident Wage"', prompts.SQL_SYSTEM_PROMPT)
        self.assertIn("Do not silently invent a total", prompts.SQL_SYSTEM_PROMPT)
        self.assertIn('Do not recompute "Per 1000" or `_per_capita` fields', prompts.SQL_SYSTEM_PROMPT)

    def test_examples_use_json_format(self) -> None:
        examples = prompts.get_relevant_examples(["state_flow"])
        self.assertIn('"reasoning":', examples)
        self.assertIn('"sql":', examples)

    def test_flow_examples_include_agency_drilldown(self) -> None:
        examples = prompts.get_relevant_examples(["state_flow"])
        self.assertIn("agency_name", examples)


class PlannerTests(unittest.TestCase):
    def test_planner_uses_agency_table_for_broad_spending(self) -> None:
        plan = planner.plan_query("Which agencies account for the most spending in Maryland?")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.table_names, ["spending_state_agency"])
        self.assertIn("Resident Wage", plan.sql)
        self.assertIn("spending_total", plan.sql)
        self.assertNotIn("Direct Payments", plan.sql)
        self.assertNotIn("Employees Wage", plan.sql)
        self.assertNotIn("Federal Residents", plan.sql)
        self.assertNotIn("Employees +", plan.sql)

    def test_planner_uses_dashboard_composite_for_broad_federal_spending(self) -> None:
        plan = planner.plan_query("How much federal money goes to Maryland?")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.table_names, ["contract_state"])
        self.assertIn("spending_total", plan.sql)
        self.assertIn("Resident Wage", plan.sql)
        self.assertNotIn("Direct Payments", plan.sql)
        self.assertNotIn("Employees Wage", plan.sql)

    def test_planner_prefers_total_assets_for_assets_question(self) -> None:
        plan = planner.plan_query("highest assets county in california?")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.table_names, ["gov_county"])
        self.assertIn("Total_Assets", plan.sql)
        self.assertNotIn("Current_Ratio", plan.sql)

    def test_planner_handles_gov_metric_ranking(self) -> None:
        plan = planner.plan_query("Which states have the highest debt ratio?")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.table_names, ["gov_state"])
        self.assertIn("Debt_Ratio", plan.sql)
        self.assertIn("ORDER BY", plan.sql)

    def test_planner_handles_finra_trend(self) -> None:
        plan = planner.plan_query("How has financial literacy changed over time nationally?")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.table_names, ["finra_state"])
        self.assertIn("SELECT Year AS period", plan.sql)

    def test_planner_routes_free_cash_flow_to_gov_state(self) -> None:
        plan = planner.plan_query("What state is highest on Free_Cash_Flow in Government Finances?")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.table_names, ["gov_state"])
        self.assertIn("Free_Cash_Flow", plan.sql)
        self.assertNotIn("state_flow", plan.sql)

    def test_planner_filters_maryland_congressional_districts(self) -> None:
        plan = planner.plan_query("Within Maryland congressional districts, which district is highest on Free_Cash_Flow?")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.table_names, ["gov_congress"])
        self.assertIn("UPPER(cd_118) LIKE 'MD-%'", plan.sql)
        self.assertIn("Free_Cash_Flow", plan.sql)

    def test_planner_builds_state_position_lookup_with_top_and_bottom_lists(self) -> None:
        plan = planner.plan_query(
            "Where does Maryland rank nationally for debt ratio, and what are the top 10 and bottom 10 states?"
        )
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.table_names, ["gov_state"])
        self.assertIn("row_kind", plan.sql)
        self.assertIn("nearby_rows", plan.sql)
        self.assertIn("top_rows", plan.sql)
        self.assertIn("bottom_rows", plan.sql)
        self.assertIn("UNION ALL", plan.sql)
        self.assertIn("LOWER(state) = 'maryland'", plan.sql)
        self.assertIn("ORDER BY Debt_Ratio DESC", plan.sql)

    def test_planner_leaderboard_sql_executes(self) -> None:
        from app.db import execute_query

        plan = planner.plan_query(
            "Where does Maryland rank nationally for debt ratio, and what are the top 10 and bottom 10 states?"
        )
        self.assertIsNotNone(plan)
        assert plan is not None
        df = execute_query(plan.sql)
        self.assertGreaterEqual(len(df), 21)
        self.assertIn("row_kind", df.columns)

    def test_planner_maps_jobs_to_employees_for_agency_questions(self) -> None:
        plan = planner.plan_query("For Virginia in 2024, which agencies dominate jobs?")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.table_names, ["spending_state_agency"])
        self.assertIn("Employees", plan.sql)
        self.assertNotIn("spending_total", plan.sql)

    def test_planner_uses_explicit_federal_period_label(self) -> None:
        plan = planner.plan_query("In the 2020-2024 period, which state is highest on Contracts?")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.table_names, ["contract_state"])
        self.assertIn("year = '2020-2024'", plan.sql)

    def test_planner_builds_position_lookup_for_single_state_followup(self) -> None:
        plan = planner.plan_query("Which state has the highest total liabilities? For Mississippi, show the current value and rank.")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.table_names, ["gov_state"])
        self.assertIn("metric_rank", plan.sql)
        self.assertIn("total_states", plan.sql)
        self.assertIn("LOWER(state) = 'mississippi'", plan.sql)

    def test_planner_returns_data_not_available_for_missing_agency_geo_runtime(self) -> None:
        plan = planner.plan_query("Within Maryland counties in 2024, which counties have the highest Department of Defense contracts?")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertIn("DATA_NOT_AVAILABLE", plan.sql)


if __name__ == "__main__":
    unittest.main()
