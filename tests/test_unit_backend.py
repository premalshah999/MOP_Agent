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
from app import prompts  # noqa: E402
from app import planner  # noqa: E402


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
            "spending_total": [36.2, 31.8, 29.1],
        })
        result = formatter.format_result("Which agencies account for the most spending in Maryland?", df)
        self.assertIn("HHS", result)
        self.assertIn("spending_total", result)

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
                with patch("app.agent.route_tables", return_value=["gov_state"]):
                    with patch("app.agent._generate_sql", return_value="SELECT 1 UNION ALL SELECT 1, 2"):
                        with patch(
                            "app.agent._execute_with_repair",
                            return_value=(None, "SELECT ...", "Set operations can only apply to expressions with same column count"),
                        ):
                            result = agent.ask_agent("Compare things", [])

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


if __name__ == "__main__":
    unittest.main()
