from __future__ import annotations

from app.core import reasoning, reasoning_tools
from app.core.analysis_plan import AnalysisContract, FormulaSpec
from app.core.pipeline import _cited_reasoning_tool_results, _reasoning_requires_final_shape


def _tool_call(step: int, name: str, arguments: dict) -> dict:
    import json

    return {
        "content": "",
        "tool_calls": [
            {
                "id": f"call-{step}",
                "name": name,
                "arguments": arguments,
                "arguments_str": json.dumps(arguments),
            }
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }


def test_aggregate_reasoning_keeps_scalar_as_primary_after_breakdown(monkeypatch) -> None:
    total_sql = "SELECT SUM(subaward_amount_year) AS total_outflow FROM mart_state_flow"
    breakdown_sql = (
        "SELECT subawardee_state_name, SUM(subaward_amount_year) AS outflow "
        "FROM mart_state_flow GROUP BY subawardee_state_name"
    )
    responses = iter(
        [
            _tool_call(1, "run_sql", {"sql": total_sql}),
            _tool_call(2, "run_sql", {"sql": breakdown_sql}),
            _tool_call(
                3,
                "answer",
                {
                    "text": "Maryland's total outflow is **$25.11B**.",
                    "key_numbers": [
                        {
                            "label": "Total Maryland outflow",
                            "value": 25_114_674_528.13,
                            "unit": "USD",
                        }
                    ],
                    "caveats": ["All available years."],
                    "primary_evidence_id": "E1",
                    "supporting_evidence_ids": [],
                },
            ),
        ]
    )
    seen_messages: list[list[dict]] = []

    def fake_chat_tools(messages, **kwargs):
        seen_messages.append(messages)
        return next(responses)

    def fake_execute_tool(name, args, **kwargs):
        assert name == "run_sql"
        if args["sql"] == total_sql:
            return {
                "sql": total_sql,
                "rows": [{"total_outflow": 25_114_674_528.13}],
                "row_count": 1,
            }
        return {
            "sql": breakdown_sql,
            "rows": [
                {"subawardee_state_name": "Tennessee", "outflow": 6_526_000_000},
                {"subawardee_state_name": "Maryland", "outflow": 3_997_000_000},
            ],
            "row_count": 2,
        }

    monkeypatch.setattr(reasoning.client, "chat_tools", fake_chat_tools)
    monkeypatch.setattr(reasoning, "execute_tool", fake_execute_tool)

    result = reasoning.run_reasoning_agent(
        "How much subcontract funding flows out of Maryland?",
        operation="aggregate",
        flow_direction="outflow",
    )

    assert result["sql"] == total_sql
    assert result["rows"] == [{"total_outflow": 25_114_674_528.13}]
    assert len(result["tool_results"]) == 2
    user_prompt = seen_messages[0][1]["content"]
    assert "operation=aggregate" in user_prompt
    assert "do not replace it" in user_prompt


def test_atomic_reasoning_results_require_complete_sql_shape() -> None:
    gap = AnalysisContract(
        tables=["contract_county"],
        metric_columns=["Grants"],
        operation="comparison",
        statistic="derived",
        result_unit="usd",
        formula=FormulaSpec(operator="subtract", operands=["Grants", "Grants"]),
        observation_grain="county",
        result_scope="single",
    )
    broad_comparison = AnalysisContract(
        tables=["contract_county"],
        metric_columns=["Grants"],
        operation="comparison",
        statistic="value",
        result_unit="usd",
        observation_grain="county",
        result_scope="grouped",
    )

    assert _reasoning_requires_final_shape(gap) is True
    assert _reasoning_requires_final_shape(broad_comparison) is False


def test_ranking_reasoning_keeps_primary_ranking_after_summary(monkeypatch) -> None:
    ranking_sql = "SELECT cd_118, Free_Cash_Flow FROM mart_gov_congress LIMIT 10"
    summary_sql = "SELECT COUNT(*) AS n FROM mart_gov_congress"
    responses = iter(
        [
            _tool_call(1, "run_sql", {"sql": ranking_sql}),
            _tool_call(2, "run_sql", {"sql": summary_sql}),
            _tool_call(
                3,
                "answer",
                {
                    "text": "NM-03 leads.",
                    "key_numbers": [],
                    "primary_evidence_id": "E1",
                    "supporting_evidence_ids": [],
                },
            ),
        ]
    )

    monkeypatch.setattr(
        reasoning.client,
        "chat_tools",
        lambda messages, **kwargs: next(responses),
    )

    def fake_execute_tool(name, args, **kwargs):
        if args["sql"] == ranking_sql:
            return {
                "sql": ranking_sql,
                "rows": [{"cd_118": "NM-03", "Free_Cash_Flow": 379_327_192}],
                "row_count": 1,
                "truncated": True,
            }
        return {"sql": summary_sql, "rows": [{"n": 408}], "row_count": 1}

    monkeypatch.setattr(reasoning, "execute_tool", fake_execute_tool)
    result = reasoning.run_reasoning_agent("Rank districts", operation="ranking")

    assert result["sql"] == ranking_sql
    assert result["rows"] == [{"cd_118": "NM-03", "Free_Cash_Flow": 379_327_192}]
    assert result["truncated"] is True


def test_large_tool_result_preserves_truncation_metadata_for_model() -> None:
    result = {
        "sql": "SELECT district, value FROM mart_gov_congress",
        "rows": [{"district": f"MD-{index:02d}", "value": index} for index in range(250)],
        "row_count": 250,
        "truncated": True,
    }

    serialized = reasoning._serialize_for_model(result, cap=1200)

    assert '"truncated": true' in serialized
    assert "executor result was capped" in serialized
    assert "MD-249" not in serialized


def test_peer_stats_is_exposed_as_visible_evidence(monkeypatch) -> None:
    peer_sql = (
        'SELECT state AS label, "Contracts" AS v FROM mart_contract_state '
        "WHERE year = '2024' ORDER BY \"Contracts\" DESC LIMIT 5"
    )
    responses = iter(
        [
            _tool_call(
                1,
                "peer_stats",
                {"table": "contract_state", "measure": "Contracts"},
            ),
            _tool_call(
                2,
                "answer",
                {
                    "text": "Texas ranks third.",
                    "key_numbers": [],
                    "primary_evidence_id": "E1",
                    "supporting_evidence_ids": [],
                },
            ),
        ]
    )
    monkeypatch.setattr(
        reasoning.client,
        "chat_tools",
        lambda messages, **kwargs: next(responses),
    )
    monkeypatch.setattr(
        reasoning,
        "execute_tool",
        lambda name, args, **kwargs: {
            "sql": peer_sql,
            "rows": [{"label": "TEXAS", "v": 58_779_865_425.12}],
            "row_count": 1,
            "stats": {"median": 7_226_407_498.27},
            "top5": [{"label": "TEXAS", "v": 58_779_865_425.12}],
            "bottom5": [],
        },
    )

    result = reasoning.run_reasoning_agent("Compare Texas to peers", operation="comparison")

    assert result["sql"] == peer_sql
    assert result["rows"] == [{"label": "TEXAS", "v": 58_779_865_425.12}]


def test_peer_stats_returns_named_entity_rank_as_primary_evidence(monkeypatch) -> None:
    def fake_execute(sql: str, max_rows: int):
        if sql.startswith("WITH ranked"):
            return [
                {
                    "label": "Maryland",
                    "v": 0.612,
                    "rank_asc": 29,
                    "rank_desc": 23,
                    "rank": 23,
                    "total": 51,
                }
            ]
        if "COUNT(*) AS n" in sql:
            return [{"n": 51, "median": 0.523}]
        if "DESC" in sql:
            return [{"label": "Missouri", "v": 0.954}]
        return [{"label": "Idaho", "v": 0.052}]

    monkeypatch.setattr(reasoning_tools, "execute_select", fake_execute)
    result = reasoning_tools.tool_peer_stats(
        "finra_state",
        "financial_constraint",
        where="Year = 2021",
        focus_value="Maryland",
        sort_direction="desc",
    )

    assert result["rows"][0]["label"] == "Maryland"
    assert result["rows"][0]["rank"] == 23
    assert result["rank_direction"] == "desc"
    assert result["focus"]["total"] == 51


def test_reasoning_injects_grounded_focus_into_peer_stats(monkeypatch) -> None:
    responses = iter(
        [
            _tool_call(
                1,
                "peer_stats",
                {
                    "table": "finra_state",
                    "measure": "financial_constraint",
                    "geo_column": "state",
                    "where": "Year = 2021",
                },
            ),
            _tool_call(
                2,
                "answer",
                {
                    "text": "Maryland ranks 23rd.",
                    "key_numbers": [],
                    "primary_evidence_id": "E1",
                    "supporting_evidence_ids": [],
                },
            ),
        ]
    )
    monkeypatch.setattr(
        reasoning.client,
        "chat_tools",
        lambda messages, **kwargs: next(responses),
    )

    def fake_execute(name, args, **kwargs):
        assert args["focus_value"] == "Maryland"
        assert args["sort_direction"] == "desc"
        return {
            "sql": "SELECT focused rank",
            "rows": [{"label": "Maryland", "v": 0.612, "rank": 23, "total": 51}],
            "row_count": 1,
            "stats": {"median": 0.523},
            "top5": [],
            "bottom5": [],
            "focus": {"label": "Maryland", "v": 0.612, "rank": 23, "total": 51},
        }

    monkeypatch.setattr(reasoning, "execute_tool", fake_execute)
    result = reasoning.run_reasoning_agent(
        "Where does Maryland rank?",
        operation="ranking",
        analysis_contract={"sort_direction": "desc"},
        focus_values_by_column={"state": ["Maryland"]},
    )

    assert result["rows"][0]["rank"] == 23
    assert result["sql"] == "SELECT focused rank"


def test_answer_evidence_reference_overrides_last_query_for_any_operation(monkeypatch) -> None:
    ranking_sql = "SELECT district, inflow_per_resident FROM mart_congress_flow ORDER BY 2 DESC"
    sample_sql = "SELECT * FROM mart_congress_flow WHERE state = 'Alaska' LIMIT 5"
    responses = iter(
        [
            _tool_call(1, "run_sql", {"sql": ranking_sql}),
            _tool_call(2, "run_sql", {"sql": sample_sql}),
            _tool_call(
                3,
                "answer",
                {
                    "text": "AL-05 leads at $3,350.76 per resident.",
                    "key_numbers": [],
                    "primary_evidence_id": "E1",
                    "supporting_evidence_ids": [],
                },
            ),
        ]
    )
    monkeypatch.setattr(reasoning.client, "chat_tools", lambda messages, **kwargs: next(responses))

    def fake_execute(name, args, **kwargs):
        if args["sql"] == ranking_sql:
            return {
                "sql": ranking_sql,
                "rows": [{"district": "AL-05", "inflow_per_resident": 3350.76}],
                "row_count": 1,
            }
        return {
            "sql": sample_sql,
            "rows": [{"district": "AK-00", "subaward_amount": 200.0}],
            "row_count": 1,
        }

    monkeypatch.setattr(reasoning, "execute_tool", fake_execute)
    result = reasoning.run_reasoning_agent(
        "Which congressional districts receive the most inflow per resident?",
        operation="comparison",
    )

    assert result["primary_evidence_id"] == "E1"
    assert result["sql"] == ranking_sql
    assert result["rows"] == [{"district": "AL-05", "inflow_per_resident": 3350.76}]
    assert result["tool_results"][0]["result"]["evidence_id"] == "E1"
    assert result["tool_results"][1]["result"]["evidence_id"] == "E2"


def test_verification_receives_only_answer_cited_evidence() -> None:
    agent = {
        "primary_evidence_id": "E1",
        "supporting_evidence_ids": ["E3"],
        "tool_results": [
            {"evidence_id": "E1", "name": "run_sql", "result": {"rows": [{"v": 1}]}},
            {"evidence_id": "E2", "name": "run_sql", "result": {"rows": [{"v": 999}]}},
            {"evidence_id": "E3", "name": "peer_stats", "result": {"rows": [{"v": 2}]}},
        ],
    }

    cited = _cited_reasoning_tool_results(agent)

    assert [item["evidence_id"] for item in cited] == ["E1", "E3"]


def test_budget_synthesis_cites_selected_primary_and_supporting_evidence(monkeypatch) -> None:
    primary_sql = "SELECT county, grants FROM mart_contract_county ORDER BY grants DESC"
    supporting_sql = "SELECT MAX(grants) - MIN(grants) AS gap FROM mart_contract_county"
    responses = iter(
        [
            _tool_call(1, "run_sql", {"sql": primary_sql}),
            _tool_call(2, "run_sql", {"sql": supporting_sql}),
        ]
    )
    monkeypatch.setattr(reasoning.client, "chat_tools", lambda messages, **kwargs: next(responses))

    def fake_execute(name, args, **kwargs):
        if args["sql"] == primary_sql:
            return {
                "sql": primary_sql,
                "rows": [{"county": "Montgomery", "grants": 7_740_000_000}],
                "row_count": 1,
            }
        return {
            "sql": supporting_sql,
            "rows": [{"gap": 7_731_450_000}],
            "row_count": 1,
        }

    monkeypatch.setattr(reasoning, "execute_tool", fake_execute)
    monkeypatch.setattr(
        reasoning,
        "write_answer",
        lambda *args, **kwargs: {
            "answer": "The gap is **$7.73B**.",
            "key_numbers": [],
            "caveats": [],
            "valid": True,
        },
    )

    result = reasoning.run_reasoning_agent(
        "What is the gap?",
        operation="comparison",
        max_calls=2,
    )

    assert result["stopped_reason"] == "budget_synthesised"
    assert result["sql"] == supporting_sql
    assert result["rows"] == [{"gap": 7_731_450_000}]
    assert result["primary_evidence_id"] == "E2"
    assert result["supporting_evidence_ids"] == ["E1"]


def test_congress_flow_analysis_adds_loaded_state_fips_bridge() -> None:
    from app.core.analysis_plan import build_analysis_contract

    contract = build_analysis_contract(
        "Rank congressional districts by subaward inflow per resident.",
        {
            "tables": ["congress_flow", "acs_congress"],
            "columns": ["subaward_amount", "Total population"],
            "geography_level": "congress",
            "year_strategy": "congress_flow: 2024; acs_congress: 2023",
            "semantic_plan": {
                "operation": "ranking",
                "statistic": "derived",
                "result_unit": "ratio",
                "formula": {
                    "operator": "divide",
                    "operands": ["subaward_amount", "Total population"],
                },
                "result_scope": "top_n",
                "top_k": 10,
                "flow_direction": "inflow",
                "observation_grain": "congress",
            },
        },
    )

    assert contract.tables == ["congress_flow", "acs_congress", "contract_congress"]
    assert contract.period_by_table["contract_congress"] == "2024"
