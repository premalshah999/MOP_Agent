from __future__ import annotations

from app.core import reasoning_agent


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

    monkeypatch.setattr(reasoning_agent.client, "chat_tools", fake_chat_tools)
    monkeypatch.setattr(reasoning_agent, "execute_tool", fake_execute_tool)

    result = reasoning_agent.run_reasoning_agent(
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
