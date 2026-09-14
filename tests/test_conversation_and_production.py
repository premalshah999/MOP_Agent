from __future__ import annotations

import hashlib

import pytest
from starlette.requests import Request

from app.core import conversation as conversation_module
from app.core import pipeline
from app.core.analysis_plan import AnalysisContract
from app.core.clarifier import generate_clarification_chips
from app.core.conversation import structured_memory
from app.core.reasoning import _build_user
from app.evals.conversation_eval import Turn, _grade


@pytest.mark.parametrize(
    ("tables", "metrics", "geography", "operation", "flow_direction", "resolved"),
    [
        (
            ["gov_state"],
            ["Total_Liabilities"],
            "state",
            "lookup",
            "none",
            {"gov_state": {"State": {"value": "Maryland"}}},
        ),
        (
            ["acs_county"],
            ["Below poverty"],
            "county",
            "ranking",
            "none",
            {"acs_county": {"state": {"value": "maryland"}}},
        ),
        (
            ["contract_state"],
            ["Grants"],
            "state",
            "comparison",
            "none",
            {"contract_state": {"state": {"values": ["MARYLAND", "VIRGINIA"]}}},
        ),
        (
            ["spending_state_agency"],
            ["contract"],
            "state",
            "breakdown",
            "none",
            {
                "spending_state_agency": {
                    "state_name": {"value": "Maryland"},
                    "agency_name": {"value": "Department of Defense"},
                }
            },
        ),
        (
            ["finra_state"],
            ["financial_literacy"],
            "state",
            "lookup",
            "none",
            {"finra_state": {"state": {"value": "New York"}}},
        ),
        (
            ["state_flow"],
            ["subaward_amount_year"],
            "state",
            "aggregate",
            "outflow",
            {"state_flow": {"rcpt_state_name": {"value": "Maryland"}}},
        ),
        (
            ["finra_state", "gov_state"],
            ["financial_literacy", "Debt_Ratio"],
            "state",
            "correlation",
            "none",
            {},
        ),
    ],
)
def test_context_memory_covers_every_dataset_family(
    tables, metrics, geography, operation, flow_direction, resolved
):
    analysis = AnalysisContract(
        tables=tables,
        metric_columns=metrics,
        geography_level=geography,
        operation=operation,
        flow_direction=flow_direction,
        effective_period=2023,
        top_k=10 if operation == "ranking" else None,
    )
    memory = pipeline._build_context_memory("standalone question", analysis, resolved)
    assert memory["standalone_question"] == "standalone question"
    assert memory["tables"] == tables
    assert memory["metrics"] == metrics
    assert memory["operation"] == operation
    assert memory["flow_direction"] == flow_direction
    if resolved:
        assert memory["filters"]


def test_structured_memory_ignores_answer_prose_and_survives_clarification():
    history = [
        {"role": "user", "content": "Compare Maryland and Virginia grants"},
        {
            "role": "assistant",
            "content": "Incorrect generated prose must never become memory: $999B",
            "contract": {
                "supported": True,
                "context_memory": {
                    "standalone_question": "Compare Maryland and Virginia grants in 2024",
                    "tables": ["contract_state"],
                    "metrics": ["Grants"],
                    "comparison_entities": ["MARYLAND", "VIRGINIA"],
                    "period": 2024,
                },
            },
        },
        {
            "role": "assistant",
            "content": "Which measure should I use?",
            "contract": {"contract_type": "CLARIFY", "resolution": "needs_clarification"},
        },
    ]
    memory = structured_memory(history)
    assert "MARYLAND" in memory and "VIRGINIA" in memory
    assert "Grants" in memory and "2024" in memory
    assert "$999B" not in memory


def test_contextualizer_can_resolve_numbered_assistant_option(monkeypatch):
    captured: dict = {}

    def fake_chat(messages, **kwargs):
        captured["prompt"] = messages[-1]["content"]
        captured["system"] = messages[0]["content"]
        captured["temperature"] = kwargs["temperature"]
        return {"standalone_question": "How much in federal grants did Maryland receive in FY2024?"}

    monkeypatch.setattr(conversation_module.client, "chat_json", fake_chat)
    history = [
        {"role": "user", "content": "How much federal money goes to Maryland?"},
        {
            "role": "assistant",
            "content": "Which funding measure should I use?",
            "contract": {"contract_type": "CLARIFY", "resolution": "needs_clarification"},
            "suggested_followups": [
                "How much in federal contracts did Maryland receive in FY2024?",
                "How much in federal grants did Maryland receive in FY2024?",
            ],
        },
    ]
    result = conversation_module.contextualize("the second one", history)
    assert result == "How much in federal grants did Maryland receive in FY2024?"
    assert "assistant options" in captured["prompt"]
    assert "the second one" in captured["prompt"]
    assert captured["temperature"] == 0.0


def test_contextualizer_prompt_distinguishes_substitution_from_comparison(monkeypatch):
    captured: dict = {}

    def fake_chat(messages, **kwargs):
        captured["system"] = messages[0]["content"]
        return {
            "latest_is_standalone": False,
            "followup_mode": "SUBSTITUTE",
            "standalone_question": "What was Virginia's poverty rate in 2023?",
            "context_added": ["metric", "period"],
        }

    monkeypatch.setattr(conversation_module.client, "chat_json", fake_chat)
    history = [
        {"role": "user", "content": "What was Maryland's poverty rate in 2023?"},
        {
            "role": "assistant",
            "content": "Prior analytical answer",
            "contract": {
                "supported": True,
                "context_memory": {
                    "tables": ["acs_state"],
                    "metrics": ["Below poverty"],
                    "entities": ["maryland"],
                    "period": 2023,
                    "operation": "lookup",
                },
            },
        },
    ]

    assert conversation_module.contextualize("what about Virginia?", history) == (
        "What was Virginia's poverty rate in 2023?"
    )
    assert "SUBSTITUTE replaces one value" in captured["system"]
    assert "does NOT mean compare Maryland" in captured["system"]


def test_contextualizer_does_not_merge_complete_new_request(monkeypatch):
    def fake_chat(messages, **kwargs):
        return {
            "latest_is_standalone": True,
            "standalone_question": "sort the Wyoming counties by Black and Asian population",
            "context_added": [],
        }

    monkeypatch.setattr(conversation_module.client, "chat_json", fake_chat)
    history = [
        {"role": "user", "content": "Correlate Asian population with median income by state"},
        {
            "role": "assistant",
            "content": "Prior analytical answer",
            "contract": {
                "supported": True,
                "context_memory": {
                    "tables": ["acs_county"],
                    "metrics": ["Asian", "Median household income"],
                    "operation": "correlation",
                },
            },
        },
    ]
    latest = "sort the Wyoming counties by Black and Asian population"
    assert conversation_module.contextualize(latest, history) == latest


def test_contextualizer_keeps_narrower_arithmetic_followup_narrow(monkeypatch):
    captured: dict = {}

    def fake_chat(messages, **kwargs):
        captured["prompt"] = messages[-1]["content"]
        captured["system"] = messages[0]["content"]
        return {
            "latest_is_standalone": True,
            "standalone_question": "Which Alaska county has the highest total assets minus total liabilities?",
            "context_added": [],
        }

    monkeypatch.setattr(conversation_module.client, "chat_json", fake_chat)
    history = [
        {
            "role": "user",
            "content": (
                "Which Alaska county has the highest combined total-assets and "
                "current-assets net differences?"
            ),
        },
        {
            "role": "assistant",
            "content": "Prior analytical answer",
            "contract": {
                "supported": True,
                "context_memory": {
                    "tables": ["gov_county"],
                    "metrics": [
                        "Total_Assets",
                        "Total_Liabilities",
                        "Current_Assets",
                        "Current_Liabilities",
                    ],
                    "focus_state": "alaska",
                    "operation": "ranking",
                },
            },
        },
    ]
    latest = "Which Alaska county has the highest total assets minus total liabilities?"
    assert conversation_module.contextualize(latest, history) == latest
    assert "omits a term" in captured["system"]


def test_result_context_keeps_verified_row_identities_only():
    memory = {"tables": ["acs_county"], "metrics": ["Below poverty"]}
    enriched = pipeline._add_result_context(
        memory,
        [
            {"state": "maryland", "county": "montgomery", "poverty": 6.8},
            {"state": "maryland", "county": "allegany", "poverty": 15.1},
        ],
        ["acs_county"],
    )
    assert enriched["result_entities"] == [
        {"state": "maryland", "county": "montgomery"},
        {"state": "maryland", "county": "allegany"},
    ]
    assert enriched["result_row_count"] == 2
    assert all("poverty" not in row for row in enriched["result_entities"])


def test_reasoning_prompt_receives_rich_memory_without_answer_prose():
    history = [
        {"role": "user", "content": "top Maryland counties by poverty"},
        {
            "role": "assistant",
            "content": "Do not feed this prose back",
            "contract": {
                "supported": True,
                "context_memory": {
                    "tables": ["acs_county"],
                    "metrics": ["Below poverty"],
                    "focus_state": "maryland",
                    "period": 2023,
                    "operation": "ranking",
                    "top_k": 10,
                },
            },
        },
    ]
    prompt = _build_user("Which have the most grants?", history, operation="ranking")
    assert "acs_county" in prompt and "Below poverty" in prompt and "top_k = 10" in prompt
    assert "Do not feed this prose back" not in prompt
    assert "current QUESTION overrides" in prompt


def test_router_outage_is_an_error_not_a_fake_clarification(monkeypatch):
    monkeypatch.setattr(pipeline, "contextualize", lambda question, history: question)
    monkeypatch.setattr(
        pipeline,
        "classify_and_route",
        lambda question, history: {
            "intent": "CLARIFY",
            "requires_sql": False,
            "needs_clarification": True,
            "clarification_question": "Could you rephrase?",
            "reason": "provider unavailable",
            "service_unavailable": True,
        },
    )
    result = pipeline.answer_question("How much grant funding did Maryland receive?")
    assert result["resolution"] == "error"
    assert result["contract"]["contract_type"] == "ERROR"
    assert "temporarily unavailable" in result["answer"]
    assert "suggested_followups" not in result


def test_federal_clarification_options_use_available_2024_period():
    chips = generate_clarification_chips("How much federal money goes to Maryland?")
    assert chips
    assert all("FY2023" not in chip for chip in chips)
    assert any("FY2024" in chip for chip in chips)


def test_conversation_gate_grades_full_multi_entity_contract():
    expected = Turn(
        "compare that with Virginia's outflow",
        {"state_flow"},
        {"subaward amount year"},
        {"maryland", "virginia"},
        operation="comparison",
        flow_direction="outflow",
    )
    result = {
        "resolution": "answered",
        "contract": {
            "context_memory": {
                "tables": ["state_flow"],
                "metrics": ["subaward_amount_year"],
                "entities": ["Maryland", "Virginia"],
                "operation": "comparison",
                "flow_direction": "outflow",
            }
        },
    }
    assert _grade(result, expected) == []


def test_legacy_password_hash_verifies_and_new_hash_records_iterations():
    from app.api import auth

    password = "secret123"
    salt = "0123456789abcdef"
    legacy_digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 120_000).hex()
    assert auth._verify_password(password, f"pbkdf2_sha256${salt}${legacy_digest}")
    current = auth._hash_password(password)
    assert current.startswith("pbkdf2_sha256$600000$")
    assert auth._verify_password(password, current)


def test_production_config_rejects_placeholders(monkeypatch):
    from app.main import _validate_production_config

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("JWT_SECRET", "change-me-before-public-use")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "replace-with-deepseek-key")
    monkeypatch.setenv("ALLOWED_ORIGINS", "https://your-domain.example")
    monkeypatch.setenv("TRUSTED_HOSTS", "your-domain.example")
    with pytest.raises(RuntimeError, match="Invalid production configuration"):
        _validate_production_config()


def test_production_config_accepts_explicit_gemini_provider(monkeypatch):
    from app.main import _validate_production_config

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("JWT_SECRET", "a-secure-test-secret-that-is-at-least-32-characters")
    monkeypatch.setenv("LLM_PROVIDER", "gemini")
    monkeypatch.setenv("GEMINI_API_KEY", "a-valid-looking-gemini-test-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "replace-with-deepseek-key")
    monkeypatch.setenv("ALLOWED_ORIGINS", "https://analytics.example.edu")
    monkeypatch.setenv("TRUSTED_HOSTS", "analytics.example.edu")
    monkeypatch.setenv("DEBUG_ERRORS", "false")
    _validate_production_config()


def test_production_config_rejects_debug_errors_and_invalid_limits(monkeypatch):
    from app.main import _validate_production_config

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("JWT_SECRET", "a-secure-test-secret-that-is-at-least-32-characters")
    monkeypatch.setenv("LLM_PROVIDER", "gemini")
    monkeypatch.setenv("GEMINI_API_KEY", "a-valid-looking-gemini-test-key")
    monkeypatch.setenv("ALLOWED_ORIGINS", "https://analytics.example.edu")
    monkeypatch.setenv("TRUSTED_HOSTS", "analytics.example.edu")
    monkeypatch.setenv("DEBUG_ERRORS", "true")
    monkeypatch.setenv("QUERY_TIMEOUT_SECONDS", "0")

    with pytest.raises(RuntimeError, match="DEBUG_ERRORS.*QUERY_TIMEOUT_SECONDS"):
        _validate_production_config()


def test_server_history_takes_precedence_over_legacy_client_history():
    from app.main import AskRequest, _effective_history

    body = AskRequest(
        question="What about Virginia?",
        history=[{"role": "user", "content": "untrusted stale context"}],
    )
    stored = [
        {
            "role": "assistant",
            "content": "Verified answer",
            "contract": {"context_memory": {"tables": ["contract_state"]}},
        }
    ]
    assert _effective_history(body, stored) is stored
    assert _effective_history(body, []) == [{"role": "user", "content": "untrusted stale context"}]


def _request(path: str, *headers: tuple[str, str]) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "https",
            "path": path,
            "query_string": b"",
            "headers": [(key.lower().encode(), value.encode()) for key, value in headers],
            "client": ("172.18.0.1", 1234),
            "server": ("testserver", 443),
        }
    )


def test_rate_limit_key_isolates_authenticated_users_behind_shared_nat():
    from app.main import _rate_limit_key

    first = _request("/api/ask", ("X-Real-IP", "203.0.113.10"), ("Authorization", "Bearer one"))
    second = _request("/api/ask", ("X-Real-IP", "203.0.113.10"), ("Authorization", "Bearer two"))

    assert _rate_limit_key(first).startswith("user:")
    assert _rate_limit_key(first) != _rate_limit_key(second)


def test_auth_rate_limit_remains_ip_scoped_and_prefers_real_ip():
    from app.main import _rate_limit_key

    request = _request(
        "/api/auth/login",
        ("X-Real-IP", "203.0.113.10"),
        ("X-Forwarded-For", "198.51.100.8"),
        ("Authorization", "Bearer ignored-on-auth-routes"),
    )

    assert _rate_limit_key(request) == "ip:203.0.113.10"
