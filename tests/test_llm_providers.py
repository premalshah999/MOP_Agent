from __future__ import annotations

import io
import json

import pytest

from app.core import reasoning
from app.llm import client

_ENV_VARS = (
    "LLM_PROVIDER",
    "LLM_BASE_URL",
    "LLM_MODEL",
    "DEEPSEEK_API_KEY",
    "DEEPSEEK_MODEL",
    "DEEPSEEK_THINKING_MODE",
    "DEEPSEEK_REASONING_EFFORT",
    "DEEPSEEK_MIN_COMPLETION_TOKENS",
    "GEMINI_API_KEY",
    "GEMINI_MODEL",
    "GEMINI_REASONING_EFFORT",
    "GEMINI_MIN_COMPLETION_TOKENS",
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
    "ASSISTANT_ROUTER_BASE_URL",
    "ASSISTANT_ROUTER_MODEL",
)


def _clear_provider_env(monkeypatch) -> None:
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_explicit_gemini_selection_does_not_silently_use_deepseek(monkeypatch) -> None:
    _clear_provider_env(monkeypatch)
    monkeypatch.setenv("LLM_PROVIDER", "gemini")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "deepseek-key")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-key")
    monkeypatch.setenv("ASSISTANT_ROUTER_BASE_URL", "https://api.deepseek.com")
    monkeypatch.setenv("ASSISTANT_ROUTER_MODEL", "deepseek-chat")
    config = client.provider_config()
    assert config.name == "gemini"
    assert config.key == "gemini-key"
    assert config.model == "gemini-3.5-flash-lite"
    assert config.base_url == "https://generativelanguage.googleapis.com/v1beta"


def test_auto_mode_preserves_legacy_provider_precedence(monkeypatch) -> None:
    _clear_provider_env(monkeypatch)
    monkeypatch.setenv("LLM_PROVIDER", "auto")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "deepseek-key")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-key")
    assert client.provider_config().name == "deepseek"
    monkeypatch.delenv("DEEPSEEK_API_KEY")
    assert client.provider_config().name == "gemini"


def test_provider_rejects_unsafe_base_urls(monkeypatch) -> None:
    _clear_provider_env(monkeypatch)
    monkeypatch.setenv("LLM_PROVIDER", "deepseek")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "deepseek-key")
    monkeypatch.setenv("LLM_BASE_URL", "file:///tmp/provider-response.json")
    with pytest.raises(client.LLMError, match=r"absolute HTTP\(S\) URL"):
        client.provider_config()

    monkeypatch.setenv("LLM_BASE_URL", "http://provider.internal/v1")
    monkeypatch.setenv("APP_ENV", "production")
    with pytest.raises(client.LLMError, match="must use HTTPS"):
        client.provider_config()


def test_deepseek_default_is_explicit_v4_and_legacy_alias_warns(monkeypatch) -> None:
    _clear_provider_env(monkeypatch)
    monkeypatch.setenv("LLM_PROVIDER", "deepseek")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "deepseek-key")
    assert client.provider_config().model == "deepseek-v4-flash"
    assert client.provider_warnings() == []

    monkeypatch.setenv("DEEPSEEK_MODEL", "deepseek-chat")
    assert "retired legacy alias" in client.provider_warnings()[0]


def test_deepseek_thinking_options_are_explicit_and_budgeted(monkeypatch) -> None:
    _clear_provider_env(monkeypatch)
    monkeypatch.setenv("LLM_PROVIDER", "deepseek")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "deepseek-key")
    monkeypatch.setenv("DEEPSEEK_THINKING_MODE", "enabled")
    monkeypatch.setenv("DEEPSEEK_REASONING_EFFORT", "high")
    monkeypatch.setenv("DEEPSEEK_MIN_COMPLETION_TOKENS", "4096")

    max_tokens, options = client._request_options(client.provider_config(), 900)

    assert max_tokens == 4096
    assert options == {"thinking": {"type": "enabled"}, "reasoning_effort": "high"}


def test_request_fingerprint_tracks_prompt_and_model_without_exposing_text(monkeypatch) -> None:
    _clear_provider_env(monkeypatch)
    monkeypatch.setenv("LLM_PROVIDER", "deepseek")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "deepseek-key")
    config = client.provider_config()
    common = {
        "json_mode": True,
        "temperature": 0.0,
        "max_tokens": 900,
        "purpose": "planner",
    }
    first = client.request_fingerprint(
        config, [{"role": "user", "content": "private question one"}], **common
    )
    same = client.request_fingerprint(
        config, [{"role": "user", "content": "private question one"}], **common
    )
    changed = client.request_fingerprint(
        config, [{"role": "user", "content": "private question two"}], **common
    )

    assert first == same
    assert first != changed
    assert "private" not in first


def test_json_repair_raises_small_completion_budget(monkeypatch) -> None:
    responses = iter(['{"value":', '{"value": 42}'])
    calls: list[dict] = []

    def fake_chat(messages, **kwargs):
        calls.append(kwargs)
        return next(responses)

    monkeypatch.setattr(client, "chat", fake_chat)
    monkeypatch.setattr(client, "is_live", lambda: True)
    assert client.chat_json(
        [{"role": "user", "content": "return JSON"}],
        max_tokens=250,
        purpose="test",
    ) == {"value": 42}
    assert calls[0]["max_tokens"] == 250
    assert calls[1]["max_tokens"] == 1024


def test_gemini_request_options_protect_thinking_json_budget(monkeypatch) -> None:
    _clear_provider_env(monkeypatch)
    monkeypatch.setenv("LLM_PROVIDER", "gemini")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-key")
    monkeypatch.setenv("GEMINI_REASONING_EFFORT", "low")
    body = client._gemini_request_body(
        client.provider_config(),
        [{"role": "user", "content": "return JSON"}],
        json_mode=True,
        temperature=0,
        max_tokens=250,
    )
    assert body["generationConfig"] == {
        "temperature": 0,
        "maxOutputTokens": 2048,
        "responseMimeType": "application/json",
        "thinkingConfig": {"thinkingLevel": "LOW"},
    }


def test_gemini_sql_generation_uses_response_json_schema(monkeypatch) -> None:
    _clear_provider_env(monkeypatch)
    monkeypatch.setenv("LLM_PROVIDER", "gemini")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-key")
    body = client._gemini_request_body(
        client.provider_config(),
        [{"role": "user", "content": "write SQL"}],
        json_mode=True,
        temperature=0,
        max_tokens=1400,
        purpose="stage4_sql",
    )

    schema = body["generationConfig"]["responseJsonSchema"]
    assert schema["required"] == ["sql"]
    assert schema["properties"]["sql"] == {"type": "string"}


def test_gemini_tool_response_preserves_thought_signature(monkeypatch) -> None:
    _clear_provider_env(monkeypatch)
    monkeypatch.setenv("LLM_PROVIDER", "gemini")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-key")
    monkeypatch.setenv("GEMINI_REASONING_EFFORT", "low")
    parts = [
        {
            "functionCall": {"name": "get_schema", "args": {"table": "contract_state"}},
            "thoughtSignature": "signed-thought",
        }
    ]
    payload = {
        "candidates": [{"content": {"role": "model", "parts": parts}}],
        "usageMetadata": {
            "promptTokenCount": 10,
            "candidatesTokenCount": 20,
            "thoughtsTokenCount": 5,
            "totalTokenCount": 35,
        },
    }

    class Response(io.StringIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

    captured: dict = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["body"] = json.loads(request.data)
        return Response(json.dumps(payload))

    monkeypatch.setattr(client.urllib.request, "urlopen", fake_urlopen)
    result = client.chat_tools(
        [{"role": "user", "content": "inspect the schema"}],
        tools=[{"type": "function", "function": {"name": "get_schema", "parameters": {}}}],
        max_tokens=300,
    )
    assert captured["url"].endswith("/v1beta/models/gemini-3.5-flash-lite:generateContent")
    assert captured["body"]["generationConfig"]["maxOutputTokens"] == 2048
    assert captured["body"]["toolConfig"] == {"functionCallingConfig": {"mode": "AUTO"}}
    assert result["assistant_message"]["gemini_parts"][0]["thoughtSignature"] == ("signed-thought")
    assert result["usage"] == {
        "prompt_tokens": 10,
        "completion_tokens": 25,
        "total_tokens": 35,
    }


def test_deepseek_thinking_tool_response_preserves_reasoning_content(monkeypatch) -> None:
    _clear_provider_env(monkeypatch)
    monkeypatch.setenv("LLM_PROVIDER", "deepseek")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "deepseek-key")
    monkeypatch.setenv("DEEPSEEK_THINKING_MODE", "enabled")
    payload = {
        "id": "response-1",
        "model": "deepseek-v4-flash",
        "choices": [
            {
                "finish_reason": "tool_calls",
                "message": {
                    "role": "assistant",
                    "content": "",
                    "reasoning_content": "I should inspect the schema.",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {
                                "name": "get_schema",
                                "arguments": '{"table":"contract_state"}',
                            },
                        }
                    ],
                },
            }
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30},
    }

    class Response(io.StringIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

    captured: dict = {}

    def fake_urlopen(request, timeout):
        captured["body"] = json.loads(request.data)
        return Response(json.dumps(payload))

    monkeypatch.setattr(client.urllib.request, "urlopen", fake_urlopen)
    result = client.chat_tools(
        [{"role": "user", "content": "inspect the schema"}],
        tools=[{"type": "function", "function": {"name": "get_schema", "parameters": {}}}],
        max_tokens=300,
        purpose="reasoning_agent",
    )

    assert captured["body"]["thinking"] == {"type": "enabled"}
    assert captured["body"]["reasoning_effort"] == "high"
    assert "temperature" not in captured["body"]
    assert result["assistant_message"]["reasoning_content"] == ("I should inspect the schema.")


def test_reasoning_loop_sends_provider_assistant_message_back_unchanged(monkeypatch) -> None:
    signature = {"google": {"thought_signature": "signed-thought"}}
    calls: list[list[dict]] = []

    def fake_chat_tools(messages, **kwargs):
        calls.append([dict(message) for message in messages])
        if len(calls) == 1:
            raw_tool_call = {
                "id": "call-1",
                "type": "function",
                "function": {"name": "get_schema", "arguments": '{"table":"contract_state"}'},
                "extra_content": signature,
            }
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-1",
                        "name": "get_schema",
                        "arguments": {"table": "contract_state"},
                        "arguments_str": '{"table":"contract_state"}',
                    }
                ],
                "assistant_message": {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [raw_tool_call],
                },
                "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
            }
        return {
            "content": "",
            "tool_calls": [
                {
                    "id": "call-2",
                    "name": "answer",
                    "arguments": {
                        "text": "The schema is available.",
                        "key_numbers": [],
                        "caveats": [],
                        "primary_evidence_id": "",
                        "supporting_evidence_ids": [],
                    },
                    "arguments_str": "{}",
                }
            ],
            "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
        }

    monkeypatch.setattr(reasoning.client, "chat_tools", fake_chat_tools)
    monkeypatch.setattr(
        reasoning,
        "execute_tool",
        lambda name, args, **kwargs: {"table": args["table"], "schema": "Employees"},
    )
    result = reasoning.run_reasoning_agent("Inspect contract_state", max_calls=2)
    assert result["stopped_reason"] == "ok"
    returned_assistant = calls[1][2]
    assert returned_assistant["tool_calls"][0]["extra_content"] == signature
