"""Provider-neutral chat client with offline modes.

Resolution order for every call:
  1. An injected stub (tests set one via set_stub) — highest priority, fully offline.
  2. A live HTTP call to DeepSeek, Gemini, or OpenAI when configured and
     LLM_MODE != "fixture". With LLM_RECORD=1 the response is saved as a fixture.
  3. A recorded fixture replay — keyed by a stable hash of (model, messages,
     json_mode). Lets CI run the golden suite with no API key.
  4. Otherwise raise LLMUnavailable so the gap is loud, never silently wrong.

No third-party dependencies (urllib only) to keep requirements.txt minimal.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from app.paths import ROOT_DIR


class LLMError(RuntimeError):
    """A live call failed or returned something unusable."""


class LLMUnavailable(LLMError):
    """No stub, no API key, and no recorded fixture for this exact request."""


Messages = list[dict[str, Any]]
_STUB: Callable[[Messages, bool, str], str] | None = None


# ---------------------------------------------------------------------------
# Test stub injection
# ---------------------------------------------------------------------------
def set_stub(handler: Callable[[Messages, bool, str], str] | None) -> None:
    """Install a deterministic offline handler: (messages, json_mode, purpose) -> str."""
    global _STUB
    _STUB = handler


def clear_stub() -> None:
    set_stub(None)


# ---------------------------------------------------------------------------
# Provider / mode resolution
# ---------------------------------------------------------------------------
_PROVIDERS = {"auto", "deepseek", "gemini", "openai"}
_DEFAULTS = {
    "deepseek": ("DEEPSEEK_API_KEY", "https://api.deepseek.com", "deepseek-v4-flash"),
    "gemini": (
        "GEMINI_API_KEY",
        "https://generativelanguage.googleapis.com/v1beta",
        "gemini-3.5-flash-lite",
    ),
    "openai": ("OPENAI_API_KEY", "https://api.openai.com/v1", "gpt-4o-mini"),
}


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    key: str | None
    base_url: str
    model: str


def provider_config() -> ProviderConfig:
    """Resolve one explicit provider; `auto` preserves legacy key precedence."""
    requested = (os.getenv("LLM_PROVIDER") or "auto").strip().casefold()
    if requested not in _PROVIDERS:
        raise LLMError(
            f"Unsupported LLM_PROVIDER={requested!r}; choose auto, deepseek, gemini, or openai"
        )
    auto_mode = requested == "auto"
    if auto_mode:
        requested = next(
            (
                name
                for name in ("deepseek", "gemini", "openai")
                if (os.getenv(_DEFAULTS[name][0]) or "").strip()
            ),
            "deepseek",
        )
    key_env, default_base, default_model = _DEFAULTS[requested]
    key = (os.getenv(key_env) or "").strip() or None
    base = (
        os.getenv("LLM_BASE_URL")
        or os.getenv(f"{requested.upper()}_BASE_URL")
        or (os.getenv("ASSISTANT_ROUTER_BASE_URL") if auto_mode else None)
        or default_base
    ).strip()
    parsed_base = urllib.parse.urlsplit(base)
    if parsed_base.scheme not in {"http", "https"} or not parsed_base.netloc:
        raise LLMError("The configured LLM base URL must be an absolute HTTP(S) URL")
    if parsed_base.username or parsed_base.password or parsed_base.query or parsed_base.fragment:
        raise LLMError(
            "The configured LLM base URL cannot contain credentials, a query, or a fragment"
        )
    if (
        os.getenv("APP_ENV", "development").strip().casefold() == "production"
        and parsed_base.scheme != "https"
    ):
        raise LLMError("The production LLM base URL must use HTTPS")
    model = (
        os.getenv("LLM_MODEL")
        or os.getenv(f"{requested.upper()}_MODEL")
        or (os.getenv("ASSISTANT_ROUTER_MODEL") if auto_mode else None)
        or default_model
    )
    return ProviderConfig(requested, key, base.rstrip("/"), model.strip())


def _provider() -> tuple[str | None, str, str]:
    """Backward-compatible tuple used by older diagnostics."""
    config = provider_config()
    return config.key, config.base_url, config.model


def active_provider() -> dict[str, str]:
    """Safe provider metadata for health/admin diagnostics (never the key)."""
    config = provider_config()
    return {"name": config.name, "base_url": config.base_url, "model": config.model}


def provider_warnings(config: ProviderConfig | None = None) -> list[str]:
    """Non-blocking operational warnings for provider aliases with known drift risk."""

    selected = config or provider_config()
    warnings: list[str] = []
    if selected.name == "deepseek" and selected.model in {"deepseek-chat", "deepseek-reasoner"}:
        warnings.append(
            f"{selected.model} is a retired legacy alias; validate and select an explicit "
            "DeepSeek V4 model before production deployment"
        )
    if selected.name == "gemini" and (
        selected.model.endswith("-latest")
        or "-preview" in selected.model
        or "-exp" in selected.model
    ):
        warnings.append(
            f"{selected.model} is not a fixed stable Gemini model and may change behavior"
        )
    return warnings


def is_live() -> bool:
    try:
        config = provider_config()
    except LLMError:
        return False
    return bool(config.key) and os.getenv("LLM_MODE", "").lower() != "fixture"


def _endpoint(base_url: str) -> str:
    return base_url if base_url.endswith("/chat/completions") else f"{base_url}/chat/completions"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
def _fixtures_path() -> Path:
    override = os.getenv("LLM_FIXTURES_PATH")
    return Path(override) if override else ROOT_DIR / "tests" / "fixtures" / "llm_fixtures.json"


def _load_fixtures() -> dict[str, Any]:
    path = _fixtures_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_fixture(key: str, content: str, purpose: str) -> None:
    path = _fixtures_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    data = _load_fixtures()
    data[key] = {"purpose": purpose, "content": content}
    path.write_text(json.dumps(data, indent=2, sort_keys=True))


def _fixture_key(model: str, messages: Messages, json_mode: bool) -> str:
    blob = json.dumps(
        {"model": model, "messages": messages, "json_mode": json_mode},
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def request_fingerprint(
    config: ProviderConfig,
    messages: Messages,
    *,
    json_mode: bool,
    temperature: float,
    max_tokens: int,
    purpose: str,
    tools: list[dict[str, Any]] | None = None,
) -> str:
    """Hash the complete model contract without persisting user or prompt text."""

    tool_contracts = [tool.get("function") or {} for tool in (tools or [])]
    blob = json.dumps(
        {
            "provider": config.name,
            "model": config.model,
            "messages": messages,
            "json_mode": json_mode,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "purpose": purpose,
            "tools": tool_contracts,
        },
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _record_llm_observation(
    *,
    config: ProviderConfig,
    messages: Messages,
    json_mode: bool,
    temperature: float,
    max_tokens: int,
    purpose: str,
    content: str,
    response_model: str = "",
    system_fingerprint: str = "",
    finish_reason: str = "",
    usage: dict[str, Any] | None = None,
    tools: list[dict[str, Any]] | None = None,
    latency_ms: int | None = None,
) -> None:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return
    from app.observability.logging import log_llm_event

    log_llm_event(
        {
            "purpose": purpose or "unspecified",
            "provider": config.name,
            "configured_model": config.model,
            "response_model": response_model or config.model,
            "system_fingerprint": system_fingerprint or None,
            "request_fingerprint": request_fingerprint(
                config,
                messages,
                json_mode=json_mode,
                temperature=temperature,
                max_tokens=max_tokens,
                purpose=purpose,
                tools=tools,
            ),
            "output_fingerprint": hashlib.sha256(content.encode("utf-8")).hexdigest(),
            "json_mode": json_mode,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "tool_names": [
                str((tool.get("function") or {}).get("name") or "") for tool in (tools or [])
            ],
            "finish_reason": finish_reason or None,
            "usage": usage or {},
            "latency_ms": latency_ms,
            "provider_warnings": provider_warnings(config),
        }
    )


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def _request_options(config: ProviderConfig, max_tokens: int) -> tuple[int, dict[str, Any]]:
    options: dict[str, Any] = {}
    if config.name == "deepseek" and config.model.startswith("deepseek-v4"):
        thinking = (os.getenv("DEEPSEEK_THINKING_MODE") or "disabled").strip().casefold()
        if thinking not in {"enabled", "disabled"}:
            raise LLMError("DEEPSEEK_THINKING_MODE must be enabled or disabled")
        options["thinking"] = {"type": thinking}
        if thinking == "enabled":
            effort = (os.getenv("DEEPSEEK_REASONING_EFFORT") or "high").strip().casefold()
            if effort not in {"low", "high", "max"}:
                raise LLMError("DEEPSEEK_REASONING_EFFORT must be low, high, or max")
            options["reasoning_effort"] = effort
            floor = int(os.getenv("DEEPSEEK_MIN_COMPLETION_TOKENS", "4096"))
            max_tokens = max(max_tokens, floor)
        return max_tokens, options
    if config.name != "gemini":
        return max_tokens, options
    # Gemini 3 thinking consumes completion tokens before the visible answer.
    # A small cap such as 250 can otherwise truncate valid JSON.
    if config.model.startswith("gemini-3"):
        floor = int(os.getenv("GEMINI_MIN_COMPLETION_TOKENS", "2048"))
        max_tokens = max(max_tokens, floor)
    return max_tokens, options


def _gemini_generation_config(
    config: ProviderConfig,
    *,
    json_mode: bool,
    temperature: float,
    max_tokens: int,
    response_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    max_tokens, _ = _request_options(config, max_tokens)
    generation: dict[str, Any] = {
        "temperature": temperature,
        "maxOutputTokens": max_tokens,
    }
    if json_mode:
        generation["responseMimeType"] = "application/json"
        if response_schema:
            generation["responseJsonSchema"] = response_schema
    effort = (os.getenv("GEMINI_REASONING_EFFORT") or "").strip().casefold()
    if effort:
        if effort not in {"none", "minimal", "low", "medium", "high"}:
            raise LLMError("GEMINI_REASONING_EFFORT must be none, minimal, low, medium, or high")
        # Native Gemini uses thinkingLevel. It has no fully disabled value for
        # Gemini 3, so `none` degrades explicitly to the documented minimum.
        level = "minimal" if effort == "none" else effort
        generation["thinkingConfig"] = {"thinkingLevel": level.upper()}
    return generation


def _message_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return ""
    if isinstance(value, list):
        texts = [str(part.get("text", "")) for part in value if isinstance(part, dict)]
        if any(texts):
            return "\n".join(text for text in texts if text)
    return json.dumps(value, default=str)


def _gemini_contents(messages: Messages) -> tuple[str, list[dict[str, Any]]]:
    """Translate the app's OpenAI-shaped history to native Gemini Content."""
    system_parts: list[str] = []
    contents: list[dict[str, Any]] = []
    call_names: dict[str, str] = {}
    pending_function_responses: list[dict[str, Any]] = []

    def flush_function_responses() -> None:
        if pending_function_responses:
            contents.append({"role": "user", "parts": list(pending_function_responses)})
            pending_function_responses.clear()

    for message in messages:
        role = str(message.get("role") or "user")
        if role == "system":
            text = _message_text(message.get("content"))
            if text:
                system_parts.append(text)
            continue
        if role == "tool":
            call_id = str(message.get("tool_call_id") or "")
            name = call_names.get(call_id) or str(message.get("name") or "tool")
            raw_result = _message_text(message.get("content"))
            try:
                result = json.loads(raw_result)
            except (json.JSONDecodeError, TypeError):
                result = {"result": raw_result}
            if not isinstance(result, dict):
                result = {"result": result}
            pending_function_responses.append(
                {"functionResponse": {"name": name, "response": result}}
            )
            continue

        flush_function_responses()
        native_role = "model" if role == "assistant" else "user"
        native_parts = message.get("gemini_parts")
        if isinstance(native_parts, list) and native_parts:
            parts = [dict(part) for part in native_parts if isinstance(part, dict)]
        else:
            parts: list[dict[str, Any]] = []
            text = _message_text(message.get("content"))
            if text:
                parts.append({"text": text})
            for index, tool_call in enumerate(message.get("tool_calls") or []):
                function = tool_call.get("function", {}) or {}
                args_raw = function.get("arguments") or "{}"
                try:
                    args = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
                except json.JSONDecodeError:
                    args = {}
                call_id = str(tool_call.get("id") or f"gemini-call-{index}")
                name = str(function.get("name") or "")
                call_names[call_id] = name
                parts.append({"functionCall": {"name": name, "args": args or {}}})
        for index, tool_call in enumerate(message.get("tool_calls") or []):
            function = tool_call.get("function", {}) or {}
            call_id = str(tool_call.get("id") or f"gemini-call-{index}")
            call_names[call_id] = str(function.get("name") or "")
        if parts:
            contents.append({"role": native_role, "parts": parts})

    flush_function_responses()
    return "\n\n".join(system_parts), contents


def _gemini_endpoint(config: ProviderConfig) -> str:
    base = config.base_url.removesuffix("/openai").rstrip("/")
    model = urllib.parse.quote(config.model, safe="-._")
    return f"{base}/models/{model}:generateContent"


def _gemini_error(exc: urllib.error.HTTPError) -> str:
    try:
        payload = json.loads(exc.read().decode("utf-8"))
        error = payload.get("error", {}) if isinstance(payload, dict) else {}
        return str(error.get("message") or exc.reason)
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        return str(exc.reason)


def _gemini_post(config: ProviderConfig, body: dict[str, Any], *, purpose: str) -> dict[str, Any]:
    if not config.key:
        raise LLMUnavailable("No Gemini API key configured")
    data = json.dumps(body).encode("utf-8")
    timeout = float(os.getenv("LLM_TIMEOUT", "60"))
    retries = int(os.getenv("LLM_RETRIES", "2"))
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        request = urllib.request.Request(
            _gemini_endpoint(config),
            data=data,
            headers={"Content-Type": "application/json", "x-goog-api-key": config.key},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            message = _gemini_error(exc)
            last_error = LLMError(f"Gemini HTTP {exc.code}: {message}")
            if exc.code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(2.0 * (attempt + 1))
                continue
            if exc.code == 429:
                raise LLMError(f"Gemini quota/rate limit exceeded: {message}") from exc
            if exc.code in (500, 502, 503, 504):
                raise LLMError(f"Gemini is temporarily unavailable: {message}") from exc
            raise last_error from exc
        except (urllib.error.URLError, TimeoutError, KeyError, ValueError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(2.0 * (attempt + 1))
                continue
    raise LLMError(f"Gemini {purpose} failed after {retries + 1} attempts: {last_error}")


def _gemini_request_body(
    config: ProviderConfig,
    messages: Messages,
    *,
    json_mode: bool,
    temperature: float,
    max_tokens: int,
    purpose: str = "",
) -> dict[str, Any]:
    system, contents = _gemini_contents(messages)
    body: dict[str, Any] = {
        "contents": contents,
        "generationConfig": _gemini_generation_config(
            config,
            json_mode=json_mode,
            temperature=temperature,
            max_tokens=max_tokens,
            response_schema=(
                {
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string"},
                        "explanation": {"type": "string"},
                    },
                    "required": ["sql"],
                    "additionalProperties": False,
                }
                if purpose.startswith("stage4_sql")
                else None
            ),
        ),
    }
    if system:
        body["systemInstruction"] = {"parts": [{"text": system}]}
    return body


def _gemini_parts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = payload.get("candidates") or []
    if not candidates:
        feedback = payload.get("promptFeedback") or {}
        raise LLMError(f"Gemini returned no candidates: {feedback}")
    content = candidates[0].get("content") or {}
    parts = content.get("parts") or []
    if not isinstance(parts, list):
        raise LLMError("Gemini returned an invalid content payload")
    return [dict(part) for part in parts if isinstance(part, dict)]


def _gemini_usage(payload: dict[str, Any]) -> dict[str, int]:
    usage = payload.get("usageMetadata") or {}
    prompt = int(usage.get("promptTokenCount") or 0)
    candidates = int(usage.get("candidatesTokenCount") or 0)
    thoughts = int(usage.get("thoughtsTokenCount") or 0)
    return {
        "prompt_tokens": prompt,
        "completion_tokens": candidates + thoughts,
        "total_tokens": int(usage.get("totalTokenCount") or prompt + candidates + thoughts),
    }


def _http_call(
    messages: Messages,
    *,
    json_mode: bool,
    temperature: float,
    max_tokens: int,
    purpose: str,
) -> str:
    started = time.perf_counter()
    config = provider_config()
    key, base, model = config.key, config.base_url, config.model
    if not key:
        raise LLMUnavailable(f"No API key configured for LLM_PROVIDER={config.name}")
    if config.name == "gemini":
        payload = _gemini_post(
            config,
            _gemini_request_body(
                config,
                messages,
                json_mode=json_mode,
                temperature=temperature,
                max_tokens=max_tokens,
                purpose=purpose,
            ),
            purpose="generation",
        )
        text = "".join(str(part.get("text") or "") for part in _gemini_parts(payload))
        if not text:
            raise LLMError("Gemini returned no text content")
        candidates = payload.get("candidates") or []
        candidate = candidates[0] if candidates and isinstance(candidates[0], dict) else {}
        _record_llm_observation(
            config=config,
            messages=messages,
            json_mode=json_mode,
            temperature=temperature,
            max_tokens=max_tokens,
            purpose=purpose,
            content=text,
            response_model=str(payload.get("modelVersion") or config.model),
            finish_reason=str(candidate.get("finishReason") or ""),
            usage=_gemini_usage(payload),
            latency_ms=round((time.perf_counter() - started) * 1000),
        )
        return text
    max_tokens, provider_options = _request_options(config, max_tokens)
    body: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": messages,
        **provider_options,
    }
    if not (
        config.name == "deepseek"
        and (provider_options.get("thinking") or {}).get("type") == "enabled"
    ):
        body["temperature"] = temperature
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    data = json.dumps(body).encode("utf-8")
    timeout = float(os.getenv("LLM_TIMEOUT", "60"))
    retries = int(os.getenv("LLM_RETRIES", "2"))
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        request = urllib.request.Request(
            _endpoint(base),
            data=data,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.load(response)
            choice = payload["choices"][0]
            content = choice["message"]["content"]
            _record_llm_observation(
                config=config,
                messages=messages,
                json_mode=json_mode,
                temperature=temperature,
                max_tokens=max_tokens,
                purpose=purpose,
                content=content,
                response_model=str(payload.get("model") or config.model),
                system_fingerprint=str(payload.get("system_fingerprint") or ""),
                finish_reason=str(choice.get("finish_reason") or ""),
                usage=payload.get("usage") or {},
                latency_ms=round((time.perf_counter() - started) * 1000),
            )
            return content
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code in (429, 500, 502, 503, 504) and attempt < retries:
                # Polite backoff: 2s, 4s — yields to the provider's own
                # rate-limit window before the next attempt.
                time.sleep(2.0 * (attempt + 1))
                continue
            if exc.code == 429:
                raise LLMError(
                    "The model is rate-limited right now. Please retry in a few seconds."
                ) from exc
            if exc.code in (500, 502, 503, 504):
                raise LLMError(
                    f"The model is temporarily unavailable (HTTP {exc.code}). Please retry."
                ) from exc
            raise LLMError(f"LLM HTTP {exc.code}: {exc.reason}") from exc
        except (urllib.error.URLError, TimeoutError, KeyError, ValueError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(2.0 * (attempt + 1))
                continue
    raise LLMError(f"LLM call failed after {retries + 1} attempts: {last_error}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def chat(
    messages: Messages,
    *,
    json_mode: bool = False,
    temperature: float = 0.0,
    max_tokens: int = 1024,
    purpose: str = "",
) -> str:
    """Return the assistant message content for `messages`."""
    if _STUB is not None:
        return _STUB(messages, json_mode, purpose)

    config = provider_config()
    model = config.model
    key = _fixture_key(model, messages, json_mode)

    if is_live():
        try:
            content = _http_call(
                messages,
                json_mode=json_mode,
                temperature=temperature,
                max_tokens=max_tokens,
                purpose=purpose,
            )
        except LLMError:
            fixtures = _load_fixtures()
            if key in fixtures:
                return fixtures[key]["content"]
            raise
        if os.getenv("LLM_RECORD", "").lower() in {"1", "true", "yes"}:
            _save_fixture(key, content, purpose)
        return content

    fixtures = _load_fixtures()
    if key in fixtures:
        return fixtures[key]["content"]
    raise LLMUnavailable(
        f"No API key, no stub, and no recorded fixture for purpose={purpose!r} "
        f"(key={key[:12]}…). Configure the selected provider key, inject a stub, or record fixtures "
        f"with LLM_RECORD=1."
    )


def chat_json(
    messages: Messages,
    *,
    temperature: float = 0.0,
    max_tokens: int = 1024,
    purpose: str = "",
) -> dict[str, Any]:
    """chat() in JSON mode, parsed. One repair retry on malformed JSON when live."""
    raw = chat(
        messages, json_mode=True, temperature=temperature, max_tokens=max_tokens, purpose=purpose
    )
    try:
        return json.loads(_strip_fences(raw))
    except json.JSONDecodeError as exc:
        if not is_live() or _STUB is not None:
            raise LLMError(f"Malformed JSON from LLM (purpose={purpose!r}): {raw[:200]}") from exc
        repair = list(messages) + [
            {"role": "assistant", "content": raw},
            {
                "role": "user",
                "content": "That was not valid JSON. Reply with ONLY a single valid JSON object.",
            },
        ]
        fixed = chat(
            repair,
            json_mode=True,
            temperature=0.0,
            # Malformed JSON is commonly a completion truncation. Repeating
            # with the same small cap guarantees a second truncation; give the
            # repair enough room to return one complete object.
            max_tokens=max(max_tokens, 1024),
            purpose=f"{purpose}:repair",
        )
        try:
            return json.loads(_strip_fences(fixed))
        except json.JSONDecodeError as exc2:
            raise LLMError(f"LLM returned malformed JSON twice (purpose={purpose!r}).") from exc2


def chat_tools(
    messages: Messages,
    tools: list[dict[str, Any]],
    *,
    tool_choice: str = "auto",
    temperature: float = 0.0,
    max_tokens: int = 1024,
    purpose: str = "",
) -> dict[str, Any]:
    """Provider-neutral tool calling.

    Returns {content: str|"", tool_calls: list[{id, name, arguments_str, arguments}],
             usage: {prompt_tokens, completion_tokens}}.

    Bypasses the stub/fixture path — tool-calling reasoning is a live-only feature
    by design (no deterministic stub for emergent multi-step behaviour).
    """
    started = time.perf_counter()
    config = provider_config()
    key, base, model = config.key, config.base_url, config.model
    if not key:
        raise LLMUnavailable(f"chat_tools requires a {config.name} API key")
    if config.name == "gemini":
        body = _gemini_request_body(
            config,
            messages,
            json_mode=False,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        declarations = [
            dict(tool.get("function") or {})
            for tool in tools
            if tool.get("type") == "function" and tool.get("function")
        ]
        body["tools"] = [{"functionDeclarations": declarations}]
        mode = {"required": "ANY", "none": "NONE"}.get(tool_choice, "AUTO")
        body["toolConfig"] = {"functionCallingConfig": {"mode": mode}}
        payload = _gemini_post(config, body, purpose="tool call")
        parts = _gemini_parts(payload)
        parsed_calls: list[dict[str, Any]] = []
        openai_calls: list[dict[str, Any]] = []
        text_parts: list[str] = []
        for index, part in enumerate(parts):
            if part.get("text") and not part.get("thought"):
                text_parts.append(str(part["text"]))
            function = part.get("functionCall") or {}
            if not function:
                continue
            args = function.get("args") or {}
            if not isinstance(args, dict):
                args = {}
            call_id = str(function.get("id") or f"gemini-call-{index}")
            name = str(function.get("name") or "")
            args_str = json.dumps(args, separators=(",", ":"))
            parsed_calls.append(
                {
                    "id": call_id,
                    "name": name,
                    "arguments_str": args_str,
                    "arguments": args,
                }
            )
            openai_calls.append(
                {
                    "id": call_id,
                    "type": "function",
                    "function": {"name": name, "arguments": args_str},
                }
            )
        content = "".join(text_parts)
        _record_llm_observation(
            config=config,
            messages=messages,
            json_mode=False,
            temperature=temperature,
            max_tokens=max_tokens,
            purpose=purpose,
            content=json.dumps(
                {"content": content, "tool_calls": parsed_calls},
                sort_keys=True,
                default=str,
            ),
            response_model=str(payload.get("modelVersion") or config.model),
            finish_reason=str(
                ((payload.get("candidates") or [{}])[0] or {}).get("finishReason") or ""
            ),
            usage=_gemini_usage(payload),
            tools=tools,
            latency_ms=round((time.perf_counter() - started) * 1000),
        )
        return {
            "content": content,
            "tool_calls": parsed_calls,
            # Keep the provider-native parts byte-for-byte equivalent across
            # the next tool turn so Gemini thought signatures remain valid.
            "assistant_message": {
                "role": "assistant",
                "content": content or None,
                "tool_calls": openai_calls,
                "gemini_parts": parts,
            },
            "usage": _gemini_usage(payload),
        }
    max_tokens, provider_options = _request_options(config, max_tokens)
    body: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": messages,
        "tools": tools,
        "tool_choice": tool_choice,
        **provider_options,
    }
    if not (
        config.name == "deepseek"
        and (provider_options.get("thinking") or {}).get("type") == "enabled"
    ):
        body["temperature"] = temperature
    data = json.dumps(body).encode("utf-8")
    timeout = float(os.getenv("LLM_TIMEOUT", "60"))
    retries = int(os.getenv("LLM_RETRIES", "2"))
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        req = urllib.request.Request(
            _endpoint(base),
            data=data,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = json.load(resp)
            choice = payload["choices"][0]["message"]
            usage = payload.get("usage", {}) or {}
            raw_calls = choice.get("tool_calls") or []
            parsed_calls: list[dict[str, Any]] = []
            for tc in raw_calls:
                fn = tc.get("function", {}) or {}
                args_str = fn.get("arguments", "") or ""
                try:
                    args = json.loads(args_str) if args_str else {}
                except json.JSONDecodeError:
                    args = {}
                parsed_calls.append(
                    {
                        "id": tc.get("id", ""),
                        "name": fn.get("name", ""),
                        "arguments_str": args_str,
                        "arguments": args,
                    }
                )
            _record_llm_observation(
                config=config,
                messages=messages,
                json_mode=False,
                temperature=temperature,
                max_tokens=max_tokens,
                purpose=purpose,
                content=json.dumps(
                    {"content": choice.get("content") or "", "tool_calls": parsed_calls},
                    sort_keys=True,
                    default=str,
                ),
                response_model=str(payload.get("model") or config.model),
                system_fingerprint=str(payload.get("system_fingerprint") or ""),
                finish_reason=str((payload.get("choices") or [{}])[0].get("finish_reason") or ""),
                usage=usage,
                tools=tools,
                latency_ms=round((time.perf_counter() - started) * 1000),
            )
            return {
                "content": choice.get("content") or "",
                "tool_calls": parsed_calls,
                # Preserve provider extensions such as Gemini thought
                # signatures when the assistant message is sent back on the
                # next tool-loop step.
                "assistant_message": {
                    key: value
                    for key, value in choice.items()
                    if key
                    in {
                        "role",
                        "content",
                        "reasoning_content",
                        "tool_calls",
                        "extra_content",
                    }
                },
                "usage": {
                    "prompt_tokens": usage.get("prompt_tokens", 0),
                    "completion_tokens": usage.get("completion_tokens", 0),
                    "total_tokens": usage.get("total_tokens", 0),
                },
            }
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise LLMError(f"chat_tools HTTP {exc.code}: {exc.reason}") from exc
        except (urllib.error.URLError, TimeoutError, KeyError, ValueError) as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(1.5 * (attempt + 1))
                continue
    raise LLMError(f"chat_tools failed: {last_exc}")


def _strip_fences(text: str) -> str:
    s = text.strip()
    if s.startswith("```"):
        s = s.split("\n", 1)[-1] if "\n" in s else s
        if s.endswith("```"):
            s = s[:-3]
        if s.startswith("json"):
            s = s[4:]
    return s.strip()
