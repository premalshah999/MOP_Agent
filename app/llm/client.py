"""DeepSeek (OpenAI-compatible) chat client with deterministic offline modes.

Resolution order for every call:
  1. An injected stub (tests set one via set_stub) — highest priority, fully offline.
  2. A live HTTP call to DeepSeek/OpenAI — when an API key is present and
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
import urllib.request
from pathlib import Path
from typing import Any, Callable

from app.paths import ROOT_DIR


class LLMError(RuntimeError):
    """A live call failed or returned something unusable."""


class LLMUnavailable(LLMError):
    """No stub, no API key, and no recorded fixture for this exact request."""


Messages = list[dict[str, str]]
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
def _provider() -> tuple[str | None, str, str]:
    deepseek = os.getenv("DEEPSEEK_API_KEY") or ""
    openai = os.getenv("OPENAI_API_KEY") or ""
    key = deepseek or openai or None
    if deepseek:
        base = os.getenv("ASSISTANT_ROUTER_BASE_URL") or "https://api.deepseek.com"
        model = os.getenv("ASSISTANT_ROUTER_MODEL") or os.getenv("DEEPSEEK_MODEL") or "deepseek-chat"
    else:
        base = os.getenv("ASSISTANT_ROUTER_BASE_URL") or "https://api.openai.com/v1"
        model = os.getenv("ASSISTANT_ROUTER_MODEL") or "gpt-4o-mini"
    return key, base.rstrip("/"), model


def is_live() -> bool:
    key, _, _ = _provider()
    return bool(key) and os.getenv("LLM_MODE", "").lower() != "fixture"


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


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def _http_call(messages: Messages, *, json_mode: bool, temperature: float, max_tokens: int) -> str:
    key, base, model = _provider()
    body: dict[str, Any] = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": messages,
    }
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
            return payload["choices"][0]["message"]["content"]
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code in (429, 500, 502, 503, 504) and attempt < retries:
                # Polite backoff: 2s, 4s — yields to the provider's own
                # rate-limit window before the next attempt.
                time.sleep(2.0 * (attempt + 1))
                continue
            if exc.code == 429:
                raise LLMError("The model is rate-limited right now. Please retry in a few seconds.") from exc
            if exc.code in (500, 502, 503, 504):
                raise LLMError(f"The model is temporarily unavailable (HTTP {exc.code}). Please retry.") from exc
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

    _, _, model = _provider()
    key = _fixture_key(model, messages, json_mode)

    if is_live():
        try:
            content = _http_call(
                messages, json_mode=json_mode, temperature=temperature, max_tokens=max_tokens
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
        f"(key={key[:12]}…). Set DEEPSEEK_API_KEY, inject a stub, or record fixtures "
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
            {"role": "user", "content": "That was not valid JSON. Reply with ONLY a single valid JSON object."},
        ]
        fixed = chat(
            repair, json_mode=True, temperature=0.0, max_tokens=max_tokens, purpose=f"{purpose}:repair"
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
    """OpenAI-compatible tool calling.

    Returns {content: str|"", tool_calls: list[{id, name, arguments_str, arguments}],
             usage: {prompt_tokens, completion_tokens}}.

    Bypasses the stub/fixture path — tool-calling reasoning is a live-only feature
    by design (no deterministic stub for emergent multi-step behaviour).
    """
    key, base, model = _provider()
    if not key:
        raise LLMUnavailable("chat_tools requires a live LLM API key")
    body: dict[str, Any] = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": messages,
        "tools": tools,
        "tool_choice": tool_choice,
    }
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
            return {
                "content": choice.get("content") or "",
                "tool_calls": parsed_calls,
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
            s = s[: -3]
        if s.startswith("json"):
            s = s[4:]
    return s.strip()
