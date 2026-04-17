from __future__ import annotations

import os

from dotenv import load_dotenv
from openai import OpenAI
import requests


load_dotenv()


_GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
_DEEPSEEK_BASE_URL = "https://api.deepseek.com"


def llm_provider() -> str:
    provider = os.getenv("LLM_PROVIDER", "").strip().lower()
    if provider:
        return provider
    if os.getenv("GEMINI_API_KEY"):
        return "gemini"
    return "deepseek"


def llm_api_key() -> str | None:
    explicit = os.getenv("LLM_API_KEY")
    if explicit:
        return explicit
    if llm_provider() == "gemini":
        return os.getenv("GEMINI_API_KEY")
    return os.getenv("DEEPSEEK_API_KEY")


def llm_base_url() -> str:
    explicit = os.getenv("LLM_BASE_URL")
    if explicit:
        return explicit
    if llm_provider() == "gemini":
        return os.getenv("GEMINI_BASE_URL", _GEMINI_BASE_URL)
    return os.getenv("DEEPSEEK_BASE_URL", _DEEPSEEK_BASE_URL)


def llm_model() -> str:
    explicit = os.getenv("LLM_MODEL")
    if explicit:
        return explicit
    if llm_provider() == "gemini":
        return os.getenv("GEMINI_MODEL", "gemini-flash-latest")
    return os.getenv("DEEPSEEK_MODEL", "deepseek-chat")


def llm_reasoner_model() -> str:
    explicit = os.getenv("LLM_REASONER_MODEL")
    if explicit:
        return explicit
    if llm_provider() == "gemini":
        return os.getenv("GEMINI_REASONER_MODEL", llm_model())
    return os.getenv("DEEPSEEK_REASONER_MODEL", llm_model())


def llm_timeout(default_seconds: float = 45.0) -> float:
    explicit = os.getenv("LLM_TIMEOUT")
    if explicit:
        return float(explicit)
    if llm_provider() == "gemini":
        return float(os.getenv("GEMINI_TIMEOUT", str(default_seconds)))
    return float(os.getenv("DEEPSEEK_TIMEOUT", str(default_seconds)))


def llm_available() -> bool:
    return bool(llm_api_key())


def llm_client(*, timeout: float | None = None) -> OpenAI:
    return OpenAI(
        api_key=llm_api_key(),
        base_url=llm_base_url(),
        timeout=timeout if timeout is not None else llm_timeout(),
    )


def llm_missing_key_message() -> str:
    return "No LLM API key is configured. Add GEMINI_API_KEY, LLM_API_KEY, or DEEPSEEK_API_KEY to .env."


def gemini_fallback_model() -> str | None:
    fallback = os.getenv("GEMINI_FALLBACK_MODEL", "").strip()
    return fallback or None


def deepseek_available() -> bool:
    return bool(os.getenv("DEEPSEEK_API_KEY"))


def deepseek_model() -> str:
    return os.getenv("DEEPSEEK_MODEL", "deepseek-chat")


def _openai_generate(
    messages: list[dict[str, str]],
    *,
    model: str,
    temperature: float = 0,
    max_tokens: int | None = None,
    timeout: float | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
) -> str:
    client = OpenAI(
        api_key=api_key or llm_api_key(),
        base_url=base_url or llm_base_url(),
        timeout=timeout if timeout is not None else llm_timeout(),
    )
    response = client.chat.completions.create(
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        messages=messages,
    )
    return (response.choices[0].message.content or "").strip()


def _gemini_contents(messages: list[dict[str, str]]) -> tuple[str | None, list[dict[str, object]]]:
    system_parts: list[str] = []
    contents: list[dict[str, object]] = []

    for message in messages:
        role = message.get("role", "user")
        content = message.get("content", "")
        if not isinstance(content, str):
            content = str(content)
        if not content.strip():
            continue

        if role == "system":
            system_parts.append(content)
            continue

        gemini_role = "model" if role == "assistant" else "user"
        part = {"text": content}
        if contents and contents[-1]["role"] == gemini_role:
            contents[-1]["parts"].append(part)  # type: ignore[index]
        else:
            contents.append({"role": gemini_role, "parts": [part]})

    system_instruction = "\n\n".join(system_parts).strip() or None
    return system_instruction, contents


def _extract_gemini_text(payload: dict[str, object]) -> str:
    candidates = payload.get("candidates") or []
    if not isinstance(candidates, list) or not candidates:
        return ""
    first = candidates[0]
    if not isinstance(first, dict):
        return ""
    content = first.get("content") or {}
    if not isinstance(content, dict):
        return ""
    parts = content.get("parts") or []
    if not isinstance(parts, list):
        return ""
    texts = [part.get("text", "") for part in parts if isinstance(part, dict) and part.get("text")]
    return "".join(texts).strip()


def _gemini_generate(
    messages: list[dict[str, str]],
    *,
    model: str,
    temperature: float = 0,
    max_tokens: int | None = None,
    timeout: float | None = None,
) -> str:
    system_instruction, contents = _gemini_contents(messages)
    if not contents:
        raise ValueError("Gemini request had no user/assistant contents to send.")

    payload: dict[str, object] = {"contents": contents}
    if system_instruction:
        payload["system_instruction"] = {"parts": [{"text": system_instruction}]}

    generation_config: dict[str, object] = {
        "temperature": temperature,
    }
    if max_tokens is not None:
        generation_config["maxOutputTokens"] = max_tokens
    if generation_config:
        payload["generationConfig"] = generation_config

    model_name = model.removeprefix("models/")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent"
    try:
        response = requests.post(
            url,
            headers={
                "x-goog-api-key": llm_api_key() or "",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=timeout if timeout is not None else llm_timeout(),
        )
        if response.status_code == 429 and deepseek_available():
            return _openai_generate(
                messages,
                model=deepseek_model(),
                temperature=temperature,
                max_tokens=max_tokens,
                timeout=timeout,
                api_key=os.getenv("DEEPSEEK_API_KEY"),
                base_url=os.getenv("DEEPSEEK_BASE_URL", _DEEPSEEK_BASE_URL),
            )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException:
        if deepseek_available():
            return _openai_generate(
                messages,
                model=deepseek_model(),
                temperature=temperature,
                max_tokens=max_tokens,
                timeout=timeout,
                api_key=os.getenv("DEEPSEEK_API_KEY"),
                base_url=os.getenv("DEEPSEEK_BASE_URL", _DEEPSEEK_BASE_URL),
            )
        raise
    text = _extract_gemini_text(data)
    if text:
        return text

    fallback = gemini_fallback_model()
    if fallback and fallback != model_name:
        return _gemini_generate(
            messages,
            model=fallback,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )
    if deepseek_available():
        return _openai_generate(
            messages,
            model=deepseek_model(),
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
            api_key=os.getenv("DEEPSEEK_API_KEY"),
            base_url=os.getenv("DEEPSEEK_BASE_URL", _DEEPSEEK_BASE_URL),
        )
    raise ValueError(f"Gemini returned no text content for model `{model_name}`.")


def llm_complete(
    messages: list[dict[str, str]],
    *,
    model: str | None = None,
    temperature: float = 0,
    max_tokens: int | None = None,
    timeout: float | None = None,
) -> str:
    target_model = model or llm_model()
    if llm_provider() == "gemini":
        return _gemini_generate(
            messages,
            model=target_model,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )

    return _openai_generate(
        messages,
        model=target_model,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )
