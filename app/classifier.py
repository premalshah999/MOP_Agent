from __future__ import annotations

import os

from dotenv import load_dotenv
from openai import OpenAI


load_dotenv()

client = OpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
)

CLASSIFIER_PROMPT = """Classify this user message into exactly one of:
- DATA_QUERY: requires a NEW database lookup or data retrieval (not referencing prior results)
- FOLLOWUP: refers to previous results or asks for details about them. Includes: "what is it?", "which one?", "show me the details", "now show...", "instead...", "also...", "what about...", "filter by...", "sort by...", or any short question using pronouns like "it", "that", "them", "those" to refer to prior data
- CONCEPTUAL: asks for a definition, explanation, or general knowledge (e.g. "what is debt ratio?", "explain OPEB")

IMPORTANT: Short questions with pronouns (it, that, them) are almost always FOLLOWUP, not CONCEPTUAL.
"What is it?" = FOLLOWUP. "What is debt ratio?" = CONCEPTUAL.

Reply with only the label. No explanation."""


def _fallback_classifier(question: str) -> str:
    q = question.lower().strip()
    followup_tokens = ["now", "instead", "also", "what about", "filter", "sort", "compare with"]
    conceptual_tokens = ["define", "meaning", "explain", "difference between"]

    # Short questions referencing "it" / "that" are almost always follow-ups
    words_clean = [w.strip("?.,!") for w in q.split()]
    if len(words_clean) <= 10 and any(w in words_clean for w in ["it", "that", "them", "those", "this"]):
        return "FOLLOWUP"
    if any(phrase in q for phrase in ["which one", "what one"]):
        return "FOLLOWUP"

    if any(t in q for t in followup_tokens):
        return "FOLLOWUP"
    # "what is X" is conceptual only for longer, definitional questions
    # Short "what is it?" / "what flow is it?" are follow-ups (handled above)
    if any(t in q for t in conceptual_tokens):
        return "CONCEPTUAL"
    if q.startswith("what is ") and len(q.split()) >= 4 and "it" not in q:
        return "CONCEPTUAL"
    return "DATA_QUERY"


def classify(question: str) -> str:
    if not os.getenv("DEEPSEEK_API_KEY"):
        return _fallback_classifier(question)

    try:
        response = client.chat.completions.create(
            model=os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
            temperature=0,
            max_tokens=10,
            messages=[
                {"role": "system", "content": CLASSIFIER_PROMPT},
                {"role": "user", "content": question},
            ],
        )
        label = (response.choices[0].message.content or "").strip().upper()
        if label in {"DATA_QUERY", "FOLLOWUP", "CONCEPTUAL"}:
            return label
    except Exception:
        pass

    return _fallback_classifier(question)
