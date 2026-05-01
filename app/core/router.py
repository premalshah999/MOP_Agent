from __future__ import annotations

import json
import os
import re
import urllib.request

from app.core.conversation import ConversationState
from app.schemas.router import RouterOutput
from app.semantic.metric_variants import looks_like_metric_variant_follow_up


_HELP_PATTERNS = (
    "who are you", "what are you", "what can you do", "how can you help", "how do i use",
    "what can i ask", "what should i ask", "your capabilities", "what are your capabilities",
    "help me", "can you help", "questions can i ask", "kind of questions", "what insights",
    "how should i use", "examples", "example questions",
)
_DISCOVERY_PATTERNS = (
    "what data", "which data", "what datasets", "which datasets", "show datasets", "available datasets",
    "data do you have", "what do you know", "what can you answer",
)
_DEFINITION_PATTERNS = (
    "what does", "define", "definition", "how is", "how do you calculate", "what is the meaning",
    "does sales mean", "is sales", "metric mean",
)
_VISUAL_PATTERNS = ("map", "chart", "plot", "visualize", "show that", "graph")
_OUT_OF_SCOPE_PATTERNS = ("joke", "poem", "recipe", "weather", "sports", "stock price")
_CLARIFICATION_PATTERNS = (
    "first", "first one", "the first one", "option one", "1", "second", "the second one", "third",
    "total federal funding received by the geography", "subcontract", "fund-flow", "specific channel",
)


def _route_with_llm(question: str, state: ConversationState) -> RouterOutput | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or os.getenv("ASSISTANT_ROUTER_MODE", "local").lower() != "llm":
        return None
    prompt = {
        "task": "Classify this user message for a multi-mode public-policy analytics assistant. Return JSON only.",
        "modes": [
            "GENERAL_ASSISTANT",
            "ASSISTANT_HELP",
            "DATASET_DISCOVERY",
            "METRIC_DEFINITION",
            "SIMPLE_ANALYTICS",
            "COMPLEX_ANALYTICS",
            "ROOT_CAUSE_ANALYSIS",
            "FOLLOW_UP_ANALYTICS",
            "VISUALIZATION_REQUEST",
            "CLARIFICATION_RESPONSE",
            "OUT_OF_SCOPE",
        ],
        "examples": {
            "who are you?": "ASSISTANT_HELP",
            "what kind of questions can I ask?": "ASSISTANT_HELP",
            "what data do you have?": "DATASET_DISCOVERY",
            "what does grants mean?": "METRIC_DEFINITION",
            "top 10 counties by funding": "SIMPLE_ANALYTICS",
            "compare Maryland vs Virginia on grants": "COMPLEX_ANALYTICS",
            "why did financial literacy change?": "ROOT_CAUSE_ANALYSIS",
            "the first one": "CLARIFICATION_RESPONSE",
            "show it on a map": "VISUALIZATION_REQUEST",
        },
        "pending_clarification": bool(state.pending_clarification_question),
        "last_user_question": state.last_user_question,
        "recent_context": state.recent_context,
        "message": question,
        "schema": {
            "mode": "one of the modes",
            "confidence": "high | medium | low",
            "requires_sql": "boolean",
            "requires_metadata": "boolean",
            "is_follow_up": "boolean",
            "needs_clarification": "boolean",
            "clarification_question": "string or null",
            "reason": "short reason",
        },
    }
    body = json.dumps(
        {
            "model": os.getenv("ASSISTANT_ROUTER_MODEL", "gpt-4.1-mini"),
            "temperature": 0,
            "messages": [
                {"role": "system", "content": "You are a strict analytics assistant router. Return valid JSON only."},
                {"role": "user", "content": json.dumps(prompt)},
            ],
        }
    ).encode()
    request = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=body,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            payload = json.load(response)
        content = payload["choices"][0]["message"]["content"]
        return RouterOutput.model_validate_json(content)
    except Exception:
        return None


def route_message(question: str, state: ConversationState) -> RouterOutput:
    llm_route = _route_with_llm(question, state)
    if llm_route:
        return llm_route
    q = question.strip().lower()
    if not q:
        return RouterOutput(mode="GENERAL_ASSISTANT", confidence="high", reason="Empty message.")
    if state.pending_clarification_question and any(re.fullmatch(rf".*\b{re.escape(pattern)}\b.*", q) for pattern in _CLARIFICATION_PATTERNS):
        return RouterOutput(mode="CLARIFICATION_RESPONSE", confidence="high", requires_sql=True, is_follow_up=True, reason="Reply resolves a pending clarification.")
    if any(pattern in q for pattern in _OUT_OF_SCOPE_PATTERNS):
        return RouterOutput(mode="OUT_OF_SCOPE", confidence="medium", reason="Likely outside the analytics assistant scope.")
    if any(pattern in q for pattern in _HELP_PATTERNS):
        return RouterOutput(mode="ASSISTANT_HELP", confidence="high", requires_sql=False, requires_metadata=True, reason="User is asking about assistant capabilities or identity.")
    if any(pattern in q for pattern in _DISCOVERY_PATTERNS):
        return RouterOutput(mode="DATASET_DISCOVERY", confidence="high", requires_sql=False, requires_metadata=True, reason="User is asking what data is available.")
    if any(pattern in q for pattern in _DEFINITION_PATTERNS) or "available years" in q or "what years" in q or "available metrics" in q:
        return RouterOutput(mode="METRIC_DEFINITION", confidence="high", requires_sql=False, requires_metadata=True, reason="User is asking for metadata or a definition.")
    if any(pattern in q for pattern in _VISUAL_PATTERNS) and len(q.split()) <= 8:
        return RouterOutput(mode="VISUALIZATION_REQUEST", confidence="medium", requires_sql=False, requires_metadata=True, is_follow_up=True, reason="Short visualization follow-up.")
    if any(phrase in q for phrase in ("why", "what drove", "drivers", "root cause", "explain")):
        return RouterOutput(mode="ROOT_CAUSE_ANALYSIS", confidence="high", requires_sql=True, requires_metadata=True, reason="Diagnostic wording.")
    if state.previous_user_messages and (
        re.match(r"^(what about|how about|and|only|just|compare|versus|vs)\b", q)
        or looks_like_metric_variant_follow_up(q)
        or any(phrase in q for phrase in ("what you gave", "that was", "instead", "same thing"))
    ):
        return RouterOutput(mode="FOLLOW_UP_ANALYTICS", confidence="high", requires_sql=True, requires_metadata=True, is_follow_up=True, reason="Follow-up wording.")
    if any(phrase in q for phrase in ("compare", " versus ", " vs ", "break down", "breakdown", "by ", "explain")):
        return RouterOutput(mode="COMPLEX_ANALYTICS", confidence="medium", requires_sql=True, requires_metadata=True, reason="Multi-part analytical wording.")
    return RouterOutput(mode="SIMPLE_ANALYTICS", confidence="medium", requires_sql=True, requires_metadata=True, reason="Default analytics workflow.")
