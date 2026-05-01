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
_FRUSTRATION_PATTERNS = (
    "are you crazy", "are you crasy", "you are wrong", "that's wrong", "that is wrong",
    "not what i asked", "this is bad", "terrible", "shitty", "stupid", "wtf",
)
_CLARIFICATION_PATTERNS = (
    "first", "first one", "the first one", "option one", "1", "second", "the second one", "third",
    "total federal funding received by the geography", "subcontract", "fund-flow", "specific channel",
)
_RANKING_TERMS = (
    "top", "bottom", "highest", "lowest", "maximum", "minimum", "max", "min",
    "most", "least", "rank", "ranking", "largest", "biggest", "smallest",
)
_ANALYTIC_SUBJECT_TERMS = (
    "population", "asian", "black", "white", "hispanic", "latino", "poverty",
    "income", "education", "college", "funding", "money", "spending", "grant",
    "grants", "contract", "contracts", "deal", "deals", "employment", "employee",
    "employees", "jobs", "asset", "assets", "liability", "liabilities", "debt",
    "finra", "financial", "literacy", "satisfaction", "constraint", "flow",
)
_METADATA_SUBJECT_TERMS = (
    "finra", "acs", "census", "government finance", "federal spending",
    "federal funding", "fund flow", "fund-flow", "subaward", "metadata",
)


def _is_complete_analytics_question(q: str) -> bool:
    has_subject = any(term in q for term in _ANALYTIC_SUBJECT_TERMS)
    has_ranking = any(term in q for term in _RANKING_TERMS)
    has_lookup = any(phrase in q for phrase in ("how much", "how many", "where is", "where are", "show me", "give me"))
    return has_subject and (has_ranking or has_lookup)


def _is_metadata_subject_question(q: str) -> bool:
    starts_like_definition = bool(re.match(r"^(what is|what are|what's|explain|tell me about)\b", q))
    has_runtime_scope = any(
        term in q
        for term in (
            " in ", " for ", " at ", "county", "counties", "state", "states", "district",
            "maryland", "california", "virginia", "texas", "202", "top", "rank", "maximum", "minimum",
        )
    )
    return starts_like_definition and not has_runtime_scope and any(term in q for term in _METADATA_SUBJECT_TERMS)


def _deterministic_guardrail_route(question: str, state: ConversationState) -> RouterOutput | None:
    q = question.strip().lower()
    if not q:
        return RouterOutput(mode="GENERAL_ASSISTANT", confidence="high", reason="Empty message.")
    if state.pending_clarification_question and any(re.fullmatch(rf".*\b{re.escape(pattern)}\b.*", q) for pattern in _CLARIFICATION_PATTERNS):
        return RouterOutput(mode="CLARIFICATION_RESPONSE", confidence="high", requires_sql=True, is_follow_up=True, reason="Reply resolves a pending clarification.")
    if any(pattern in q for pattern in _FRUSTRATION_PATTERNS):
        return RouterOutput(mode="CONVERSATION_REPAIR", confidence="high", requires_sql=False, requires_metadata=True, is_follow_up=True, reason="User is correcting or expressing frustration.")
    if any(pattern in q for pattern in _OUT_OF_SCOPE_PATTERNS):
        return RouterOutput(mode="OUT_OF_SCOPE", confidence="medium", reason="Likely outside the analytics assistant scope.")
    if any(pattern in q for pattern in _HELP_PATTERNS):
        return RouterOutput(mode="ASSISTANT_HELP", confidence="high", requires_sql=False, requires_metadata=True, reason="User is asking about assistant capabilities or identity.")
    if any(pattern in q for pattern in _DISCOVERY_PATTERNS):
        return RouterOutput(mode="DATASET_DISCOVERY", confidence="high", requires_sql=False, requires_metadata=True, reason="User is asking what data is available.")
    if _is_metadata_subject_question(q) or any(pattern in q for pattern in _DEFINITION_PATTERNS) or "available years" in q or "what years" in q or "available metrics" in q:
        return RouterOutput(mode="METRIC_DEFINITION", confidence="high", requires_sql=False, requires_metadata=True, reason="User is asking for metadata or a definition.")
    if _is_complete_analytics_question(q):
        return RouterOutput(mode="SIMPLE_ANALYTICS", confidence="high", requires_sql=True, requires_metadata=True, reason="Complete analytics question with metric/domain signal.")
    return None


def _chat_completion_endpoint(base_url: str) -> str:
    base = base_url.rstrip("/")
    if base.endswith("/chat/completions"):
        return base
    return f"{base}/chat/completions"


def _route_with_llm(question: str, state: ConversationState) -> RouterOutput | None:
    deepseek_key = os.getenv("DEEPSEEK_API_KEY")
    api_key = deepseek_key or os.getenv("OPENAI_API_KEY")
    if not api_key or os.getenv("ASSISTANT_ROUTER_MODE", "local").lower() != "llm":
        return None
    base_url = os.getenv("ASSISTANT_ROUTER_BASE_URL") or ("https://api.deepseek.com" if deepseek_key else "https://api.openai.com/v1")
    model = os.getenv("ASSISTANT_ROUTER_MODEL") or os.getenv("DEEPSEEK_MODEL") or ("deepseek-chat" if deepseek_key else "gpt-4.1-mini")
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
            "CONVERSATION_REPAIR",
            "OUT_OF_SCOPE",
        ],
        "examples": {
            "who are you?": "ASSISTANT_HELP",
            "what kind of questions can I ask?": "ASSISTANT_HELP",
            "what data do you have?": "DATASET_DISCOVERY",
            "what is FINRA?": "METRIC_DEFINITION",
            "explain ACS": "METRIC_DEFINITION",
            "what does grants mean?": "METRIC_DEFINITION",
            "maximum asian population by count": "SIMPLE_ANALYTICS",
            "rank maximum asian population by percentage": "SIMPLE_ANALYTICS",
            "top 10 counties by funding": "SIMPLE_ANALYTICS",
            "compare Maryland vs Virginia on grants": "COMPLEX_ANALYTICS",
            "why did financial literacy change?": "ROOT_CAUSE_ANALYSIS",
            "the first one": "CLARIFICATION_RESPONSE",
            "show it on a map": "VISUALIZATION_REQUEST",
            "are you crazy?": "CONVERSATION_REPAIR",
            "I meant counties, not states": "FOLLOW_UP_ANALYTICS",
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
            "model": model,
            "temperature": 0,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a strict router for a production public-policy analytics assistant. "
                        "Classify intent, not data. Be typo-tolerant: 'defence' means defense, 'dept' means department, "
                        "'deals' usually means contract dollars, and misspelled county/state corrections are follow-ups. "
                        "Do not route a complete analytics request as a follow-up only because it says count, percentage, amount, or ratio. "
                        "Route dataset-family questions like 'what is FINRA?' to METRIC_DEFINITION and do not send them to SQL. "
                        "If the user is frustrated or says the answer was wrong, route to CONVERSATION_REPAIR. "
                        "Return valid JSON only."
                    ),
                },
                {"role": "user", "content": json.dumps(prompt)},
            ],
        }
    ).encode()
    request = urllib.request.Request(
        _chat_completion_endpoint(base_url),
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
    q = question.strip().lower()
    guardrail_route = _deterministic_guardrail_route(question, state)
    if guardrail_route:
        return guardrail_route
    llm_route = _route_with_llm(question, state)
    if llm_route:
        if llm_route.mode == "FOLLOW_UP_ANALYTICS" and _is_complete_analytics_question(q):
            return RouterOutput(mode="SIMPLE_ANALYTICS", confidence="high", requires_sql=True, requires_metadata=True, reason="LLM follow-up route overridden because the question is complete.")
        if llm_route.mode not in {"METRIC_DEFINITION", "DATASET_DISCOVERY"} and _is_metadata_subject_question(q):
            return RouterOutput(mode="METRIC_DEFINITION", confidence="high", requires_sql=False, requires_metadata=True, reason="LLM SQL route overridden for known metadata subject.")
        return llm_route
    if any(pattern in q for pattern in _VISUAL_PATTERNS) and len(q.split()) <= 8:
        return RouterOutput(mode="VISUALIZATION_REQUEST", confidence="medium", requires_sql=False, requires_metadata=True, is_follow_up=True, reason="Short visualization follow-up.")
    if any(phrase in q for phrase in ("why", "what drove", "drivers", "root cause", "explain")):
        return RouterOutput(mode="ROOT_CAUSE_ANALYSIS", confidence="high", requires_sql=True, requires_metadata=True, reason="Diagnostic wording.")
    if state.previous_user_messages and (
        re.match(r"^(what about|how about|and|only|just|compare|versus|vs)\b", q)
        or looks_like_metric_variant_follow_up(q)
        or any(phrase in q for phrase in ("what you gave", "that was", "instead", "same thing", "i meant", "not states", "not state", "not counties", "not county", "countis"))
    ):
        return RouterOutput(mode="FOLLOW_UP_ANALYTICS", confidence="high", requires_sql=True, requires_metadata=True, is_follow_up=True, reason="Follow-up wording.")
    if any(phrase in q for phrase in ("compare", " versus ", " vs ", "break down", "breakdown", "by ", "explain")):
        return RouterOutput(mode="COMPLEX_ANALYTICS", confidence="medium", requires_sql=True, requires_metadata=True, reason="Multi-part analytical wording.")
    return RouterOutput(mode="SIMPLE_ANALYTICS", confidence="medium", requires_sql=True, requires_metadata=True, reason="Default analytics workflow.")
