from __future__ import annotations

import re
from dataclasses import dataclass, field


_STATE_NAMES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware",
    "district of columbia", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa",
    "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey",
    "new mexico", "new york", "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota", "tennessee", "texas", "utah",
    "vermont", "virginia", "washington", "west virginia", "wisconsin", "wyoming",
}


@dataclass
class ConversationState:
    previous_user_messages: list[str] = field(default_factory=list)
    previous_assistant_messages: list[str] = field(default_factory=list)
    last_user_question: str | None = None
    last_analytical_question: str | None = None
    pending_clarification_question: str | None = None
    pending_clarification_options: list[str] = field(default_factory=list)
    last_states: list[str] = field(default_factory=list)
    recent_context: str = ""


def extract_states(text: str) -> list[str]:
    q = text.lower()
    states: list[str] = []
    for state in sorted(_STATE_NAMES, key=len, reverse=True):
        if re.search(rf"\b{re.escape(state)}\b", q):
            states.append(state.title())
    return list(dict.fromkeys(states))


def build_conversation_state(history: list[dict[str, str]] | None, current_question: str) -> ConversationState:
    state = ConversationState()
    for item in history or []:
        content = item.get("content", "")
        if not content or content == current_question:
            continue
        if item.get("role") == "user":
            state.previous_user_messages.append(content)
        elif item.get("role") == "assistant":
            state.previous_assistant_messages.append(content)
    state.last_user_question = state.previous_user_messages[-1] if state.previous_user_messages else None
    for message in reversed(state.previous_user_messages):
        if any(
            token in message.lower()
            for token in (
                "fund", "money", "grant", "contract", "poverty", "income", "financial", "subcontract", "flow",
                "debt", "liabil", "asset", "population", "asian", "black", "white", "hispanic", "latino",
                "education", "college", "employment", "employee", "resident", "county", "state",
            )
        ):
            state.last_analytical_question = message
            break
    for message in reversed(state.previous_assistant_messages):
        if "Valid interpretations:" in message or "Needs clarification" in message or "one more detail" in message:
            state.pending_clarification_question = state.last_user_question
            options = []
            for line in message.splitlines():
                cleaned = line.strip("- ").strip()
                if cleaned and cleaned != "Valid interpretations:" and not cleaned.startswith("I need"):
                    options.append(cleaned)
            state.pending_clarification_options = options
            break
    combined = "\n".join(state.previous_user_messages[-4:])
    state.last_states = extract_states(combined)
    context_lines = []
    for item in (history or [])[-8:]:
        role = item.get("role")
        content = item.get("content", "").strip()
        if role in {"user", "assistant"} and content and content != current_question:
            context_lines.append(f"{role}: {content}")
    state.recent_context = "\n".join(context_lines)
    return state
