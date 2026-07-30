"""Grounded concept discovery for ambiguous, misspelled, and unsupported asks.

The router decides whether a question is answerable. This module decides what
the user can ask next without letting an LLM invent a variable. Every option is
constructed from the semantic registry and is therefore executable by the same
analytics pipeline that will receive the follow-up.
"""

from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from functools import lru_cache
import re
from typing import Any

from app.semantic.registry import get_dataset, load_registry


_WORD_RE = re.compile(r"[a-z0-9]+")
_GENERIC = {
    "a", "an", "and", "are", "by", "compare", "data", "did", "do", "for",
    "from", "have", "highest", "how", "i", "in", "is", "it", "lowest",
    "many", "most", "much", "of", "on", "rank", "show", "states", "the",
    "to", "top", "what", "which", "with",
}
_STATE_NAMES = tuple(
    sorted(
        "Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|"
        "Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|"
        "Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|"
        "Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|"
        "North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|"
        "South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|"
        "Wisconsin|Wyoming|District of Columbia".split("|"),
        key=len,
        reverse=True,
    )
)
_STATE_RE = re.compile(r"\b(" + "|".join(re.escape(name) for name in _STATE_NAMES) + r"|DC)\b", re.I)


def _normalize(value: str) -> str:
    words = []
    for word in _WORD_RE.findall(value.casefold().replace("&", " and ")):
        if len(word) > 4 and word.endswith("s") and not word.endswith("ss"):
            word = word[:-1]
        words.append(word)
    return " ".join(words)


def _meaningful_tokens(value: str) -> set[str]:
    return {word for word in _normalize(value).split() if word not in _GENERIC and len(word) > 1}


@dataclass(frozen=True)
class MetricConcept:
    dataset_id: str
    variable: str
    label: str
    description: str
    unit: str
    family: str
    geography: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class ConceptMatch:
    concept: MetricConcept
    score: float


def _label(value: str) -> str:
    text = " ".join(value.replace("_", " ").replace(",", " ").replace("&", "and").split())
    if text.casefold() == "subaward amount year":
        text = "subaward amount"
    return text[:1].upper() + text[1:]


@lru_cache(maxsize=1)
def concept_catalog() -> tuple[MetricConcept, ...]:
    concepts: list[MetricConcept] = []
    for dataset in load_registry().datasets.values():
        for metric in dataset.metrics.values():
            aliases = {
                _normalize(metric.id),
                _normalize(metric.label),
                _normalize(metric.description),
                *(_normalize(alias) for alias in metric.synonyms),
            }
            aliases.discard("")
            concepts.append(
                MetricConcept(
                    dataset_id=dataset.id,
                    variable=metric.id,
                    label=_label(metric.label),
                    description=metric.description,
                    unit=metric.unit,
                    family=dataset.family,
                    geography=dataset.geography,
                    aliases=tuple(sorted(aliases)),
                )
            )
    return tuple(concepts)


def _requested_geography(question: str) -> str:
    q = question.casefold()
    if "congress" in q or re.search(r"\bdistricts?\b", q):
        return "congressional_district"
    if re.search(r"\bcount(?:y|ies)\b", q):
        return "county"
    return "state"


def _alias_score(query: str, alias: str) -> float:
    query_tokens = _meaningful_tokens(query)
    alias_tokens = _meaningful_tokens(alias)
    if not alias_tokens or not query_tokens:
        return 0.0
    if alias in query and len(alias) >= 4:
        return 1.0
    overlap = len(query_tokens & alias_tokens) / len(alias_tokens)
    query_words = _normalize(query).split()
    alias_words = alias.split()
    width = max(1, len(alias_words))
    windows = [" ".join(query_words[i : i + width]) for i in range(max(1, len(query_words) - width + 1))]
    fuzzy = max((SequenceMatcher(None, alias, window).ratio() for window in windows), default=0.0)
    # Token containment catches "poverty rate" -> "below poverty"; the
    # n-gram ratio catches real typos such as "financial literasy".
    return max(overlap * 0.92, fuzzy)


def discover_metrics(question: str, limit: int = 5) -> list[ConceptMatch]:
    geography = _requested_geography(question)
    q = question.casefold()
    flow_signal = bool(re.search(r"\b(subawards?|subcontracts?|inflows?|outflows?)\b", q))
    ranked: list[ConceptMatch] = []
    for concept in concept_catalog():
        score = max((_alias_score(question, alias) for alias in concept.aliases), default=0.0)
        if flow_signal:
            score += 0.2 if concept.family == "subaward_flow" else -0.08
        if concept.geography == geography:
            score = min(1.0, score + 0.025)
        score = max(0.0, min(1.0, score))
        ranked.append(ConceptMatch(concept, score))
    ranked.sort(key=lambda item: (-item.score, _dataset_priority(item.concept.dataset_id)))

    # Collapse sibling-grain and duplicate federal tables so five suggestions
    # do not all present the same measure under different physical files.
    output: list[ConceptMatch] = []
    seen: set[tuple[str, str]] = set()
    for match in ranked:
        key = (match.concept.family, _normalize(match.concept.label))
        if key in seen:
            continue
        seen.add(key)
        output.append(match)
        if len(output) >= limit:
            break
    return output


def _dataset_priority(dataset_id: str) -> int:
    preferred = (
        "contract_state", "acs_state", "gov_state", "finra_state", "state_flow",
        "spending_state_agency", "contract_county", "acs_county", "gov_county",
    )
    try:
        return preferred.index(dataset_id)
    except ValueError:
        return len(preferred)


def _concept(dataset_id: str, variable: str) -> MetricConcept | None:
    return next(
        (item for item in concept_catalog() if item.dataset_id == dataset_id and item.variable == variable),
        None,
    )


def _topic_defaults(question: str) -> list[MetricConcept]:
    q = question.casefold()
    if any(term in q for term in ("subaward", "subcontract", "flow", "inflow", "outflow")):
        refs = [("state_flow", "subaward_amount_year")]
    elif any(term in q for term in ("federal", "funding", "award", "grant", "contract")):
        refs = [
            ("contract_state", "Contracts"),
            ("contract_state", "Grants"),
            ("contract_state", "Direct Payments"),
        ]
    elif any(term in q for term in ("debt", "fiscal", "liability", "revenue", "expense", "cash")):
        refs = [
            ("gov_state", "Debt_Ratio"),
            ("gov_state", "Total_Liabilities_per_capita"),
            ("gov_state", "Free_Cash_Flow_per_capita"),
        ]
    elif any(term in q for term in ("literacy", "financial health", "stress", "risk")):
        refs = [
            ("finra_state", "financial_literacy"),
            ("finra_state", "financial_constraint"),
            ("finra_state", "satisfied"),
        ]
    elif any(term in q for term in ("population", "income", "poverty", "education", "demographic", "housing")):
        refs = [
            ("acs_state", "Total population"),
            ("acs_state", "Median household income"),
            ("acs_state", "Below poverty"),
        ]
    else:
        refs = [
            ("acs_state", "Below poverty"),
            ("contract_state", "Grants"),
            ("gov_state", "Debt_Ratio"),
            ("finra_state", "financial_literacy"),
            ("state_flow", "subaward_amount_year"),
        ]
    return [concept for ref in refs if (concept := _concept(*ref)) is not None]


def _states(question: str) -> list[str]:
    values: list[str] = []
    for match in _STATE_RE.finditer(question):
        value = "District of Columbia" if match.group(0).casefold() == "dc" else match.group(0).title()
        if value not in values:
            values.append(value)
    return values[:2]


def _period(concept: MetricConcept) -> str:
    dataset = get_dataset(concept.dataset_id)
    if dataset is None or dataset.default_year in (None, ""):
        return ""
    prefix = "FY" if concept.dataset_id.startswith(("contract_", "spending_", "gov_")) else ""
    return f" in {prefix}{dataset.default_year}"


def question_for(concept: MetricConcept, original_question: str) -> str:
    states = _states(original_question)
    label = concept.label.casefold()
    period = _period(concept)
    q = original_question.casefold()
    if concept.dataset_id.endswith("_flow"):
        if not states:
            return "Which 10 states receive the most federal subaward funding?"
        state = states[0]
        if "outflow" in q or re.search(r"\b(from|out of)\b", q):
            return f"How much federal subaward funding flows out of {state}?"
        if "inflow" in q or re.search(r"\b(to|into)\b", q):
            return f"How much federal subaward funding flows into {state}?"
        return f"Which states receive the most federal subaward funding from {state}?"
    if len(states) == 2:
        return f"Compare {states[0]} and {states[1]} on {label}{period}."
    if len(states) == 1:
        if re.search(r"\b(rank|ranking|top|highest|lowest|best|worst)\b", q):
            return f"Where does {states[0]} rank nationally on {label}{period}?"
        if concept.dataset_id.startswith(("contract_", "spending_")) and concept.unit.casefold() == "usd":
            return f"How much did {states[0]} receive in {label}{period}?"
        return f"What is {states[0]}'s {label}{period}?"
    if concept.geography == "county":
        return f"Which 10 counties have the highest {label}{period}?"
    return f"Which 10 states have the highest {label}{period}?"


def _flow_questions(original_question: str) -> list[str]:
    states = _states(original_question)
    state = states[0] if states else "Maryland"
    peer = "Maryland" if state.casefold() == "virginia" else "Virginia"
    q = original_question.casefold()
    if "outflow" in q or re.search(r"\b(from|out of)\b", q):
        return [
            f"How much federal subaward funding flows out of {state}?",
            f"Which states receive the most federal subaward funding from {state}?",
            f"Which federal agencies account for the most subaward outflow from {state}?",
            f"Compare {state}'s subaward outflow with {peer}'s.",
        ]
    if "inflow" in q or re.search(r"\b(to|into)\b", q):
        return [
            f"How much federal subaward funding flows into {state}?",
            f"Which states send the most federal subaward funding into {state}?",
            f"Which federal agencies account for the most subaward inflow to {state}?",
            f"Compare {state}'s subaward inflow with {peer}'s.",
        ]
    return [
        "Which 10 states receive the most federal subaward funding?",
        "Which 10 states send out the most federal subaward funding?",
        f"Which states receive the most federal subaward funding from {state}?",
    ]


def build_guidance(
    question: str,
    *,
    intent: str = "CLARIFY",
    clarification: str = "",
    max_items: int = 5,
) -> dict[str, Any]:
    matches = discover_metrics(question, limit=max_items)
    close = [match for match in matches if match.score >= 0.72]
    if close:
        close = [match for match in close if match.concept.family == close[0].concept.family]
    candidates: list[MetricConcept] = [match.concept for match in close]
    if close:
        best_family = close[0].concept.family
        best_dataset = close[0].concept.dataset_id
        candidates.extend(
            concept
            for concept in concept_catalog()
            if concept.family == best_family and concept.dataset_id == best_dataset
        )
    else:
        candidates.extend(_topic_defaults(question))

    suggestions: list[str] = []
    selected: list[MetricConcept] = []
    if candidates and candidates[0].family == "subaward_flow":
        suggestions.extend(_flow_questions(question)[:max_items])
        selected.append(candidates[0])
    for concept in candidates:
        suggestion = question_for(concept, question)
        if suggestion in suggestions:
            continue
        suggestions.append(suggestion)
        selected.append(concept)
        if len(suggestions) >= max_items:
            break

    best = close[0] if close else None
    if intent == "UNANSWERABLE":
        if best:
            answer = (
                f"I couldn't find that exact term in the data library. The closest supported "
                f"measure is **{best.concept.label}**. Choose an option below, or tell me "
                "which place and period you want to analyze."
            )
        else:
            answer = (
                "I couldn't match that term to a variable in the current data library. "
                "I can still help you reach a supported answer—choose the closest direction "
                "below, or tell me the measure, place, and period you have in mind."
            )
        resolution = "unsupported"
    else:
        if best and best.score >= 0.86:
            answer = (
                f"I found a close supported measure: **{best.concept.label}**. "
                "Is that what you meant? Choose a question below or add the place and period."
            )
        else:
            answer = clarification or (
                "I can answer this, but I need the exact measure you mean. Choose one of the "
                "supported questions below, or tell me the measure, place, and period."
            )
        resolution = "needs_clarification"

    return {
        "answer": answer,
        "resolution": resolution,
        "confidence": "high" if suggestions else "medium",
        "suggestions": suggestions,
        "context_memory": {
            "original_question": question,
            "discovery_candidates": [
                {
                    "dataset": concept.dataset_id,
                    "variable": concept.variable,
                    "label": concept.label,
                }
                for concept in selected
            ],
        },
    }
