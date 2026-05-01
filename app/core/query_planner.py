from __future__ import annotations

import re
from typing import Any

from app.core.ambiguity_resolver import resolve_unavailable_metric_proxy
from app.schemas.query_plan import Filter, QueryPlan, QuerySpec
from app.schemas.semantic_context import SemanticContext
from app.semantic.matcher import best_metric_match, normalized_question_tokens
from app.semantic.metric_variants import (
    asks_for_per_capita,
    looks_like_metric_variant_follow_up,
    select_metric_variant,
)
from app.semantic.registry import get_dataset, load_registry


_STATE_ABBREVIATIONS = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "DC": "District Of Columbia",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois",
    "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana",
    "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin",
    "WY": "Wyoming",
}
_STATE_NAMES = {value.lower() for value in _STATE_ABBREVIATIONS.values()}
_STATE_TO_ABBREVIATION = {value: key for key, value in _STATE_ABBREVIATIONS.items()}
_LOWERCASE_SAFE_ABBREVIATIONS = set(_STATE_ABBREVIATIONS) - {"IN", "OR", "ME"}
_UNSUPPORTED_CONCEPTS = {
    "crime": ["poverty rate", "financial constraint", "direct payments", "contracts", "grants"],
    "violent crime": ["poverty rate", "financial constraint", "direct payments"],
    "unemployment": ["poverty rate", "median household income", "financial constraint"],
    "schools": ["education attainment", "poverty rate", "median household income"],
    "health": ["poverty rate", "financial constraint", "median household income"],
}
_FUNDING_TERMS = ("funding", "federal money", "federal spending", "spending", "money", "grant", "grants", "contract", "contracts", "payment", "payments", "wage", "employee", "employees", "jobs", "employment")
_FLOW_TERMS = ("subaward", "subawards", "subcontract", "subcontracts", "flow", "flows", "inflow", "outflow")
_GOVERNMENT_FINANCE_TERMS = (
    "liabilities", "liability", "revenue", "debt", "cash flow", "expenses", "expense",
    "assets", "asset", "pension", "net position",
)
def _lower(question: str) -> str:
    return question.lower().strip()


def _geo_level(question: str) -> str:
    q = _lower(question)
    if "county" in q or "counties" in q:
        return "county"
    if "congress" in q or "district" in q:
        return "congress"
    return "state"


def _extract_states(question: str) -> list[str]:
    q = _lower(question)
    states: list[str] = []
    for state in sorted(_STATE_NAMES, key=len, reverse=True):
        if re.search(rf"\b{re.escape(state)}\b", q):
            states.append(state.title())
    for abbr, state in _STATE_ABBREVIATIONS.items():
        if re.search(rf"\b{abbr}\b", question):
            states.append(state)
        elif abbr in _LOWERCASE_SAFE_ABBREVIATIONS and re.search(rf"\b{abbr.lower()}\b", q):
            states.append(state)
    return list(dict.fromkeys(states))


def _state_abbreviation(state: str) -> str | None:
    return _STATE_TO_ABBREVIATION.get(state)


def _extract_top_k(question: str) -> int:
    q = _lower(question)
    word_numbers = {
        "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
        "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
        "fifteen": 15, "twenty": 20,
    }
    match = re.search(r"\b(?:top|first|largest|biggest|highest|lowest|bottom)\s+(\d+)\b", q)
    if match:
        return min(max(int(match.group(1)), 1), 100)
    word_match = re.search(r"\b(?:top|first|largest|biggest|highest|lowest|bottom)\s+(one|two|three|four|five|six|seven|eight|nine|ten|fifteen|twenty)\b", q)
    if word_match:
        return word_numbers[word_match.group(1)]
    trailing_match = re.search(r"\b(\d+)\s+(?:states?|counties|districts?|agencies)\b", q)
    if trailing_match:
        return min(max(int(trailing_match.group(1)), 1), 100)
    return 10


def _extract_year(question: str, dataset_id: str) -> str | int | None:
    dataset = get_dataset(dataset_id)
    if not dataset or not dataset.year_column:
        return None
    q = _lower(question)
    if "five year" in q or "5 year" in q or "2020-2024" in q:
        return "2020-2024"
    match = re.search(r"\b(20\d{2})\b", q)
    if match:
        year = match.group(1)
        return int(year) if any(isinstance(item, int) for item in dataset.available_years) else year
    return dataset.default_year


def _unsupported_alternatives(question: str) -> list[str] | None:
    q = _lower(question)
    for term, alternatives in _UNSUPPORTED_CONCEPTS.items():
        if re.search(rf"\b{re.escape(term)}\b", q):
            return alternatives
    return None


def _has_flow_signal(question: str) -> bool:
    q = _lower(question)
    if any(term in q for term in ("subaward", "subawards", "subcontract", "subcontracts", "inflow", "outflow", "fund flow", "fund flows")):
        return True
    return bool(re.search(r"\bflows?\b", q)) and "cash flow" not in q and "free cash flow" not in q


def _has_domain_signal(question: str) -> bool:
    q = _lower(question)
    token_set = normalized_question_tokens(question)
    signals = (
        *_FUNDING_TERMS,
        *_FLOW_TERMS,
        "poverty", "income", "population", "education", "bachelor", "bachelors", "college", "hispanic", "latino", "black", "asian", "white", "homeownership", "renters",
        *_GOVERNMENT_FINANCE_TERMS,
        "financial literacy", "literacy", "satisfaction", "constraint", "stress", "risk", "finra", "agency", "agencies", "department",
    )
    return (
        any(signal in q for signal in signals)
        or _has_flow_signal(question)
        or bool(token_set & {"poverty", "income", "population", "education", "bachelor", "college", "liability", "asset", "revenue", "expense", "literacy", "constraint"})
    )


def _is_ambiguous_money_lookup(question: str, intent: str) -> bool:
    q = _lower(question)
    broad_money = any(term in q for term in ("federal money", "funding", "spending", "money"))
    explicit_channel = any(term in q for term in ("grant", "contract", "direct payment", "resident wage", "subaward", "subcontract")) or _has_flow_signal(question)
    scoped_ranking = any(term in q for term in ("top", "highest", "maximum", "bottom", "lowest", "rank", "counties", "states", "agency", "agencies"))
    normalized_money_metric = asks_for_per_capita(question)
    return broad_money and not explicit_channel and not scoped_ranking and not normalized_money_metric and intent in {"DIRECT_LOOKUP", "AMBIGUOUS"}


def _definition_answer(question: str) -> QueryPlan:
    return QueryPlan(interpreted_question=question, intent="DEFINITION", queries=[])


def _history_text(history: list[dict[str, str]] | None) -> str:
    if not history:
        return ""
    return "\n".join(item.get("content", "") for item in history[-6:] if item.get("role") == "user")


def _choose_dataset(question: str, context: SemanticContext, intent: str) -> str | None:
    geo = _geo_level(question)
    q = _lower(question)
    registry = load_registry()

    if _has_flow_signal(question):
        dataset_id = f"{geo}_flow"
        return dataset_id if dataset_id in registry.datasets else None

    if ("agency" in q or "agencies" in q or "department" in q) and any(token in q for token in _FUNDING_TERMS):
        return "spending_state_agency"

    preferred_prefix = None
    if any(token in q for token in _FUNDING_TERMS):
        preferred_prefix = "contract"
    elif any(token in q for token in ("poverty", "income", "population", "education", "bachelor", "bachelors", "college", "hispanic", "latino", "black", "asian", "white", "homeownership", "renters")):
        preferred_prefix = "acs"
    elif any(token in q for token in _GOVERNMENT_FINANCE_TERMS):
        preferred_prefix = "gov"
    elif any(token in q for token in ("financial literacy", "fin lit", "literacy", "satisfaction", "constraint", "stress", "hardship", "risk", "finra")):
        preferred_prefix = "finra"

    if preferred_prefix:
        dataset_id = f"{preferred_prefix}_{geo}"
        if get_dataset(dataset_id):
            return dataset_id

    for item in context.datasets:
        dataset = get_dataset(item.dataset_id)
        if dataset and dataset.geography == geo:
            return item.dataset_id
    return context.datasets[0].dataset_id if context.datasets else None


def _choose_metric(dataset_id: str | None, question: str, context: SemanticContext) -> str | None:
    if not dataset_id:
        return None
    dataset = get_dataset(dataset_id)
    if not dataset:
        return None
    q = _lower(question)
    context_metric_ids = [hit.metric_id for hit in context.metrics if hit.dataset_id == dataset_id]
    match = best_metric_match(dataset, question, context_metric_ids=context_metric_ids)
    if match:
        return match.metric_id
    return None


def _metric_matches_elsewhere(question: str) -> list[tuple[str, str, float]]:
    matches: list[tuple[str, str, float]] = []
    for candidate in load_registry().datasets.values():
        match = best_metric_match(candidate, question, min_score=32.0)
        if match:
            matches.append((candidate.id, match.metric_id, match.score))
    return sorted(matches, key=lambda item: item[2], reverse=True)


def _unsupported_metric_geo_plan(question: str, dataset_id: str) -> QueryPlan | None:
    requested = get_dataset(dataset_id)
    if not requested:
        return None
    matches = _metric_matches_elsewhere(question)
    if not matches:
        return None
    best_dataset_id, best_metric_id, _score = matches[0]
    best_dataset = get_dataset(best_dataset_id)
    if not best_dataset or best_metric_id in requested.metrics:
        return None
    metric = best_dataset.metrics[best_metric_id]
    same_metric_datasets = []
    for candidate_id, metric_id, _ in matches:
        candidate = get_dataset(candidate_id)
        if candidate and metric_id == best_metric_id:
            same_metric_datasets.append(candidate)
    available_scopes = sorted({f"{candidate.display_name} ({candidate.geography})" for candidate in same_metric_datasets})
    requested_scope = requested.geography
    alternatives = [
        f"Use `{best_metric_id}` at an available level: {', '.join(available_scopes[:4])}.",
    ]
    if requested.id.startswith("contract_county") and best_metric_id.startswith("employees"):
        alternatives.extend(
            [
                "For counties, rank by Federal residents, Federal residents per 1,000, Resident wage, Grants, Contracts, or Direct payments.",
                "For Maryland employment by agency, ask: `rank agencies in Maryland by federal employees`.",
                "For state-level employment, ask: `rank states based on federal employees`.",
            ]
        )
    else:
        sample_metrics = ", ".join(metric.label for metric in list(requested.metrics.values())[:8])
        alternatives.append(f"For {requested.display_name}, available metrics include: {sample_metrics}.")
    return QueryPlan(
        interpreted_question=question,
        intent="UNANSWERABLE",
        datasets=[dataset_id],
        metrics=[best_metric_id],
        ambiguities=[
            (
                f"`{metric.label}` is supported in the loaded data, but not at the requested {requested_scope} level "
                f"for {requested.display_name}."
            )
        ],
        alternatives=alternatives,
    )


def _inherit_from_history(question: str, history: list[dict[str, str]] | None) -> tuple[str | None, str | None, str | None, list[str]]:
    inherited: list[str] = []
    current = question.strip()
    should_inherit = not _has_domain_signal(question) or looks_like_metric_variant_follow_up(question)
    history_items = [
        item
        for item in (history or [])
        if item.get("role") in {"user", "assistant"} and item.get("content") and item.get("content", "").strip() != current
    ]
    for index in range(len(history_items) - 1, -1, -1):
        item = history_items[index]
        contract = item.get("contract") or {}
        dataset_id = contract.get("family")
        metric_id = contract.get("metric")
        if not dataset_id or not metric_id:
            continue
        dataset = get_dataset(dataset_id)
        if not dataset or metric_id not in dataset.metrics:
            continue
        source_question = None
        for prior in reversed(history_items[: index + 1]):
            if prior.get("role") == "user":
                source_question = prior.get("content")
                break
        if should_inherit:
            inherited.append(f"Inherited dataset context from the recent conversation: {dataset_id}.")
            inherited.append(f"Inherited metric context from the recent conversation: {metric_id}.")
            return dataset_id, metric_id, source_question, inherited

    user_items = [
        item.get("content", "")
        for item in history_items
        if item.get("role") == "user" and item.get("content") and item.get("content", "").strip() != current
    ]
    if not user_items:
        return None, None, None, inherited
    dataset_id = None
    metric_id = None
    source_question = None
    for text in reversed(user_items[-6:]):
        context = SemanticContext()
        candidate_dataset = _choose_dataset(text, context, "DIRECT_LOOKUP")
        candidate_metric = _choose_metric(candidate_dataset, text, context)
        if candidate_dataset and candidate_metric and get_dataset(candidate_dataset) and candidate_metric in get_dataset(candidate_dataset).metrics:
            dataset_id = candidate_dataset
            metric_id = candidate_metric
            source_question = text
            break
    q = _lower(question)
    if dataset_id and should_inherit:
        inherited.append(f"Inherited dataset context from the recent conversation: {dataset_id}.")
    else:
        dataset_id = None
        source_question = None
    if metric_id and should_inherit:
        inherited.append(f"Inherited metric context from the recent conversation: {metric_id}.")
    else:
        metric_id = None
    return dataset_id, metric_id, source_question, inherited


def _filters_for(dataset_id: str, question: str, *, include_default_year: bool = True) -> tuple[list[Filter], list[str]]:
    dataset = get_dataset(dataset_id)
    if not dataset:
        return [], []
    filters: list[Filter] = []
    assumptions: list[str] = []
    states = _extract_states(question)
    if states and "state" in dataset.dimensions:
        filters.append(Filter(field="state", operator="IN" if len(states) > 1 else "=", value=states if len(states) > 1 else states[0]))
    elif states and "congressional_district" in dataset.dimensions:
        abbreviation = _state_abbreviation(states[0])
        if abbreviation:
            filters.append(Filter(field="congressional_district", operator="LIKE", value=f"{abbreviation}-%"))
            assumptions.append(f"Filtered congressional districts to {states[0]} using the {abbreviation}- district prefix.")
    if include_default_year and dataset.year_column:
        year = _extract_year(question, dataset_id)
        if year is not None and year != "Fiscal Year 2023":
            filters.append(Filter(field="year", operator="=", value=year))
            assumptions.append(f"Used the period {year} for {dataset.display_name}.")
        elif year == "Fiscal Year 2023":
            assumptions.append("Used the single available government-finance period: Fiscal Year 2023.")
    return filters, assumptions


def _dimension_for_query(dataset_id: str, question: str, intent: str) -> str:
    dataset = get_dataset(dataset_id)
    if not dataset:
        return "label"
    q = _lower(question)
    if "agency" in q or "agencies" in q or "department" in q:
        return "agency" if "agency" in dataset.dimensions else dataset.label_column
    if intent == "TREND" and "year" in dataset.dimensions:
        return "year"
    if "source" in q or "origin" in q or "from" in q:
        if "source_state" in dataset.dimensions:
            return "source_state"
        if "source_place" in dataset.dimensions:
            return "source_place"
    if "destination" in q or "recipient" in q or "to " in q or "inflow" in q:
        if "destination_state" in dataset.dimensions:
            return "destination_state"
        if "destination_place" in dataset.dimensions:
            return "destination_place"
    return dataset.label_column


def _should_default_to_ranking(dataset_id: str, question: str, filters: list[Filter], dimension: str) -> bool:
    dataset = get_dataset(dataset_id)
    if not dataset:
        return False
    q = _lower(question)
    if any(token in q for token in ("top", "highest", "maximum", "most", "largest", "rank", "ranked")):
        return True
    has_state_filter = any(filter_.field == "state" for filter_ in filters)
    plural_geo_requested = (
        ("counties" in q and dataset.geography == "county")
        or ("districts" in q and dataset.geography == "congress")
        or ("states" in q and dataset.geography == "state")
    )
    asks_for_scoped_geo = dataset.geography in {"county", "congress"} and (has_state_filter or plural_geo_requested)
    dimension_is_place = dimension in {dataset.label_column, "county", "congressional_district"}
    lookup_wording = any(phrase in q for phrase in ("how much", "what is", "what was", "show me", "give me"))
    return asks_for_scoped_geo and dimension_is_place and not lookup_wording


def _has_focus_filter(dataset_id: str, filters: list[Filter], dimension: str) -> bool:
    dataset = get_dataset(dataset_id)
    if not dataset:
        return False
    dimension_fields = {dimension, dataset.label_column}
    for dimension_id, definition in dataset.dimensions.items():
        if definition.column == dimension:
            dimension_fields.add(dimension_id)
    return any(filter_.field in dimension_fields and filter_.operator in {"=", "IN"} for filter_ in filters)


def _is_position_question(dataset_id: str, question: str, filters: list[Filter], dimension: str) -> bool:
    if not _has_focus_filter(dataset_id, filters, dimension):
        return False
    q = _lower(question)
    position_phrases = (
        "where does", "where do", "rank", "ranks", "standing", "stand", "position",
        "place nationally", "nationally", "compared to other", "compared with other",
        "relative to other", "among states", "among counties", "among districts",
    )
    return any(phrase in q for phrase in position_phrases)


def _flow_filters(dataset_id: str, question: str) -> tuple[list[Filter], list[str], str]:
    q = _lower(question)
    states = _extract_states(question)
    filters: list[Filter] = []
    assumptions: list[str] = []
    dimension = "destination_state"
    if "outflow" in q or "from" in q:
        dimension = "destination_state"
        if states:
            filters.append(Filter(field="source_state", operator="=", value=states[0]))
            assumptions.append(f"Interpreted the flow as outgoing from {states[0]}.")
    elif "inflow" in q or "to" in q or "goes to" in q:
        dimension = "source_state"
        if states:
            filters.append(Filter(field="destination_state", operator="=", value=states[-1]))
            assumptions.append(f"Interpreted the flow as incoming to {states[-1]}.")
    if dataset_id in {"county_flow", "congress_flow"}:
        dimension = "source_place" if dimension == "source_state" else "destination_place"
    dataset = get_dataset(dataset_id)
    year = _extract_year(question, dataset_id)
    if dataset and dataset.year_column and year is not None:
        filters.append(Filter(field="year", operator="=", value=year))
        assumptions.append(f"Used fiscal year {year} for fund-flow data.")
    return filters, assumptions, dimension


def _make_query(
    *,
    operation: QuerySpec.model_fields["operation"].annotation,
    dataset_id: str,
    metric_id: str,
    dimension: str,
    filters: list[Filter],
    question: str,
    intent: str,
) -> QuerySpec:
    q = _lower(question)
    return QuerySpec(
        name="primary",
        purpose="Answer the resolved analytical request from the curated dataset.",
        dataset=dataset_id,
        operation=operation,
        metric=metric_id,
        dimensions=[dimension],
        filters=filters,
        order="ASC" if any(token in q for token in ("lowest", "minimum", "bottom", "least")) else "DESC",
        limit=_extract_top_k(question) if operation in {"ranking", "breakdown", "flow_ranking"} or intent in {"AGGREGATION", "BREAKDOWN"} else None,
    )


def create_query_plan(
    question: str,
    intent_payload: dict[str, Any],
    context: SemanticContext,
    history: list[dict[str, str]] | None = None,
) -> QueryPlan:
    intent = intent_payload["intent"]
    q = _lower(question)
    if intent == "DEFINITION":
        return _definition_answer(question)

    unsupported = _unsupported_alternatives(question)
    if unsupported:
        return QueryPlan(
            interpreted_question=question,
            intent="UNANSWERABLE",
            ambiguities=["That concept is not available in the loaded runtime datasets."],
            alternatives=unsupported,
        )

    if intent_payload.get("mode") == "CLARIFICATION_RESPONSE":
        q = _lower(question)
        previous_question = intent_payload.get("clarifies_question") or _history_text(history)
        states = _extract_states(previous_question)
        if any(phrase in q for phrase in ("first", "option one", "total federal funding", "received by the geography")):
            dataset_id = f"contract_{_geo_level(previous_question)}"
            if get_dataset(dataset_id):
                filters, assumptions = _filters_for(dataset_id, previous_question, include_default_year=True)
                metric_id = "total_federal_funding"
                metric = get_dataset(dataset_id).metrics[metric_id]
                assumptions.append("Resolved the prior clarification as total federal funding received by the geography.")
                assumptions.append(f"Interpreted the metric as {metric.label}.")
                query = _make_query(
                    operation="lookup" if states else "ranking",
                    dataset_id=dataset_id,
                    metric_id=metric_id,
                    dimension=get_dataset(dataset_id).label_column,
                    filters=filters,
                    question=previous_question,
                    intent="DIRECT_LOOKUP",
                )
                query.limit = None if states else 10
                return QueryPlan(
                    interpreted_question=f"Direct lookup for total federal funding using {get_dataset(dataset_id).display_name}.",
                    intent="DIRECT_LOOKUP",
                    datasets=[dataset_id],
                    metrics=[metric_id],
                    dimensions=[get_dataset(dataset_id).label_column],
                    filters=filters,
                    queries=[query],
                    assumptions=assumptions,
                )
        if "subcontract" in q or "flow" in q or "second" in q:
            rewritten = f"subcontract inflow to {' and '.join(states) if states else previous_question}"
            intent = "DIRECT_LOOKUP"
            question = rewritten
        elif "grant" in q or "contract" in q or "direct payment" in q or "third" in q:
            rewritten = f"{question} in {' and '.join(states)}" if states else question
            intent = "DIRECT_LOOKUP"
            question = rewritten

    if _is_ambiguous_money_lookup(question, intent):
        return QueryPlan(
            interpreted_question=question,
            intent="AMBIGUOUS",
            ambiguities=[
                "Federal money is ambiguous: it can mean total geography-level funding, directional subcontract/fund flow, or a specific channel such as grants or contracts."
            ],
            alternatives=[
                "total federal funding received by the geography",
                "subcontract/fund-flow inflow or outflow",
                "a specific channel such as grants, contracts, direct payments, or resident wages",
            ],
        )

    if intent == "AMBIGUOUS" and intent_payload.get("mode") == "FOLLOW_UP_ANALYTICS":
        if looks_like_metric_variant_follow_up(question) or any(
            term in q
            for term in (
                "distribution", "ranking", "rank", "top", "maximum", "minimum", "amount based",
                "based on amount", "ratio based", "based on ratio", "percentage", "percent", "share",
                "per capita", "per-capita", "per 1000", "per 1,000", "per thousand", "p/c",
            )
        ):
            intent = "AGGREGATION"
        else:
            intent = "DIRECT_LOOKUP"
    elif intent == "AMBIGUOUS":
        intent = "DIRECT_LOOKUP"

    inherited_dataset, inherited_metric, inherited_question, inherited_assumptions = _inherit_from_history(question, history)
    should_use_inherited_shape = inherited_question and (not _has_domain_signal(question) or looks_like_metric_variant_follow_up(question))
    shape_question = f"{inherited_question} {question}" if should_use_inherited_shape else question
    if intent_payload.get("mode") == "FOLLOW_UP_ANALYTICS" and not inherited_metric and (not _has_domain_signal(question) or looks_like_metric_variant_follow_up(question)):
        return QueryPlan(
            interpreted_question=question,
            intent="AMBIGUOUS",
            ambiguities=[
                "This looks like a follow-up correction, but I do not have the prior analytical result in the current request context."
            ],
            alternatives=[
                "Restate the metric and geography, for example: `rank states by Asian population count`.",
                "Continue inside the same chat thread so I can carry the prior dataset and metric forward.",
            ],
        )
    should_prefer_inherited = not _has_domain_signal(question) or looks_like_metric_variant_follow_up(question)
    if inherited_dataset and should_prefer_inherited:
        dataset_id = inherited_dataset
    else:
        dataset_id = _choose_dataset(question, context, intent) or inherited_dataset
    current_metric = _choose_metric(dataset_id, question, context)
    if inherited_metric and should_prefer_inherited:
        metric_id = inherited_metric
    else:
        metric_id = current_metric or inherited_metric
    resolver_assumptions: list[str] = []

    if _has_flow_signal(question):
        dataset_id = dataset_id or f"{_geo_level(question)}_flow"
        metric_id = "subaward_amount"

    if dataset_id and not metric_id:
        matches_elsewhere = _metric_matches_elsewhere(question)
        proxy = resolve_unavailable_metric_proxy(
            question=question,
            dataset_id=dataset_id,
            matches_elsewhere=matches_elsewhere,
        )
        if proxy:
            metric_id = proxy.metric_id
            resolver_assumptions.extend(proxy.assumptions)
        unsupported_metric_geo = None if proxy else _unsupported_metric_geo_plan(question, dataset_id)
        if unsupported_metric_geo:
            return unsupported_metric_geo

    variant_selection = select_metric_variant(get_dataset(dataset_id) if dataset_id else None, metric_id, question)
    metric_override_assumption: str | None = None
    if variant_selection:
        prior_metric = metric_id
        metric_id = variant_selection.metric_id
        metric_override_assumption = (
            f"Switched from `{prior_metric}` to `{metric_id}` because the question asked for a {variant_selection.reason} variant of the same metric concept."
        )

    if not dataset_id or not metric_id:
        return QueryPlan(
            interpreted_question=question,
            intent="AMBIGUOUS",
            datasets=[dataset_id] if dataset_id else [],
            ambiguities=["I could not resolve a supported dataset and metric from the question."],
            alternatives=["Try poverty, income, grants, contracts, direct payments, financial literacy, debt ratio, or subaward flow."],
        )

    dataset = get_dataset(dataset_id)
    if not dataset:
        return QueryPlan(interpreted_question=question, intent="UNANSWERABLE", ambiguities=[f"Dataset `{dataset_id}` is not loaded."])

    if dataset.family == "fund_flow":
        filters, assumptions, dimension = _flow_filters(dataset_id, shape_question)
        operation = "flow_ranking"
    else:
        include_year = intent not in {"TREND", "ROOT_CAUSE"}
        filters, assumptions = _filters_for(dataset_id, shape_question, include_default_year=include_year)
        dimension = _dimension_for_query(dataset_id, shape_question, intent)
        operation = "ranking"
        if intent == "ROOT_CAUSE" and dataset.family in {"federal_funding", "federal_spending"}:
            return QueryPlan(
                interpreted_question=question,
                intent="AMBIGUOUS",
                datasets=[dataset_id],
                metrics=[metric_id],
                ambiguities=[
                    "The loaded federal funding tables have a 2024 row and a 2020-2024 aggregate row, not annual causal drivers. I should not describe that as a true trend or root cause."
                ],
                alternatives=[
                    "current 2024 ranking",
                    "agency breakout where loaded",
                    "2024 versus 2020-2024 aggregate with an explicit caveat",
                    "fund-flow inflow/outflow analysis",
                ],
            )
        if intent == "ROOT_CAUSE" and dataset.year_column and len(dataset.available_years) > 2:
            operation = "trend"
            dimension = "year"
            assumptions.append("Used a period-over-period diagnostic because root-cause analysis must be grounded in available dimensions.")
        elif _is_position_question(dataset_id, shape_question, filters, dimension):
            operation = "position"
        elif dimension == "agency":
            operation = "breakdown"
        elif intent == "DIRECT_LOOKUP":
            operation = "ranking" if _should_default_to_ranking(dataset_id, shape_question, filters, dimension) else "lookup"
        elif intent == "COMPARISON":
            operation = "compare"
        elif intent == "TREND":
            operation = "trend"
            dimension = "year"
        elif intent == "BREAKDOWN":
            operation = "breakdown"

    assumptions.extend(inherited_assumptions)
    assumptions.extend(resolver_assumptions)
    metric = dataset.metrics[metric_id]
    if metric_override_assumption:
        assumptions.append(metric_override_assumption)
    assumptions.append(f"Interpreted the metric as {metric.label}.")
    if intent == "COMPARISON" and len(_extract_states(shape_question)) < 2 and "state" in dataset.dimensions:
        return QueryPlan(
            interpreted_question=question,
            intent="AMBIGUOUS",
            datasets=[dataset_id],
            metrics=[metric_id],
            ambiguities=["A comparison needs at least two named geographies or entities."],
            alternatives=["Example: compare Maryland vs Virginia on grants."],
        )

    query = _make_query(
        operation=operation,
        dataset_id=dataset_id,
        metric_id=metric_id,
        dimension=dimension,
        filters=filters,
        question=shape_question,
        intent=intent,
    )
    return QueryPlan(
        interpreted_question=f"{intent.replace('_', ' ').title()} for {metric.label} using {dataset.display_name}.",
        intent=intent,
        datasets=[dataset_id],
        metrics=[metric_id],
        dimensions=[dimension],
        filters=filters,
        queries=[query],
        assumptions=assumptions,
    )
