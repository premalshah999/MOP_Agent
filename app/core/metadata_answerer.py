from __future__ import annotations

from app.core.assistant_profile import PROFILE
from app.schemas.final_answer import FinalAnswer
from app.semantic.registry import load_registry


_FAMILY_DEFINITIONS = {
    "finra": {
        "title": "FINRA financial capability",
        "summary": (
            "FINRA refers here to the FINRA Investor Education Foundation financial capability data loaded into this assistant. "
            "It is used for state, county, and congressional-district indicators such as financial literacy, financial satisfaction, "
            "financial constraint, alternative financing, and risk aversion."
        ),
        "family": "finra",
    },
    "acs": {
        "title": "ACS / Census demographics",
        "summary": (
            "ACS means American Community Survey data. In this assistant, ACS tables cover demographics and socioeconomic indicators "
            "such as population, race and ethnicity shares/counts, poverty, household income, education, homeownership, and renters."
        ),
        "family": "acs",
    },
    "census": {
        "title": "ACS / Census demographics",
        "summary": (
            "Census questions in this assistant are answered from the processed ACS demographic tables, not from live Census APIs."
        ),
        "family": "acs",
    },
    "government finance": {
        "title": "Government finance",
        "summary": (
            "Government finance tables describe fiscal-position metrics such as assets, liabilities, revenue, expenses, debt ratio, "
            "current ratio, pension liability, net position, and free cash flow."
        ),
        "family": "government_finance",
    },
    "federal spending": {
        "title": "Federal spending / agency breakdown",
        "summary": (
            "Federal spending tables summarize contracts, grants, direct payments, resident wages, employees, and related per-1,000 metrics. "
            "The agency table can break those measures down by department or agency."
        ),
        "family": "federal_spending",
    },
    "federal funding": {
        "title": "Federal funding by geography",
        "summary": (
            "Federal funding tables summarize geography-level contracts, grants, resident wages, direct payments, federal residents, "
            "and employment-related measures where available."
        ),
        "family": "federal_funding",
    },
    "fund flow": {
        "title": "Subaward fund flow",
        "summary": (
            "Fund flow means directional subaward or subcontract movement between places. It is different from total federal spending received by a geography."
        ),
        "family": "fund_flow",
    },
}


def _family_definition_answer(q: str) -> FinalAnswer | None:
    registry = load_registry()
    matched = next((definition for term, definition in _FAMILY_DEFINITIONS.items() if term in q), None)
    if not matched:
        return None
    datasets = [dataset for dataset in registry.datasets.values() if dataset.family == matched["family"]]
    metrics = sorted({metric.label for dataset in datasets for metric in dataset.metrics.values()})
    periods = []
    for dataset in datasets:
        if dataset.available_years:
            periods.append(f"{dataset.id}: {', '.join(str(item) for item in dataset.available_years)}")
    caveats = []
    for dataset in datasets:
        for caveat in dataset.caveats:
            if caveat not in caveats:
                caveats.append(caveat)
    lines = [
        f"**{matched['title']}**",
        "",
        matched["summary"],
        "",
        "Loaded runtime tables:",
        *[f"- `{dataset.id}` ({dataset.geography})" for dataset in datasets],
        "",
        "Common metrics I can answer with:",
        "- " + ", ".join(metrics[:10]) if metrics else "- Metadata only.",
    ]
    if periods:
        lines.extend(["", "Available periods:", *[f"- {item}" for item in periods]])
    if caveats:
        lines.extend(["", "Important caveats:", *[f"- {item}" for item in caveats[:3]]])
    return FinalAnswer(answer="\n".join(lines), confidence="high", caveats=caveats[:3])


def assistant_help_answer() -> FinalAnswer:
    lines = [
        f"I’m the {PROFILE.name}.",
        "",
        PROFILE.purpose,
        "",
        "I can help with:",
        *[f"- {item}" for item in PROFILE.capabilities],
        "",
        "Good questions to try:",
        *[f"- {item}" for item in PROFILE.example_questions],
        "",
        "How I work:",
        *[f"- {item}" for item in PROFILE.limitations],
    ]
    return FinalAnswer(answer="\n".join(lines), confidence="high")


def dataset_discovery_answer() -> FinalAnswer:
    registry = load_registry()
    families: dict[str, list[str]] = {}
    for dataset in registry.datasets.values():
        families.setdefault(dataset.family, []).append(dataset.id)
    family_labels = {
        "acs": "ACS / Census demographics",
        "government_finance": "Government finance",
        "finra": "FINRA financial capability",
        "federal_funding": "Federal funding by geography",
        "federal_spending": "Federal spending / agency breakdown",
        "fund_flow": "Subaward fund flow",
    }
    lines = [
        "I can currently answer questions from these approved data areas:",
        "",
    ]
    for family, dataset_ids in sorted(families.items()):
        metrics = []
        for dataset_id in dataset_ids:
            dataset = registry.datasets[dataset_id]
            metrics.extend(metric.label for metric in dataset.metrics.values())
        sample_metrics = ", ".join(sorted(set(metrics))[:8])
        lines.extend(
            [
                f"**{family_labels.get(family, family.replace('_', ' ').title())}**",
                f"- Runtime tables: {', '.join(sorted(dataset_ids))}",
                f"- Example metrics: {sample_metrics or 'metadata only'}",
                "",
            ]
        )
    lines.extend(
        [
            "Examples:",
            "- Top 10 counties in Maryland by federal funding.",
            "- Compare Maryland vs Virginia on grants.",
            "- Which agencies provide the most grants to Maryland?",
            "- Subcontract inflow to Maryland.",
            "- What years are available for FINRA county data?",
        ]
    )
    return FinalAnswer(answer="\n".join(lines), confidence="high")


def metric_definition_answer(question: str) -> FinalAnswer:
    q = question.lower()
    registry = load_registry()
    family_answer = _family_definition_answer(q)
    if family_answer:
        return family_answer
    matches = []
    for dataset in registry.datasets.values():
        for metric in dataset.metrics.values():
            phrases = [metric.id.replace("_", " "), metric.label.lower(), *metric.synonyms, *metric.default_for]
            if any(phrase and phrase.lower() in q for phrase in phrases):
                matches.append((dataset, metric))
    if "year" in q or "available" in q:
        targets = list(registry.datasets.values())
        if "finra" in q:
            targets = [dataset for dataset in targets if dataset.family == "finra"]
        if "county" in q:
            targets = [dataset for dataset in targets if dataset.geography == "county"]
        lines = ["Available periods in the loaded runtime tables:"]
        for dataset in targets:
            if dataset.available_years:
                lines.append(f"- {dataset.id}: {', '.join(str(item) for item in dataset.available_years)}")
        return FinalAnswer(answer="\n".join(lines), confidence="high")
    if not matches:
        return dataset_discovery_answer()
    lines = ["Here is how I understand that metric:"]
    for dataset, metric in matches[:5]:
        lines.extend(
            [
                "",
                f"**{metric.label}** in `{dataset.id}`",
                f"- Definition: {metric.description}",
                f"- SQL expression: `{metric.sql}`",
                f"- Unit: {metric.unit}",
                f"- Default aggregation: {metric.aggregation}",
            ]
        )
        if dataset.caveats:
            lines.append(f"- Caveat: {dataset.caveats[0]}")
    return FinalAnswer(answer="\n".join(lines), confidence="high")


def out_of_scope_answer() -> FinalAnswer:
    return FinalAnswer(
        answer=(
            "I’m focused on the approved Maryland Opportunity Project analytics datasets, so I cannot help with that request directly.\n\n"
            "I can still help with public-policy data questions such as funding rankings, ACS demographics, government finance, FINRA financial capability, agency breakouts, and fund-flow analysis."
        ),
        confidence="high",
    )


def conversation_repair_answer() -> FinalAnswer:
    return FinalAnswer(
        answer=(
            "You’re right to call that out. I should not keep running the previous state-level interpretation after you correct the scope.\n\n"
            "What went wrong: I over-carried the earlier state-level context instead of treating your follow-up as a correction. "
            "For a correction like “I meant counties in Maryland,” the assistant should rebuild the query with county geography, keep only the still-valid metric concept, and clearly say when it has to use a documented proxy.\n\n"
            "A better corrected question would be: **rank Maryland counties by employment-related federal presence**. "
            "Because direct county-level employee counts are not loaded, I should answer with the county-level proxy **Federal residents** and state that assumption up front."
        ),
        confidence="high",
    )
