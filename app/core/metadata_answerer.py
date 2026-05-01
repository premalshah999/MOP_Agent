from __future__ import annotations

from app.core.assistant_profile import PROFILE
from app.schemas.final_answer import FinalAnswer
from app.semantic.registry import load_registry


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
