from __future__ import annotations

import re
from typing import Any

from app.schemas.final_answer import FinalAnswer, KeyNumber
from app.schemas.query_plan import QueryPlan
from app.semantic.registry import get_dataset, load_registry


def _format_number(value: Any, unit: str | None) -> str:
    if not isinstance(value, (int, float)):
        return str(value)
    if unit == "dollars" or (unit and unit.startswith("dollars")):
        abs_value = abs(float(value))
        if abs_value >= 1_000_000_000:
            return f"${value / 1_000_000_000:.2f}B"
        if abs_value >= 1_000_000:
            return f"${value / 1_000_000:.2f}M"
        if abs_value >= 1_000:
            return f"${value / 1_000:.2f}K"
        return f"${value:,.0f}"
    if unit == "percent":
        return f"{value:.2f}%"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return f"{value:,}"


def _format_label(value: Any) -> str:
    label = str(value)
    if label.isupper() and not re.match(r"^[A-Z]{2}-\d+", label):
        return label.title()
    if label.islower():
        titled = label.title()
        for token in (" Of ", " And ", " The "):
            titled = titled.replace(token, token.lower())
        return titled
    return label


def _definition_answer(question: str) -> FinalAnswer:
    q = question.lower()
    registry = load_registry()
    if "who are you" in q or "what are you" in q:
        answer = (
            "I'm the Maryland Opportunity Project analytics assistant. I answer through approved metadata, validated query plans, "
            "whitelisted DuckDB views, executed results, and known caveats. I can handle rankings, comparisons, lookups, trends, "
            "agency breakouts, fund-flow questions, definitions, and dataset availability."
        )
        return FinalAnswer(answer=answer, confidence="high")
    if "year" in q or "available" in q:
        narrowed = []
        for dataset in registry.datasets.values():
            dataset_text = f"{dataset.id} {dataset.display_name}".lower()
            if ("finra" in q and dataset.id.startswith("finra_")) or ("county" in q and dataset.geography == "county") or dataset.id in q:
                narrowed.append(dataset)
        if "finra" in q and "county" in q:
            narrowed = [dataset for dataset in registry.datasets.values() if dataset.id == "finra_county"]
        targets = narrowed or list(registry.datasets.values())
        lines = ["Available periods in the loaded runtime tables:"]
        for dataset in targets:
            if dataset.available_years:
                years = ", ".join(str(item) for item in dataset.available_years)
                lines.append(f"- {dataset.id}: {years}")
        return FinalAnswer(answer="\n".join(lines), confidence="high")
    if "metric" in q or "field" in q or "what can" in q:
        lines = ["Supported metric families include:"]
        families: dict[str, set[str]] = {}
        for dataset in registry.datasets.values():
            families.setdefault(dataset.family, set()).update(metric.label for metric in dataset.metrics.values())
        for family, metrics in sorted(families.items()):
            sample = ", ".join(sorted(metrics)[:8])
            lines.append(f"- {family}: {sample}")
        return FinalAnswer(answer="\n".join(lines), confidence="high")
    answer = (
        "I can answer grounded analytics questions over the loaded Maryland Opportunity Project datasets. "
        "Good examples: `top 10 counties in Maryland by funding`, `compare Maryland vs Virginia on grants`, "
        "`trend financial literacy by year`, or `subcontract inflow to Maryland`."
    )
    return FinalAnswer(answer=answer, confidence="high")


def _scope_line(plan: QueryPlan) -> str:
    if not plan.queries:
        return ""
    filters = []
    for filter_ in plan.queries[0].filters:
        if filter_.field == "year":
            filters.append(f"period {filter_.value}")
        elif filter_.operator == "IN":
            values = ", ".join(str(value) for value in filter_.value)
            filters.append(f"{filter_.field} in {values}")
        else:
            filters.append(f"{filter_.field} {filter_.operator} {filter_.value}")
    return f"Scope: {', '.join(filters)}." if filters else ""


def _scope_text(plan: QueryPlan) -> str:
    return _scope_line(plan).removeprefix("Scope: ").rstrip(".")


def _ranked_table_lines(rows: list[dict[str, Any]], unit: str | None, metric_label: str) -> list[str]:
    lines = [f"| Rank | Geography | {metric_label} |", "|---:|---|---:|"]
    for row in rows:
        lines.append(f"| {int(row['rank'])} | {_format_label(row['label'])} | {_format_number(row['metric_value'], unit)} |")
    return lines


def _bullet_value_lines(rows: list[dict[str, Any]], unit: str | None) -> list[str]:
    return [f"- {_format_label(row['label'])}: {_format_number(row['metric_value'], unit)}" for row in rows]


def _proxy_notes(plan: QueryPlan) -> list[str]:
    return [
        item
        for item in plan.assumptions
        if "proxy" in item.lower() or "not loaded" in item.lower()
    ]


def _filter_value(plan: QueryPlan, field: str) -> Any | None:
    if not plan.queries:
        return None
    for filter_ in plan.queries[0].filters:
        if filter_.field == field and filter_.operator == "=":
            return filter_.value
    return None


def _methodology_line(query_operation: str, metric_label: str) -> str:
    if query_operation == "trend":
        return f"I grouped the validated rows by period and calculated {metric_label} for each returned period."
    if query_operation == "compare":
        return f"I filtered to the requested comparison entities, calculated {metric_label}, and ordered the results from highest to lowest."
    if query_operation == "breakdown":
        return f"I grouped the validated rows by the requested breakdown dimension and calculated {metric_label} for each group."
    if query_operation == "flow_ranking":
        return f"I treated this as directional subaward flow and summed the approved subaward amount field by counterparty."
    if query_operation == "position":
        return f"I calculated {metric_label} for the full peer set first, ranked those peers, then pulled out the requested geography."
    if query_operation == "lookup":
        return f"I filtered to the requested scope and calculated {metric_label} from the approved metric definition."
    return f"I ranked the requested geography by {metric_label} using the approved metric definition."


def _next_questions(dataset_id: str, metric_id: str, operation: str) -> list[str]:
    dataset = get_dataset(dataset_id)
    if not dataset:
        return []
    if operation in {"lookup", "position"} and dataset.geography == "state":
        return [
            f"Compare Maryland vs Virginia on {metric_id.replace('_', ' ')}.",
            "Show the top 10 states for this metric.",
            "Break this down by agency if agency data is loaded.",
        ]
    if operation in {"ranking", "breakdown"}:
        return [
            "Show this as a map.",
            "Compare the top result with Maryland.",
            "Switch to a per-capita/per-1,000 version if available.",
        ]
    if operation == "flow_ranking":
        return [
            "Show outflow instead of inflow.",
            "Filter this flow by agency.",
            "Show the same flow at county level if available.",
        ]
    return ["Ask for a comparison, ranking, or definition for this metric."]


def _answer_for_rows(plan: QueryPlan, rows: list[dict[str, Any]], dataset_id: str, metric_id: str) -> tuple[list[str], list[KeyNumber]]:
    dataset = get_dataset(dataset_id)
    metric = dataset.metrics[metric_id] if dataset else None
    metric_label = metric.label if metric else "metric"
    unit = metric.unit if metric else None
    query = plan.queries[0]
    key_numbers = [KeyNumber(label=_format_label(row["label"]), value=row["metric_value"], unit=unit) for row in rows[:5]]
    scope = _scope_text(plan)
    proxy_notes = _proxy_notes(plan)

    if query.operation in {"ranking", "breakdown", "flow_ranking"}:
        noun = "results"
        if dataset:
            noun = "counties" if dataset.geography == "county" else "states" if dataset.geography == "state" else "districts"
        if query.operation == "breakdown":
            dimension = query.dimensions[0] if query.dimensions else ""
            if dimension == "agency":
                noun = "agencies"
            elif dimension in {"congressional_district", "cd_118"}:
                noun = "districts"
            elif dimension == "county":
                noun = "counties"
            elif dimension == "state":
                noun = "states"
            else:
                noun = dimension.replace("_", " ") or noun
        rank_word = "bottom" if query.order == "ASC" else "top"
        direction_word = "lowest" if query.order == "ASC" else "highest"
        agency = _filter_value(plan, "agency")
        if agency and metric_id == "contracts":
            lines = [
                (
                    f"I can answer the aggregate version of this: the {rank_word} {len(rows)} {noun} by "
                    f"**{agency} contract dollars**. The loaded table is agency-by-geography totals, so it does not expose individual deal or award records."
                )
            ]
        else:
            lines = [
                f"I read this as a {rank_word} {len(rows)} {noun} ranking by **{metric_label}**.",
            ]
        if proxy_notes:
            lines.extend(
                [
                    "",
                    f"One important data note up front: {proxy_notes[0]} "
                    "So this answer stays grounded in the county-level runtime table, but it should be read as a proxy analysis rather than a direct employee-count ranking.",
                ]
            )
        if scope:
            lines.append(f"The scope I used is **{scope}**.")
        lines.extend(
            [
                "",
                f"Here is the {direction_word}-to-{('highest' if query.order == 'ASC' else 'lowest')} ranking returned by the validated query:",
                "",
                *_ranked_table_lines(rows, unit, metric_label),
                "",
                "The main pattern is fairly concentrated at the top of the list.",
            ]
        )
        top_value = rows[0]["metric_value"]
        second_value = rows[1]["metric_value"] if len(rows) > 1 else None
        lines.append(f"{_format_label(rows[0]['label'])} is #1 with {_format_number(top_value, unit)}.")
        if second_value is not None:
            gap = abs(top_value - second_value)
            pct_gap = gap / second_value * 100 if second_value else None
            gap_sentence = (
                f"The gap between #1 and #2 is {_format_number(gap, unit)}"
                + (f", or about {pct_gap:.1f}% relative to #2." if pct_gap is not None else ".")
            )
            lines.append(gap_sentence)
        if len(rows) >= 3:
            returned_total = sum(float(row["metric_value"] or 0) for row in rows)
            top_three = sum(float(row["metric_value"] or 0) for row in rows[:3])
            if returned_total:
                lines.append(
                    f"The top three entries account for {top_three / returned_total * 100:.1f}% of the value among these returned top {len(rows)} rows."
                )
    elif query.operation == "compare":
        lines = [
            f"I treated this as a side-by-side comparison for **{metric_label}**.",
        ]
        if scope:
            lines.append(f"The comparison scope is **{scope}**.")
        lines.extend(["", "Here are the returned values:", "", *_ranked_table_lines(rows, unit, metric_label)])
        if len(rows) >= 2:
            delta = rows[0]["metric_value"] - rows[1]["metric_value"]
            pct = delta / rows[1]["metric_value"] * 100 if rows[1]["metric_value"] else None
            lines.extend(
                [
                    "",
                    f"{_format_label(rows[0]['label'])} is higher than {_format_label(rows[1]['label'])} by {_format_number(delta, unit)}.",
                ]
            )
            if pct is not None:
                lines.append(f"That is about {pct:.1f}% higher than {_format_label(rows[1]['label'])}.")
    elif query.operation == "trend":
        first = rows[0]
        last = rows[-1]
        delta = last["metric_value"] - first["metric_value"]
        pct = delta / first["metric_value"] * 100 if first["metric_value"] else None
        lines = [
            f"I read this as a trend question for **{metric_label}**.",
            f"Across the returned periods, the value moved from {_format_number(first['metric_value'], unit)} in {_format_label(first['label'])} to {_format_number(last['metric_value'], unit)} in {_format_label(last['label'])}.",
            "",
            f"That is a net change of {_format_number(delta, unit)}"
            + (f", or {pct:.1f}%." if pct is not None else "."),
            "",
            "Period-by-period values:",
            "",
            "| Period | Value |",
            "|---|---:|",
        ]
        lines.extend(f"| {_format_label(row['label'])} | {_format_number(row['metric_value'], unit)} |" for row in rows)
    elif query.operation == "position":
        row = rows[0]
        label = _format_label(row["label"])
        total_count = int(row["total_count"]) if row.get("total_count") is not None else None
        peer_average = row.get("peer_average")
        direct = f"{label} ranks #{int(row['rank'])}"
        if total_count:
            direct += f" of {total_count}"
        direct += f" for **{metric_label}**, with a value of {_format_number(row['metric_value'], unit)}."
        lines = [
            direct,
            "",
            f"I ranked the full peer set first, then pulled out {label}; that avoids the common mistake of filtering to the focus geography before ranking.",
            f"Rank logic: #1 is the {'lowest' if query.order == 'ASC' else 'highest'} value for this metric.",
        ]
        if peer_average is not None:
            delta = row["metric_value"] - peer_average
            direction = "above" if delta >= 0 else "below"
            lines.append(f"Peer average: {_format_number(peer_average, unit)}; {label} is {_format_number(abs(delta), unit)} {direction} that average.")
            if peer_average:
                lines.append(f"Relative difference from the peer average: {delta / peer_average * 100:.1f}%.")
        if row.get("peer_max") is not None and row.get("peer_min") is not None:
            lines.append(f"The peer range runs from {_format_number(row['peer_min'], unit)} to {_format_number(row['peer_max'], unit)}.")
    else:
        row = rows[0]
        receives_money = dataset and dataset.family in {"federal_funding", "federal_spending", "fund_flow"} and metric and metric.unit == "dollars"
        verb = "received" if receives_money else "has"
        label = _format_label(row["label"])
        value = _format_number(row["metric_value"], unit)
        if metric_id == "employees":
            direct_sentence = f"{label} has **{value} federal employees** in the loaded 2024 state-level employment data."
        elif metric_id == "federal_residents":
            direct_sentence = f"{label} has **{value} federal residents** in the loaded county-level federal presence data."
        else:
            direct_sentence = f"{label} {verb} **{metric_label}** of **{value}**."
        lines = [
            direct_sentence,
            "",
            "I treated this as a scoped lookup rather than a ranking. The value comes from the approved metric definition and the filtered runtime view.",
        ]

    if query.operation in {"lookup", "position"} and scope:
        lines.extend(["", f"Scope used: **{scope}**."])
    if metric:
        lines.extend(
            [
                "",
                "How I calculated it:",
                f"- Metric: **{metric.label}**.",
                f"- Definition: {metric.description}",
                f"- Unit: {metric.unit}; aggregation: {metric.aggregation}.",
            ]
        )
        if query.operation in {"ranking", "breakdown", "flow_ranking", "position"}:
            direction = "lowest to highest" if query.order == "ASC" else "highest to lowest"
            lines.append(f"- Ranking direction: {direction}.")
    lines.extend(["", f"Methodology note: {_methodology_line(query.operation, metric_label)}"])
    next_questions = _next_questions(dataset_id, metric_id, query.operation)
    if next_questions:
        lines.extend(["", "Useful follow-ups:", *[f"- {item}" for item in next_questions[:3]]])
    return lines, key_numbers


def _relevant_caveats(dataset_id: str, metric_id: str, raw_caveats: list[str], warnings: list[str]) -> list[str]:
    caveats = list(raw_caveats)
    funding_metric_ids = {
        "total_federal_funding",
        "contracts",
        "grants",
        "resident_wage",
        "direct_payments",
        "contracts_per_1000",
        "grants_per_1000",
        "resident_wage_per_1000",
        "direct_payments_per_1000",
    }
    if dataset_id.startswith("contract_") and metric_id not in funding_metric_ids:
        caveats = [
            caveat
            for caveat in caveats
            if "Broad funding defaults" not in caveat and "Funding tables expose" not in caveat
        ]
    return caveats + warnings


def generate_answer(question: str, plan: QueryPlan, executions: list[dict[str, Any]], verification: dict[str, Any]) -> FinalAnswer:
    if plan.intent == "DEFINITION":
        return _definition_answer(question)
    if plan.intent == "UNANSWERABLE":
        alternatives = plan.alternatives or ["Ask for one of the supported public-policy metrics."]
        alternative_lines = "\n".join(f"- {item}" for item in alternatives)
        reason = plan.ambiguities[0] if plan.ambiguities else "The requested concept is not available in the loaded runtime datasets."
        return FinalAnswer(
            answer=(
                f"**Direct answer:** I cannot answer that exact request from the loaded runtime datasets.\n\n"
                f"**Why**\n"
                f"- {reason}\n\n"
                f"**What I can answer instead**\n"
                f"{alternative_lines}"
            ).strip(),
            assumptions=plan.assumptions,
            confidence="high",
        )
    if plan.intent == "AMBIGUOUS":
        ambiguity = plan.ambiguities[0] if plan.ambiguities else "I could not resolve that to a supported dataset and metric."
        alternatives = "\n".join(f"- {item}" for item in plan.alternatives)
        answer = f"I need one more detail before I can answer reliably. {ambiguity}"
        if alternatives:
            answer += f"\n\nValid interpretations:\n{alternatives}"
        return FinalAnswer(answer=answer, assumptions=plan.assumptions, confidence="low")

    execution = executions[0] if executions else {"rows": [], "sql": ""}
    rows = execution.get("rows") or []
    dataset = get_dataset(plan.datasets[0]) if plan.datasets else None
    if not rows:
        return FinalAnswer(
            answer="I ran the validated query, but it returned no rows for the requested scope.",
            assumptions=plan.assumptions,
            caveats=(dataset.caveats if dataset else []) + verification.get("warnings", []),
            sql_used=[execution.get("sql", "")] if execution.get("sql") else [],
            confidence="low",
        )

    lines, key_numbers = _answer_for_rows(plan, rows, plan.datasets[0], plan.metrics[0])
    if plan.assumptions:
        lines.extend(["", "Important assumptions I used:", *[f"- {item}" for item in plan.assumptions]])
    caveats = _relevant_caveats(
        plan.datasets[0],
        plan.metrics[0],
        list(dataset.caveats if dataset else []),
        verification.get("warnings", []),
    )
    if caveats:
        lines.extend(["", "Data caveats:", *[f"- {item}" for item in caveats[:5]]])

    return FinalAnswer(
        answer="\n".join(lines),
        key_numbers=key_numbers,
        assumptions=plan.assumptions,
        caveats=caveats,
        sql_used=[item.get("sql", "") for item in executions if item.get("sql")],
        confidence="high" if verification.get("status") == "ok" else "medium",
    )
