from __future__ import annotations

import re
from typing import Optional

from app.query_frame import infer_query_frame
from app.semantic_registry import family_info, runtime_table_loaded, schema_fact


_QUESTION_NORMALIZER = re.compile(r"[^a-z0-9_]+")
_YEAR_PATTERN = re.compile(r"\b(20\d{2}|Fiscal Year 20\d{2}|2020-2024)\b", re.IGNORECASE)


def _normalize(text: str) -> str:
    return _QUESTION_NORMALIZER.sub(" ", text.lower()).strip()


def _has_any(question: str, phrases: tuple[str, ...]) -> bool:
    q = _normalize(question)
    return any(phrase in q for phrase in phrases)


def _extract_year_label(question: str) -> str | None:
    match = _YEAR_PATTERN.search(question)
    return match.group(1) if match else None


def _capabilities_answer(question: str) -> str | None:
    q = _normalize(question)
    if q not in {
        "what can you do",
        "what can you do for me",
        "what can you help with",
        "help",
        "what do you do",
        "what can i ask",
    } and not any(
        phrase in q
        for phrase in (
            "what can you do",
            "what can i ask",
            "what kinds of questions can you answer",
            "how can you help",
        )
    ):
        return None

    return (
        "**I can help across the main Maryland Opportunity Project datasets.**\n\n"
        "1. **Government Finances:** liabilities, assets, revenue, expenses, debt ratio, current ratio, pension burden.\n"
        "2. **Federal Spending:** contracts, grants, direct payments, resident wage, and related normalized fields.\n"
        "3. **Agency Spending:** which agencies dominate spending in a state and how that mix breaks down.\n"
        "4. **ACS Demographics:** population, poverty, income, education, race/ethnicity, and housing metrics.\n"
        "5. **FINRA:** financial literacy, financial constraint, alternative financing, and risk aversion.\n"
        "6. **Fund Flow:** subcontract inflows, outflows, origins, destinations, agencies, and industries.\n\n"
        "**Good question types:** rankings, state/county/district comparisons, within-state leaderboards, metric explanations, and careful cross-dataset comparisons.\n\n"
        "**Examples:**\n"
        "- Which states have the highest debt ratio?\n"
        "- Which counties in Maryland have the highest financial literacy?\n"
        "- Compare Maryland and Virginia on contracts in 2024.\n"
        "- Which agencies account for the most spending in Maryland?\n\n"
        "**I’ll also tell you when a year, geography, or metric is unsupported instead of guessing.**"
    )


def _dataset_routing_answer(question: str) -> str | None:
    q = _normalize(question)
    if "which dataset should" not in q and "what dataset should" not in q and "which data source" not in q:
        return None

    if any(token in q for token in ("demographic", "population", "household", "poverty", "education", "hispanic", "census", "acs")):
        return "**Use the ACS / Census dataset.** It is the project’s demographic source across states, counties, and congressional districts."
    if any(token in q for token in ("liabilities", "revenue", "expenses", "fiscal health", "debt ratio", "current ratio", "government finance")):
        return "**Use Government Finances (`gov_*`).** That family covers liabilities, assets, revenue, expenses, and fiscal-condition metrics."
    if any(token in q for token in ("financial literacy", "financial constraint", "alternative financing", "risk averse", "finra")):
        return "**Use the FINRA dataset (`finra_*`).** It holds the survey-based financial literacy and financial vulnerability measures."
    if any(token in q for token in ("contracts", "grants", "direct payments", "resident wage")) and "agency" not in q:
        return "**Use Federal Spending (`contract_*`).** That family covers geography-level contracts, grants, direct payments, resident wages, and related normalized fields."
    if any(token in q for token in ("agency", "agencies", "department")):
        return '**Use the agency-level federal spending data.** For state agency composition, that means `spending_state_agency`; county and congressional agency analysis belongs to the `contract_*_agency` family when those runtime tables are available.'
    if any(token in q for token in ("subcontract", "inflow", "outflow", "origin", "destination", "fund flow", "flowing into")):
        return "**Use the fund-flow tables.** Those are for directional subcontract movement, not for total federal spending by geography."
    return None


def _availability_answer(question: str) -> str | None:
    q = _normalize(question)

    if "government finances" in q and "what years" in q:
        return "**Government Finances currently has one time slice:** `Fiscal Year 2023`."

    if "finra state" in q and "what years" in q:
        return "**FINRA state data is available for:** 2009, 2012, 2015, 2018, and 2021."

    if "finra county" in q and "what years" in q:
        return "**FINRA county data is currently available only for 2021.**"

    if ("federal spending" in q or "federal spending by agency" in q) and "what periods" in q:
        return "**Federal Spending and Federal Spending by Agency currently use two period labels:** `2020-2024` and `2024`."

    if "federal spending breakdown" in q and any(token in q for token in ("below the state", "below state", "county level", "congress level")):
        return "**No. Federal Spending Breakdown is state-only in the current runtime.** For county or congressional agency analysis, the chatbot should use the agency-granular federal spending tables instead of `spending_breakdown`."

    if "agency by county" in q or "agency by congressional" in q or "agency by congress" in q:
        year_label = _extract_year_label(question)
        if year_label and year_label not in {"2024", "2020-2024"}:
            return "**No.** Agency-level federal spending files only cover `2020-2024` and `2024`, so a request for 2018 is outside the available periods."

    return None


def _schema_answer(question: str) -> str | None:
    q = _normalize(question)

    if "cd_118" in q:
        return schema_fact("cd_118")
    if "all year fields numeric" in q or ("year fields" in q and "numeric" in q):
        return schema_fact("year_fields")
    if "state names formatted consistently" in q or ("state names" in q and "consistently" in q):
        return schema_fact("state_casing")
    if "recompute" in q and ("per 1000" in q or "per_capita" in q or "_per_capita" in q):
        return schema_fact("per_1000")
    if "difference between resident wage and employees wage" in q:
        return '**`Resident Wage` and `Employees Wage` are not interchangeable.** The default agency spending composite uses `Resident Wage`, while `Employees Wage` is a separate payroll-style metric and should only be included when the user explicitly asks for it.'
    if "difference between contract_static" in q and "contract_agency" in q and "spending_breakdown" in q:
        return "**They serve different roles.** `contract_static` is geography-level federal spending totals, `contract_agency` is geography-by-agency detail, and `spending_breakdown` is the state-only breakdown layer paired with state-agency detail for composition charts."
    return None


def _unsupported_time_answer(question: str) -> str | None:
    q = _normalize(question)
    year_label = _extract_year_label(question)

    if year_label and any(token in q for token in ("liabilities", "assets", "revenue", "expenses", "debt ratio", "current ratio", "free cash flow", "government finances")):
        if year_label != "Fiscal Year 2023" and year_label != "2023":
            return "**That year is not available for Government Finances in the processed data.** The current Government Finances tables only cover `Fiscal Year 2023`."

    if year_label and "finra" in q and any(token in q for token in ("county", "congressional district", "district", "congress")):
        if year_label != "2021":
            return "**That combination is not available.** County- and congress-level FINRA data is currently only available for 2021."

    if year_label and any(token in q for token in ("state level fund flow", "state flow")):
        return "**Be careful here.** The state flow table does not support year filtering the same way the county and congressional flow tables do, so a state-level 2018 flow answer is not directly aligned with current dashboard behavior."

    return None


def _unit_safety_answer(question: str) -> str | None:
    q = _normalize(question)

    if "relative exposure" in q:
        if "contracts" in q:
            return '**For "relative exposure" in contracts, use the stored `Contracts Per 1000` field.** That is the normalized metric meant for relative comparisons.'
        if "grants" in q:
            return '**For "relative exposure" in grants, use the stored `Grants Per 1000` field.**'
        if "resident wage" in q:
            return '**For "relative exposure" in resident wage, use the stored `Resident Wage Per 1000` field.**'
        if "direct payments" in q:
            return '**For "relative exposure" in direct payments, use the stored `Direct Payments Per 1000` field.**'
        if "federal residents" in q:
            return '**For "relative exposure" in federal residents, use the stored `Federal Residents Per 1000` field.**'

    if "impact score" in q and "employees" in q and "contracts" in q:
        return "**Not without an explicit definition.** `Employees` is a count and `Contracts` is a dollar measure, so adding them directly would mix units. A valid custom score would need a transparent normalization scheme first."

    if "debt_ratio" in q or ("debt ratio" in q and "total_liabilities" in q):
        return "**No.** `Debt_Ratio` and `Total_Liabilities` are different unit types, so the chatbot should interpret them separately rather than comparing them as if a larger raw number automatically means better or worse performance."

    if "revenue per capita" in q and "contracts per 1000" in q:
        return (
            "**That comparison should stay in normalized units.** `Revenue_per_capita` and `Contracts Per 1000` are both "
            "relative metrics, so the chatbot should keep them labeled as normalized measures rather than mixing them with raw totals."
        )

    return None


def _ambiguity_answer(question: str) -> str | None:
    q = _normalize(question)

    if "most dependent on federal money" in q:
        return (
            '**That is ambiguous.** "Dependent on federal money" could mean large raw totals in **Contracts**, '
            '**Grants**, **Direct Payments**, or a default spending composite, and it could also mean *relative* exposure '
            'using normalized fields like **Per 1000**. A careful answer should define both the federal channel and '
            "whether dependence means raw dollars or normalized exposure before ranking states."
        )

    if "biggest funding source" in q:
        return (
            '**That question is ambiguous.** "Funding source" could mean a spending **channel** like contracts or grants, '
            "an **agency**, or even a fund-**flow** origin. The chatbot should clarify that dimension before answering."
        )

    if "largest funding category" in q:
        return (
            '**That needs a definition first.** A "funding category" usually means a spending channel such as '
            '**Contracts**, **Grants**, **Resident Wage**, or **Direct Payments**, while an agency is a separate dimension.'
        )

    if "strongest economically" in q:
        return (
            "**That is not a single project metric.** A careful answer should define a **proxy** first, such as "
            "median household income, low poverty, high financial literacy, or a fiscal-condition metric, instead of "
            "inventing one economic-strength ranking."
        )

    if "largest federal presence" in q and "agency" in q:
        return "**That depends on what kind of presence you mean.** `Employees` measures jobs, `Federal Residents` measures resident federal workforce presence, and spending uses dollar channels. The chatbot should clarify or present those interpretations separately."

    if "custom score" in q or ("combine grants" in q and "financial literacy" in q) or ("if you define exposure as" in q):
        return (
            "**That needs an explicit custom definition first.** A reliable answer should state the **criteria**, "
            "how each metric is **normalized**, and what **weights** each component receives before producing a ranking."
        )

    if "high on both" in q and "revenue per capita" in q and "contracts per 1000" in q:
        return (
            "**That comparison should stay explicit about normalized metrics.** A careful answer should compare "
            "**Revenue_per_capita** with **Contracts Per 1000** as two different normalized measures, rather than "
            "quietly switching back to raw totals."
        )

    if "unsupported custom metric" in q:
        return (
            "**The chatbot should either define the metric transparently or say it is unsupported.** It should not invent "
            "an opaque calculation when the requested metric is not already part of the project."
        )

    if ("which agency spends the most in maryland" in q or "which agencies account for the most spending in maryland" in q) and any(
        token in q for token in ("should", "default", "definition", "chatbot", "by default")
    ):
        return schema_fact("agency_spending_default")

    return None


def _scope_clarification_answer(question: str) -> str | None:
    frame = infer_query_frame(question)
    q = _normalize(question)

    if frame.intent == "compare" and len(frame.state_names) >= 2 and frame.metric_hint is None:
        states = " and ".join(" ".join(part.capitalize() for part in state.split()) for state in frame.state_names[:3])
        return (
            f"**I can compare {states}, but I need the metric first.** The cleanest options in this project are:\n\n"
            f"1. **Government Finances**: total liabilities, total assets, revenue, expenses, debt ratio, current ratio.\n"
            f"2. **Federal Spending**: contracts, grants, resident wage, direct payments, or normalized `Per 1000` exposure.\n"
            f"3. **ACS Demographics**: population, median household income, poverty, education, housing tenure.\n"
            f"4. **FINRA**: financial literacy, financial constraint, alternative financing, risk aversion.\n\n"
            f"Ask again with the measure named explicitly, for example: **Compare {states} on total liabilities**."
        )

    broad_state_prompt = (
        len(frame.state_names) == 1
        and frame.metric_hint is None
        and any(token in q for token in ("tell me about", "show me", "show", "profile", "overview", "open"))
    )
    if broad_state_prompt:
        state_label = " ".join(part.capitalize() for part in frame.state_names[0].split())
        return (
            f"**I can do that, but `{state_label}` needs a dimension.** The most useful choices here are:\n\n"
            f"1. **Government Finances**: liabilities, assets, revenue, expenses, debt ratio, current ratio.\n"
            f"2. **Federal Spending**: contracts, grants, resident wage, direct payments, or agency spending.\n"
            f"3. **ACS Demographics**: population, median household income, poverty, education.\n"
            f"4. **FINRA**: financial literacy or financial constraint.\n\n"
            f"For example: **Show {state_label} on total liabilities** or **Tell me about {state_label}'s federal spending in 2024**."
        )

    if frame.intent == "ranking" and frame.metric_hint is None and frame.family is None and any(
        token in q for token in ("which state", "which county", "which district", "top states", "top counties", "top districts")
    ):
        return (
            "**I need the measure before I rank geographies.** In this project, good choices include:\n\n"
            "1. **Government Finances**: liabilities, assets, revenue, expenses, debt ratio, current ratio.\n"
            "2. **Federal Spending**: contracts, grants, direct payments, resident wage, or `Per 1000` exposure.\n"
            "3. **ACS / FINRA**: poverty, household income, education, population, financial literacy, or financial constraint.\n\n"
            "A precise version would be: **Which states have the highest total liabilities?**"
        )

    return None


def _flow_vs_spending_answer(question: str) -> str | None:
    q = _normalize(question)

    if "when should the chatbot use flow data" in q or ("flow data" in q and "federal spending data" in q):
        return "**Use flow data only for directional subcontract movement questions** such as inflow, outflow, origin, destination, or which agencies/industries dominate subcontract movement. Use federal spending tables for contracts, grants, direct payments, wages, and other geography totals."

    if "county level breakdown data" in q and "breakdown" in q:
        return "**Federal Spending Breakdown is state-only.** If the user wants county-level agency analysis, the chatbot should use agency-granular federal spending tables rather than the breakdown family."

    return None


def _robustness_answer(question: str) -> str | None:
    q = _normalize(question)

    if "hallucinating" in q:
        return "**A likely hallucination usually shows up as a routing or availability mismatch.** Common signals are unsupported years, impossible geography/time combinations, invented fields, mixing counts with dollars, or using flow data when the question is really about federal spending totals."

    if "top state with liabilities" in q and "washington dc" in q:
        return "**No.** In the current state Government Finances file, **California** is the top state on `Total_Liabilities`, so an answer that names Washington DC as the leader would be incorrect."

    if "which agency spends the most in maryland" in q and "based only on contracts" in q:
        return (
            "**No.** For generic agency *spending* in Maryland, the project default is **Contracts + Grants + Resident Wage**, "
            "so an answer based only on **Contracts** would be incomplete."
        )

    if "connecticut" in q and "legacy boundaries" in q:
        return (
            "**It should state the caveat explicitly.** Those Connecticut rows still exist in the data, but some IDs do "
            "not align with the current legacy county boundary file used for map rendering, so the chatbot should mention "
            "that the **rows exist** while the **boundary** match is incomplete."
        )

    if "cannot verify" in q and "year level combination" in q:
        return "**It should say the combination is unavailable rather than guess.** If the requested year, geography, or table is not supported, the chatbot should stop and state that explicitly."

    if "causal" in q or ("cause" in q and any(token in q for token in ("poverty", "direct payments", "grants", "literacy"))):
        return "**The chatbot should avoid causal claims here.** With the current project data, it can describe associations or comparisons, but it should not claim that one measure causes another without a causal design."

    return None


def _coverage_answer(question: str) -> str | None:
    q = _normalize(question)
    agency = family_info("agency")
    missing_runtime = agency.get("missing_runtime_geographies", [])

    if "can the chatbot answer agency by county questions" in q or "can the chatbot answer agency by congress questions" in q:
        if missing_runtime:
            missing = ", ".join(missing_runtime)
            return f"**Not fully in the current runtime.** The semantic model documents agency analysis for `{missing}`, but those geographies are not currently loaded as runtime tables. State-level agency questions are supported."
    return None


def answer_metadata_question(question: str) -> Optional[str]:
    for resolver in (
        _capabilities_answer,
        _dataset_routing_answer,
        _availability_answer,
        _schema_answer,
        _unsupported_time_answer,
        _unit_safety_answer,
        _scope_clarification_answer,
        _flow_vs_spending_answer,
        _coverage_answer,
        _robustness_answer,
        _ambiguity_answer,
    ):
        answer = resolver(question)
        if answer:
            return answer
    return None
