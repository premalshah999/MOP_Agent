"""Deterministic guard against cross-dataset period hallucinations."""

from __future__ import annotations

import re
from typing import Any

from app.semantic.registry import get_dataset


_YEAR_RE = re.compile(r"(?<!\d)(20\d{2})(?!\d)")


def _human_label(value: str) -> str:
    return " ".join(value.replace("_", " ").replace(",", " ").replace("&", "and").split())


def period_claim_issues(
    answer: str,
    caveats: list[str],
    tables: list[str],
    metric_columns: list[str],
) -> list[str]:
    """Find government-finance metrics attributed to a year other than FY2023.

    Government-finance tables are fixed FY2023 snapshots. In cross-dataset
    answers an LLM can incorrectly carry another table's year into a government
    metric label (for example, ``Debt Ratio (2021)``). The regular faithfulness
    judge is probabilistic, so this invariant is enforced mechanically.
    """
    gov_tables = [table for table in tables if table.startswith("gov_")]
    if not gov_tables:
        return []

    labels: set[str] = set()
    for table in gov_tables:
        dataset = get_dataset(table)
        if dataset is None:
            continue
        for column in metric_columns:
            metric = dataset.metrics.get(column)
            if metric is not None:
                labels.add(_human_label(metric.label))
                labels.add(_human_label(metric.id))
    if not labels:
        return []

    text = "\n".join([answer or "", *(caveats or [])])
    issues: list[str] = []
    for label in sorted(labels, key=len, reverse=True):
        if len(label) < 4:
            continue
        pattern = re.compile(re.escape(label), re.IGNORECASE)
        for match in pattern.finditer(text):
            # Inspect only the claim following this metric name, stopping at a
            # sentence/table boundary so another dataset's nearby year does
            # not become a false positive.
            tail = text[match.end() : match.end() + 100]
            boundary = re.search(r"[|\n.!?]", tail)
            segment = tail[: boundary.start()] if boundary else tail
            for year_match in _YEAR_RE.finditer(segment):
                year = int(year_match.group(1))
                prefix = segment[max(0, year_match.start() - 8) : year_match.start()].casefold()
                if year != 2023 and "not " not in prefix:
                    issue = f"{label} was attributed to {year}; government-finance data is FY2023"
                    if issue not in issues:
                        issues.append(issue)
    return issues


def canonical_period_notes(tables: list[str]) -> list[str]:
    notes: list[str] = []
    if any(table.startswith("gov_") for table in tables):
        notes.append("Government-finance measures use the fixed FY2023 snapshot.")
    if "finra_state" in tables:
        notes.append("FINRA state measures use the survey year selected in the query.")
    return notes


def mixed_period_note(tables: list[str], effective_period: Any) -> str:
    """Return a user-facing note when joined datasets represent different periods."""
    family_periods: list[tuple[str, str]] = []
    seen_families: set[str] = set()
    period_map = effective_period if isinstance(effective_period, dict) else {}
    for table in tables:
        dataset = get_dataset(table)
        if dataset is None or dataset.family in seen_families:
            continue
        seen_families.add(dataset.family)
        if table.startswith("gov_"):
            family_periods.append(("Government-finance measures", "FY2023"))
            continue
        value = period_map.get(table, dataset.default_year)
        if value in (None, "", "catalog snapshot"):
            continue
        if table.startswith("finra_"):
            family_periods.append(("FINRA measures", f"survey year {value}"))
        elif table.startswith(("contract_", "spending_")):
            family_periods.append(("Federal-funding measures", f"FY{value}"))
        elif table.startswith("acs_"):
            family_periods.append(("Census measures", str(value)))
        else:
            family_periods.append((dataset.display_name, str(value)))
    if len({period for _, period in family_periods}) < 2:
        return ""
    clauses = [f"{family} use {period}" for family, period in family_periods]
    if len(clauses) == 2:
        joined = f"{clauses[0]}; {clauses[1]}"
    else:
        joined = "; ".join(clauses)
    return f"{joined}. These are different source periods, not same-year observations."
