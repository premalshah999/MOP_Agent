"""Tiny domain glossary. Backend scans the answer text for matches and ships
only the terms actually mentioned in the response envelope, so the frontend
can render them as `<abbr title="...">` tooltips on hover.

Keep additions short — one line per term. Use the most familiar abbreviation
as the key; case-insensitive match at word boundaries.
"""

from __future__ import annotations

import re
from typing import Iterable

# Acronyms / source-name abbreviations the answer prompts commonly emit raw.
# Definitions are short and stakeholder-friendly, not academic.
_GLOSSARY: dict[str, str] = {
    "ACS": "American Community Survey — the Census Bureau's annual survey of household demographics.",
    "NFCS": "National Financial Capability Survey — FINRA Foundation's tri-annual household financial-literacy survey.",
    "BLS": "Bureau of Labor Statistics — federal source for employment and wage data.",
    "FY": "Fiscal year (federal: Oct 1 – Sep 30), not calendar year.",
    "GDP": "Gross domestic product — total market value of all goods and services produced.",
    "CBP": "County Business Patterns — Census series on firm count, employment, and payroll by county/industry.",
    "MSA": "Metropolitan Statistical Area — a Census-defined region around a population core.",
    "OOI": "Object of incidence — the entity (state, agency, recipient) a metric refers to.",
    "USASpending": "USASpending.gov — federal source for grants, contracts, and direct payments by recipient/agency.",
    "p25": "25th percentile — value below which 25% of observations fall.",
    "p75": "75th percentile — value below which 75% of observations fall.",
    "YoY": "Year-over-year change.",
}

_TERMS = list(_GLOSSARY.keys())


def _word_boundary_pattern(terms: Iterable[str]) -> re.Pattern[str]:
    # Case-sensitive: NFCS and "nfcs" are both fine; we anchor to the
    # uppercase form in the map but match anywhere.
    escaped = sorted((re.escape(t) for t in terms), key=len, reverse=True)
    return re.compile(r"\b(" + "|".join(escaped) + r")\b")


_PATTERN = _word_boundary_pattern(_TERMS)


def detect_terms(text: str) -> dict[str, str]:
    """Return only the glossary entries actually mentioned in `text`."""
    if not text:
        return {}
    seen: set[str] = set()
    for m in _PATTERN.finditer(text):
        # Normalize to the canonical map key (preserves NFCS over nfcs).
        for k in _TERMS:
            if k.lower() == m.group(0).lower():
                seen.add(k)
                break
    return {k: _GLOSSARY[k] for k in seen}


def get_glossary() -> dict[str, str]:
    """Full glossary, for the About page or admin debugging."""
    return dict(_GLOSSARY)
