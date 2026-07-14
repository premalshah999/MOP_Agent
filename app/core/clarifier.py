"""Concrete-suggestion generator for under-specified questions.

When the routing/intent stages decide they can't answer a question without
more info, today the orchestrator returns a bare "Which dataset and measure
should I use?" — which is the most-common production failure (per the
3,411-query audit: 'how much federal money to Maryland' alone hits 129x,
'best state'/'compare X vs Y' without a metric hits 56x).

This module produces 3–5 concrete, clickable alternatives the user can
pick instead of retyping. Hybrid design:

1. Pattern-match the 3 highest-frequency ambiguity shapes (money / ranking
   / comparison without metric) → hardcoded, dataset-aware chips. Fast,
   deterministic, works when DeepSeek is down.
2. Fall back to an LLM call grounded in `domain_summary()` for anything
   else, so even novel ambiguous questions get useful chips.
"""

from __future__ import annotations

import re

from app.llm import client
from app.semantic.registry import domain_summary


# Pattern detectors. Order matters; the first match wins.
_MONEY_RE = re.compile(
    r"(how much|what.*amount|federal money|federal funding|how much money)",
    re.IGNORECASE,
)
_RANKING_RE = re.compile(
    # Lenient: "best X" / "top X" / "best counties for X" / "highest financial literacy"
    # all need ranking chips; metric is implied missing.
    r"\b(best|top|highest|lowest|worst|leading|ranking?|rank)\b",
    re.IGNORECASE,
)
_COMPARE_RE = re.compile(
    r"\b(compare|comparison|vs\.?|versus|better|worse)\b",
    re.IGNORECASE,
)
_STATE_RE = re.compile(
    # 50 states + DC. Compact; covers most common cases users will type.
    r"\b(alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|"
    r"florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|"
    r"maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|"
    r"nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|"
    r"north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|"
    r"south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|"
    r"wisconsin|wyoming|district of columbia|dc)\b",
    re.IGNORECASE,
)


def _focus_state(question: str) -> str | None:
    m = _STATE_RE.search(question)
    return m.group(0).title() if m else None


def _focus_states(question: str, max_n: int = 2) -> list[str]:
    """All distinct state names in the question (preserves order)."""
    seen: list[str] = []
    for m in _STATE_RE.finditer(question):
        v = m.group(0).title()
        if v not in seen:
            seen.append(v)
        if len(seen) >= max_n:
            break
    return seen


def _money_chips(question: str) -> list[str]:
    state = _focus_state(question) or "Maryland"
    return [
        f"How much in federal contracts did {state} receive in FY2023?",
        f"How much in federal grants did {state} receive in FY2023?",
        f"How much in direct payments did {state} receive in FY2023?",
        f"Total federal spending in {state} in FY2023 (contracts + grants + payments)",
        f"Top 5 federal agencies funding {state}",
    ]


def _ranking_chips(question: str) -> list[str]:
    """User asked 'best/top/highest X states' without specifying X."""
    return [
        "Top 5 states by median household income (2023)",
        "Top 5 states by federal grants received (FY2023)",
        "Top 5 states by financial literacy index (NFCS 2021)",
        "Top 5 states by government debt ratio (FY2023)",
        "Top 5 states with the lowest poverty rate (2023)",
    ]


def _compare_chips(question: str) -> list[str]:
    """User asked 'compare X vs Y' but didn't pick a measure.

    Uses BOTH states from the question when present; otherwise pairs the
    focus state with Virginia (or Pennsylvania if MD already mentioned).
    """
    states = _focus_states(question, max_n=2)
    if len(states) >= 2:
        a, b = states[0], states[1]
    else:
        a = states[0] if states else "Maryland"
        b = "Virginia" if a.lower() != "virginia" else "Pennsylvania"
    return [
        f"Compare {a} and {b} on median household income (2023)",
        f"Compare {a} and {b} on poverty rate (2023)",
        f"Compare {a} and {b} on federal grants received (FY2023)",
        f"Compare {a} and {b} on government debt ratio (FY2023)",
        f"Compare {a} and {b} on financial literacy index (2021)",
    ]


_LLM_SYSTEM = """You suggest 3-5 short, concrete follow-up questions a user
could ask instead of their ambiguous question. EVERY suggestion MUST be
answerable using ONLY the catalog below — never invent metrics, geographies,
or year ranges that aren't present.

Each suggestion: one sentence, specific (named state/year/metric), no
question marks, no quoting the user's original phrasing back to them.

Return ONLY JSON: {"suggestions": ["...", "...", "..."]}"""


def _llm_chips(question: str) -> list[str]:
    """Fall back to a grounded LLM call for ambiguity shapes we didn't pattern-match."""
    try:
        catalog = domain_summary()
    except Exception:
        catalog = ""
    try:
        raw = client.chat_json(
            [
                {"role": "system", "content": _LLM_SYSTEM},
                {
                    "role": "user",
                    "content": (
                        f"CATALOG:\n{catalog[:3500]}\n\n"
                        f"User's ambiguous question: {question}\n\n"
                        "Suggest 3-5 specific follow-ups."
                    ),
                },
            ],
            temperature=0.2,
            max_tokens=400,
            purpose="clarifier_llm",
        )
    except client.LLMError:
        # Last-ditch generic chips so the user always gets *something*.
        return [
            "Top 5 states by median household income (2023)",
            "Compare Maryland and Virginia on federal grants",
            "What was Maryland's poverty rate in 2023?",
        ]
    items = raw.get("suggestions") or []
    out = [str(s).strip() for s in items if str(s).strip()]
    return out[:5]


def generate_clarification_chips(question: str) -> list[str]:
    """Return 3-5 concrete questions the user can click instead of retyping.

    Pattern-matches the three highest-frequency ambiguity shapes from the
    production log (money / ranking / comparison without a metric) and falls
    back to an LLM call for everything else.
    """
    if _MONEY_RE.search(question):
        return _money_chips(question)
    if _RANKING_RE.search(question):
        return _ranking_chips(question)
    if _COMPARE_RE.search(question):
        return _compare_chips(question)
    return _llm_chips(question)
