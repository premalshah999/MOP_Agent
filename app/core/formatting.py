"""Numeric formatting for answer key_numbers.

The LLM emits {label, value, unit} where value is usually a raw number.
Render-ready text is more consistent if we format server-side using simple
heuristics on the label + unit, rather than relying on the LLM to do it.
"""

from __future__ import annotations

import re
from typing import Any


_CURRENCY_UNIT = {"usd", "$", "dollar", "dollars", "us dollars"}
_PCT_UNIT = {"%", "percent", "percentage", "pct"}
_COUNT_UNIT = {
    "people", "person", "persons", "household", "households", "firms",
    "businesses", "establishments", "count", "counts", "n", "records",
    "units", "jobs",
}

_CURRENCY_LABEL = re.compile(
    r"(income|wage|earnings?|revenue|spending|funding|grants?|contracts?|"
    r"awards?|appropriations?|gdp|value|price|cost|budget|payroll|sales|"
    r"receipts?|dollars?|expenditures?)",
    re.IGNORECASE,
)
_PCT_LABEL = re.compile(
    r"(rate|share|percent|percentage|proportion|ratio|prevalence|coverage)",
    re.IGNORECASE,
)
_COUNT_LABEL = re.compile(
    r"(count|number|firms|establishments|households|population|workers|"
    r"employees|jobs|filings|applications|recipients|beneficiaries)",
    re.IGNORECASE,
)


def _to_float(v: Any) -> float | None:
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        # Strip $, %, commas, spaces — if the LLM already pre-formatted, recover the number.
        s = v.strip().replace(",", "").replace("$", "").replace("%", "").strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def fmt_currency(n: float) -> str:
    """`$1,234,567`; compact for billions / trillions so callouts don't overflow."""
    a = abs(n)
    sign = "-" if n < 0 else ""
    if a >= 1e12:
        return f"{sign}${a / 1e12:.2f}T"
    if a >= 1e9:
        return f"{sign}${a / 1e9:.2f}B"
    if a >= 1e8:
        # Hundreds of millions read better compact too
        return f"{sign}${a / 1e6:.0f}M"
    # Up to ~$100M: full number with commas — keeps cents off integer dollars
    if a == int(a):
        return f"{sign}${int(a):,}"
    return f"{sign}${a:,.2f}"


def fmt_pct(n: float) -> str:
    """Heuristic: 0–1 → treat as fraction (0.825 → 82.5%); else assume already a percent."""
    val = n * 100 if -1.0 <= n <= 1.0 and n != 0 else n
    # 1 decimal unless the value is a clean integer
    if abs(val - round(val)) < 1e-9:
        return f"{int(round(val))}%"
    return f"{val:.1f}%"


def fmt_count(n: float) -> str:
    a = abs(n)
    sign = "-" if n < 0 else ""
    if a >= 1e9:
        return f"{sign}{a / 1e9:.2f}B"
    if a >= 1e7:
        return f"{sign}{a / 1e6:.1f}M"
    if a == int(a):
        return f"{sign}{int(a):,}"
    return f"{sign}{a:,.2f}"


def format_key_number(item: dict[str, Any]) -> dict[str, Any]:
    """Return a new dict with `value` rewritten as a render-ready string and
    `unit` cleared when the format already encodes the unit ($, %)."""
    label = str(item.get("label", "") or "")
    unit_raw = str(item.get("unit", "") or "").strip()
    unit_lc = unit_raw.lower()
    n = _to_float(item.get("value"))

    if n is None:
        # Non-numeric value — pass through untouched.
        return {"label": label, "value": item.get("value"), "unit": unit_raw}

    is_currency = unit_lc in _CURRENCY_UNIT or (not unit_lc and bool(_CURRENCY_LABEL.search(label)))
    is_pct = unit_lc in _PCT_UNIT or (not unit_lc and bool(_PCT_LABEL.search(label)))
    is_count = unit_lc in _COUNT_UNIT or (not unit_lc and bool(_COUNT_LABEL.search(label)))

    if is_currency:
        return {"label": label, "value": fmt_currency(n), "unit": ""}
    if is_pct:
        return {"label": label, "value": fmt_pct(n), "unit": ""}
    if is_count:
        return {"label": label, "value": fmt_count(n), "unit": unit_raw if unit_lc not in _COUNT_UNIT else ""}

    # Plain numeric with a non-currency/non-pct unit (e.g. "years", "miles"):
    # add commas to the number but keep the unit visible.
    return {"label": label, "value": fmt_count(n), "unit": unit_raw}


def format_key_numbers(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [format_key_number(it) for it in (items or [])]


# ---------------------------------------------------------------------------
# Row-existence validation for key_numbers.
#
# The LLM produces key_numbers freely — without server-side validation it can
# fabricate arithmetic (e.g. "Top 5 combined: $3.65B" when the actual sum is
# $3.70B). The faithfulness judge looks at prose; key_numbers are structured
# data displayed prominently in the UI callout, so a wrong value here is a
# very visible hallucination. This helper checks each item's value against
# the raw rows (or a small set of derivable aggregates) and drops any that
# can't be traced — caller decides whether to downgrade confidence too.
# ---------------------------------------------------------------------------

def _row_numeric_pool(rows: list[dict[str, Any]]) -> set[float]:
    """Collect every numeric scalar in the returned rows for membership tests."""
    pool: set[float] = set()
    for r in rows or []:
        for v in r.values():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                pool.add(float(v))
    return pool


def _row_aggregate_pool(rows: list[dict[str, Any]]) -> set[float]:
    """Sums + means + min + max for every numeric column — covers the common
    LLM moves of presenting `Top N combined`, `average across listed rows`,
    `largest`, `smallest`. Only computed when there are <=60 rows (the
    answer prompt caps shown rows at 60 anyway)."""
    if not rows or len(rows) > 60:
        return set()
    by_col: dict[str, list[float]] = {}
    for r in rows:
        for k, v in r.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                by_col.setdefault(k, []).append(float(v))
    agg: set[float] = set()
    for vals in by_col.values():
        if not vals:
            continue
        agg.add(sum(vals))
        agg.add(sum(vals) / len(vals))  # mean
        agg.add(min(vals))
        agg.add(max(vals))
    return agg


def validate_key_numbers_against_rows(
    items: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    *,
    rel_tol: float = 0.01,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Return `(kept, dropped_labels)`. An item is kept when its numeric value
    is within `rel_tol` (default ±1%) of any value in the rows OR any obvious
    aggregate (sum / mean / min / max per numeric column).

    Items with no numeric value are kept as-is (they're descriptors, not
    claims). Items with peer-context-only meaning (rank labels like
    "National rank", "of 51") are also kept — those are validated separately
    by the peer_context module."""
    if not items:
        return [], []
    if not rows:
        # No rows -> can't validate anything. Keep items but caller can
        # downgrade confidence based on len(dropped) == 0 and rows == [].
        return list(items), []

    pool = _row_numeric_pool(rows) | _row_aggregate_pool(rows)
    if not pool:
        return list(items), []

    def _is_rank_label(label: str) -> bool:
        lc = label.lower()
        return any(t in lc for t in ("rank", "place", "of 5", "of 50", "of 51", "of 52", "of 56"))

    kept: list[dict[str, Any]] = []
    dropped: list[str] = []
    for it in items:
        raw_val = it.get("value")
        if _is_rank_label(str(it.get("label", ""))):
            kept.append(it)  # peer_context owns rank validation
            continue
        n = _to_float(raw_val)
        if n is None:
            kept.append(it)
            continue
        if any(abs(n - rv) <= rel_tol * max(1.0, abs(rv)) for rv in pool):
            kept.append(it)
        else:
            dropped.append(str(it.get("label", "")) or "unknown")
    return kept, dropped
