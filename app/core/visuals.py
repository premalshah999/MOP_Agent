"""Visualization layer.

Deterministic (no LLM). Given the executed SQL rows + routing context it infers:
  - an inline Vega-Lite chart (bar / grouped bar / line / histogram)
  - a ChatbotMapIntent so the MapLibre choropleth (MapPreview) can color the
    /geo boundary GeoJSON straight from these rows.

It is intentionally defensive: any uncertainty -> no chart / map disabled. It
must never raise into the pipeline (the orchestrator also guards it).
"""

from __future__ import annotations

import re
from typing import Any

# Row keys that are identifiers/dimensions, never the measure to plot.
_NON_MEASURE = {
    "state", "county", "cd_118", "fips", "state_fips", "county_fips", "year",
    "Year", "act_dt_fis_yr", "agency", "agency_name", "rcpt_state_name",
    "subawardee_state_name", "rcpt_cd_name", "subawardee_cd_name",
    "rcpt_cty_name", "subawardee_cty_name", "rcpt_state", "subawardee_state",
    "label", "rank", "Unnamed: 0",
}
_GEO_LABEL_PRIORITY = (
    "county", "cd_118", "rcpt_cd_name", "subawardee_cd_name",
    "rcpt_state_name", "subawardee_state_name", "state", "agency", "label",
)
_YEAR_KEYS = ("Year", "year", "act_dt_fis_yr")
_MONEY_HINT = re.compile(
    r"contract|grant|payment|wage|fund|amount|asset|liabilit|revenue|expense|"
    r"spend|income|bond|opeb|pension|cash|debt(?!_ratio)|subaward",
    re.I,
)
_DISTRIBUTION_Q = re.compile(r"distribut|histogram|spread|how .*vary|variation|range of", re.I)
_COMPARE_Q = re.compile(r"\bcompare\b|\bvs\b|\bversus\b|\bbetween\b", re.I)
_TREND_Q = re.compile(r"\btrend\b|over time|over the years|by year|year over year|each year", re.I)

_US_STATES = (
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "district of columbia", "florida", "georgia",
    "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky",
    "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada", "new hampshire",
    "new jersey", "new mexico", "new york", "north carolina", "north dakota",
    "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
    "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "west virginia", "wisconsin", "wyoming",
)


def _state_in_question(question: str) -> str | None:
    q = question.lower()
    for name in sorted(_US_STATES, key=len, reverse=True):
        if re.search(rf"\b{re.escape(name)}\b", q):
            return name.title()
    return None


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _numeric_columns(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return []
    cols: list[str] = []
    for key in rows[0]:
        if key in _NON_MEASURE:
            continue
        vals = [r.get(key) for r in rows if r.get(key) is not None]
        if vals and sum(1 for v in vals if _is_number(v)) >= max(1, len(vals) // 2):
            cols.append(key)
    return cols


def _label_column(rows: list[dict[str, Any]], numeric: list[str]) -> str | None:
    keys = list(rows[0].keys())
    for cand in _GEO_LABEL_PRIORITY:
        if cand in keys:
            return cand
    for key in keys:
        if key not in numeric and key not in _YEAR_KEYS:
            return key
    return None


def _year_column(rows: list[dict[str, Any]]) -> str | None:
    keys = rows[0].keys()
    for k in _YEAR_KEYS:
        if k in keys:
            distinct = {r.get(k) for r in rows if r.get(k) is not None}
            if len(distinct) >= 2:
                return k
    return None


def _fmt(measure: str) -> str | None:
    return "~s" if _MONEY_HINT.search(measure) else None


def _axis(measure: str) -> dict[str, Any]:
    ax: dict[str, Any] = {"title": measure.replace("_", " ")}
    f = _fmt(measure)
    if f:
        ax["format"] = f
    return ax


def _spec_base(title: str) -> dict[str, Any]:
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "title": title,
        "width": "container",
        "height": 260,
        "autosize": {"type": "fit", "contains": "padding"},
    }


def build_charts(
    question: str,
    routing: dict[str, Any],
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return a list of ChartBlock dicts ({title, subtitle, spec}); [] if none fit."""
    if not rows or len(rows) < 2:
        return []
    numeric = _numeric_columns(rows)
    if not numeric:
        return []
    measure = numeric[0]
    year_col = _year_column(rows)
    pretty_m = measure.replace("_", " ")

    # Trend -> line (a time series needs no categorical label column)
    if year_col and len({r.get(year_col) for r in rows}) >= 2:
        data = [
            {"year": str(r.get(year_col)), "value": r.get(measure)}
            for r in rows if _is_number(r.get(measure))
        ]
        if len(data) >= 2:
            spec = _spec_base(f"{pretty_m} over time")
            spec.update({
                "data": {"values": data},
                "mark": {"type": "line", "point": True},
                "encoding": {
                    "x": {"field": "year", "type": "ordinal", "axis": {"title": "Year"}},
                    "y": {"field": "value", "type": "quantitative", "axis": _axis(measure)},
                },
            })
            return [{"title": f"{pretty_m} trend", "subtitle": "", "spec": spec}]

    label_col = _label_column(rows, numeric)
    if not label_col:
        return []
    plotted = rows[:25]
    data = [
        {"label": str(r.get(label_col)), "value": r.get(measure)}
        for r in plotted if _is_number(r.get(measure))
    ]
    if len(data) < 2:
        return []

    # Distribution -> histogram (explicitly asked, or many rows with a non-entity spread)
    if _DISTRIBUTION_Q.search(question) or len(rows) > 30:
        spec = _spec_base(f"Distribution of {pretty_m}")
        spec.update({
            "data": {"values": [{"value": d["value"]} for d in data]},
            "mark": {"type": "bar", "tooltip": True},
            "encoding": {
                "x": {"field": "value", "type": "quantitative", "bin": {"maxbins": 20}, "axis": _axis(measure)},
                "y": {"aggregate": "count", "type": "quantitative", "axis": {"title": "Count"}},
            },
        })
        return [{"title": f"Distribution of {pretty_m}", "subtitle": "", "spec": spec}]

    # Comparison -> vertical colored bars (few named entities)
    if _COMPARE_Q.search(question) and 2 <= len(data) <= 8:
        spec = _spec_base(f"{pretty_m} comparison")
        spec.update({
            "data": {"values": data},
            "mark": {"type": "bar", "tooltip": True},
            "encoding": {
                "x": {"field": "label", "type": "nominal", "sort": "-y", "axis": {"title": None}},
                "y": {"field": "value", "type": "quantitative", "axis": _axis(measure)},
                "color": {"field": "label", "type": "nominal", "legend": None},
            },
        })
        return [{"title": f"{pretty_m} comparison", "subtitle": "", "spec": spec}]

    # Default: ranking / breakdown -> horizontal bar, sorted
    spec = _spec_base(f"Top {len(data)} by {pretty_m}")
    spec.update({
        "data": {"values": data},
        "mark": {"type": "bar", "tooltip": True},
        "encoding": {
            "y": {"field": "label", "type": "nominal", "sort": "-x", "axis": {"title": None}},
            "x": {"field": "value", "type": "quantitative", "axis": _axis(measure)},
        },
        "height": {"step": 20},
    })
    return [{"title": f"{pretty_m} ranking", "subtitle": "", "spec": spec}]


def _row_has_geo(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    r = rows[0]
    return any(
        isinstance(r.get(k), str)
        for k in ("state", "county", "cd_118", "rcpt_state_name", "subawardee_state_name")
    )


def _infer_level(routing: dict[str, Any], rows: list[dict[str, Any]]) -> str | None:
    lvl = (routing.get("geography_level") or "").lower()
    if lvl in {"state", "county", "congress"}:
        return lvl
    if not rows:
        return None
    keys = rows[0].keys()
    if "cd_118" in keys or "rcpt_cd_name" in keys or "subawardee_cd_name" in keys:
        return "congress"
    if "county" in keys:
        return "county"
    if any(k in keys for k in ("state", "rcpt_state_name", "subawardee_state_name")):
        return "state"
    return None


def build_map_intent(
    question: str,
    routing: dict[str, Any],
    resolved: dict[str, Any],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """ChatbotMapIntent dict. Disabled intent when a map would not be meaningful."""
    disabled = {"enabled": False, "mapType": "none"}
    if not rows or not _row_has_geo(rows):
        return disabled
    numeric = _numeric_columns(rows)
    if not numeric:
        return disabled
    level = _infer_level(routing, rows)
    if level not in {"state", "county", "congress"}:
        return disabled

    tables = routing.get("tables") or []
    is_flow = any(t.endswith("_flow") for t in tables)
    measure = numeric[0]
    pretty_m = measure.replace("_", " ")

    # Resolved focus state (any table's resolved state value), Title-cased.
    focus_state = None
    for tbl_res in resolved.values():
        sv = tbl_res.get("state") if isinstance(tbl_res, dict) else None
        if isinstance(sv, dict) and sv.get("value"):
            focus_state = str(sv["value"]).title()
            break
    # Fallback: a state named in the question (tables like gov_congress have
    # no `state` column, so the resolver can't surface it).
    if not focus_state:
        focus_state = _state_in_question(question)

    labels = [str(r.get("state") or r.get("county") or r.get("cd_118") or r.get("label") or "")
              for r in rows][:12]
    n = len(rows)

    if is_flow:
        map_type = "flow-state-focused" if focus_state else "flow-map"
    elif _COMPARE_Q.search(question) and 2 <= n <= 8:
        map_type = "atlas-comparison"
    elif focus_state and level in {"county", "congress"}:
        map_type = "single-state-ranked-subregions"
    elif focus_state and level == "state" and n == 1:
        map_type = "single-state-spotlight"
    elif re.search(r"\btop\s+\d+\b|\bbottom\s+\d+\b", question, re.I) and n <= 25:
        map_type = "top-n-highlight"
    else:
        map_type = "atlas-single-metric"

    intent: dict[str, Any] = {
        "enabled": True,
        "mapType": map_type,
        "level": level,
        "metric": measure,
        "title": f"{pretty_m} by {level}",
        "subtitle": (f"{focus_state} — " if focus_state else "") + "geographic view",
        "buttonLabel": "View map",
        "reason": f"Result is geographic ({level}) with a numeric measure ({pretty_m}).",
        "showLegend": True,
    }
    if focus_state and map_type != "atlas-comparison":
        intent["state"] = focus_state
    if map_type == "atlas-comparison":
        intent["comparisonIds"] = [s for s in labels if s]
        intent["comparisonLabels"] = [s for s in labels if s]
    if map_type == "top-n-highlight":
        m = re.search(r"\b(top|bottom)\s+(\d+)\b", question, re.I)
        intent["topN"] = int(m.group(2)) if m else min(n, 10)
    return intent


def focus_state(question: str, resolved: dict[str, Any]) -> str | None:
    for tbl_res in resolved.values():
        sv = tbl_res.get("state") if isinstance(tbl_res, dict) else None
        if isinstance(sv, dict) and sv.get("value"):
            return str(sv["value"]).title()
    return _state_in_question(question)


def enrich_rows_for_map(
    question: str,
    routing: dict[str, Any],
    resolved: dict[str, Any],
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """The map matches county rows by county + state. County SQL usually filters
    state in WHERE and omits it from SELECT, so inject the focus state so the
    choropleth can place the counties. Harmless for charts/answers."""
    if not rows:
        return rows
    level = _infer_level(routing, rows)
    if level != "county":
        return rows
    r0 = rows[0]
    if any(k in r0 for k in ("state", "state_name", "rcpt_state_name", "subawardee_state_name")):
        return rows
    fs = focus_state(question, resolved)
    if not fs:
        return rows
    return [{**r, "state": fs} for r in rows]


def build_visuals(
    question: str,
    routing: dict[str, Any],
    resolved: dict[str, Any],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Single entry point. Never raises — returns safe empties on any failure."""
    try:
        charts = build_charts(question, routing, rows)
    except Exception:
        charts = []
    try:
        map_intent = build_map_intent(question, routing, resolved, rows)
    except Exception:
        map_intent = {"enabled": False, "mapType": "none"}
    return {
        "charts": charts,
        "chart": charts[0]["spec"] if charts else None,
        "map_intent": map_intent,
    }
