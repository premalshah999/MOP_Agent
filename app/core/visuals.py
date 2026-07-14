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
    r"spend|income|bond|opeb|pension|cash|debt(?!_ratio)|subaward|"
    r"receiv|award|inflow|outflow|disburs|dollar",
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


def _pick_measure(question: str, numeric: list[str]) -> str:
    """Choose the numeric column to visualize. When the SQL returns several
    numeric columns, prefer the one whose name shares words with the
    question — plotting `numeric[0]` blindly charted the wrong measure for
    multi-metric results (e.g. a poverty question showing Debt_Ratio because
    the join put it first)."""
    if len(numeric) <= 1:
        return numeric[0]
    q_words = set(re.findall(r"[a-z]+", question.lower()))
    def overlap(col: str) -> int:
        col_words = set(re.findall(r"[a-z]+", col.lower().replace("_", " ")))
        return len(col_words & q_words)
    best = max(numeric, key=overlap)
    return best if overlap(best) > 0 else numeric[0]


# Data-ink palette (validated: terracotta vs blue ΔE 83.5, both ≥3:1 on the
# warm surface #fdfcfa). Terracotta is the app accent AND the data hue so the
# whole product reads as one system; blue is the opposing diverging pole.
_PRIMARY = "#c6613f"       # terracotta, primary marks
_PRIMARY_SOFT = "#e8c4b4"  # light terracotta, lollipop stick / secondary
_ACCENT_NEG = "#2a78d6"    # diverging opposite pole (negatives)
_HOVER_DIM = 0.35          # non-hovered mark opacity when a mark is hovered


def _pretty_label(s: Any) -> str:
    """Title-case ALL-UPPERCASE labels for visual consistency with prose tables.
    Leaves already-cased strings alone (e.g. "Department of Defense")."""
    text = str(s)
    if text and text.isupper() and any(c.isalpha() for c in text):
        # "PRINCE GEORGE'S" -> "Prince George's", "BALTIMORE CITY" -> "Baltimore City"
        return " ".join(w[:1].upper() + w[1:].lower() for w in text.split())
    return text


def _is_money(measure: str) -> bool:
    return bool(_MONEY_HINT.search(measure))


def _fmt(measure: str) -> str | None:
    return "~s" if _is_money(measure) else None


# d3's SI format uses G for 10^9; dollars read as B. The axis labelExpr
# rewrites the formatted tick ("8G" -> "$8B", "500M" -> "$500M").
_MONEY_LABEL_EXPR = "'$' + replace(datum.label, 'G', 'B')"


def _qaxis(measure: str, *, grid: bool = True, title: str | None = None) -> dict[str, Any]:
    # title=None keeps prior behaviour (no axis label); pass an explicit string
    # for charts where the measure isn't already obvious from the chart title
    # (trend chart's y, histogram's x).
    ax: dict[str, Any] = {"title": title, "grid": grid, "tickCount": 5}
    if _is_money(measure):
        ax["format"] = "~s"
        ax["labelExpr"] = _MONEY_LABEL_EXPR
    return ax


def _tooltip(label_title: str, measure: str) -> list[dict[str, Any]]:
    # Money tooltips show the exact dollar figure ("$7,740,185,920") —
    # hover is where precision belongs; the axis stays compact.
    fmt = "$,.0f" if _is_money(measure) else ",.2f"
    return [
        {"field": "label", "type": "nominal", "title": label_title},
        {"field": "value", "type": "quantitative", "title": measure.replace("_", " "),
         "format": fmt},
    ]


def _spec_base(*, stepped: bool = False) -> dict[str, Any]:
    """`stepped=True` for charts with discrete (step) height — Vega's
    `autosize:fit` conflicts with discrete height, so we constrain to fit-x."""
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "width": "container",
        "autosize": {"type": "fit-x" if stepped else "fit", "contains": "padding"},
    }


def build_charts(
    question: str,
    routing: dict[str, Any],
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return a list of ChartBlock dicts ({title, subtitle, spec}); [] if none fit.

    Chart type is chosen for the question shape; styling is intentionally
    minimal and inherits the shared theme in VegaChart.tsx.
    """
    if not rows or len(rows) < 2:
        return []
    numeric = _numeric_columns(rows)
    if not numeric:
        return []
    measure = _pick_measure(question, numeric)
    year_col = _year_column(rows)
    pretty_m = measure.replace("_", " ")

    # Trend -> soft area + line
    # Heatmap: agency × state breakdown with a numeric measure
    if "agency" in rows[0] and "state" in rows[0]:
        agencies = {str(r.get("agency")) for r in rows if r.get("agency") is not None}
        states = {str(r.get("state")) for r in rows if r.get("state") is not None}
        if len(agencies) >= 3 and len(states) >= 3 and 9 <= len(rows) <= 200:
            data = [
                {
                    "agency": _pretty_label(r.get("agency")),
                    "state": _pretty_label(r.get("state")),
                    "value": r.get(measure),
                }
                for r in rows if _is_number(r.get(measure))
            ]
            spec = _spec_base()
            spec.update({
                "data": {"values": data},
                "height": min(360, 22 * len(agencies) + 40),
                "mark": {"type": "rect", "tooltip": True, "cornerRadius": 2, "stroke": "#ffffff", "strokeWidth": 1},
                "encoding": {
                    "y": {"field": "agency", "type": "nominal", "axis": {"title": None, "labelLimit": 200}},
                    "x": {"field": "state", "type": "nominal", "axis": {"title": None, "labelAngle": -45, "labelLimit": 60}},
                    "color": {
                        "field": "value", "type": "quantitative",
                        "scale": {"scheme": "blues"},
                        "legend": {"title": pretty_m, "format": _fmt(measure),
                                   **({"labelExpr": _MONEY_LABEL_EXPR} if _is_money(measure) else {})},
                    },
                    "tooltip": [
                        {"field": "agency", "type": "nominal", "title": "agency"},
                        {"field": "state", "type": "nominal", "title": "state"},
                        {"field": "value", "type": "quantitative", "title": pretty_m,
                         "format": "$,.0f" if _is_money(measure) else ",.2f"},
                    ],
                },
            })
            return [{"title": f"{pretty_m} by agency and state", "subtitle": "", "spec": spec}]

    if year_col and len({r.get(year_col) for r in rows}) >= 2:
        data = sorted(
            ({"year": str(r.get(year_col)), "value": r.get(measure)} for r in rows if _is_number(r.get(measure))),
            key=lambda d: d["year"],
        )
        if len(data) >= 2:
            spec = _spec_base()
            spec.update({
                "data": {"values": data},
                "height": 240,
                "encoding": {
                    "x": {"field": "year", "type": "ordinal", "axis": {"title": None, "labelAngle": 0, "grid": True}},
                    "y": {"field": "value", "type": "quantitative", "axis": _qaxis(measure, title=pretty_m)},
                    "tooltip": [
                        {"field": "year", "type": "ordinal", "title": "year"},
                        {"field": "value", "type": "quantitative", "title": pretty_m,
                         "format": "$,.0f" if _is_money(measure) else ",.2f"},
                    ],
                },
                "layer": [
                    {"mark": {"type": "area", "line": False, "opacity": 0.10, "color": _PRIMARY}},
                    {"mark": {"type": "line", "strokeWidth": 2, "color": _PRIMARY,
                              "point": {"filled": True, "size": 42, "color": _PRIMARY}}},
                ],
            })
            return [{"title": f"{pretty_m} over time", "subtitle": "", "spec": spec}]

    label_col = _label_column(rows, numeric)
    if not label_col:
        return []
    label_title = label_col.replace("_", " ")
    plotted = rows[:20]
    data = [
        {"label": _pretty_label(r.get(label_col)), "value": r.get(measure)}
        for r in plotted if _is_number(r.get(measure))
    ]
    if len(data) < 2:
        return []

    # Distribution -> histogram
    if _DISTRIBUTION_Q.search(question) or len(rows) > 30:
        spec = _spec_base()
        spec.update({
            "data": {"values": [{"value": d["value"]} for d in data]},
            "height": 240,
            "mark": {"type": "bar", "tooltip": True, "color": _PRIMARY, "cornerRadius": 2},
            "encoding": {
                "x": {"field": "value", "type": "quantitative", "bin": {"maxbins": 18}, "axis": _qaxis(measure, grid=False, title=pretty_m)},
                "y": {"aggregate": "count", "type": "quantitative", "axis": {"title": "count", "grid": True}},
            },
        })
        return [{"title": f"Distribution of {pretty_m}", "subtitle": "", "spec": spec}]

    sort_desc = {"field": "value", "order": "descending"}

    # Diverging bars: values straddle zero (e.g. Free_Cash_Flow with negatives).
    values_only = [d["value"] for d in data if isinstance(d["value"], (int, float))]
    if values_only and len(data) >= 3 and min(values_only) < 0 < max(values_only):
        spec = _spec_base(stepped=True)
        spec.update({
            "data": {"values": data},
            "height": {"step": 28},
            "mark": {"type": "bar", "cornerRadiusEnd": 3, "tooltip": True},
            "encoding": {
                "y": {"field": "label", "type": "nominal", "sort": sort_desc, "axis": {"title": None, "labelLimit": 180}},
                "x": {"field": "value", "type": "quantitative", "axis": _qaxis(measure)},
                "color": {
                    "condition": {"test": "datum.value < 0", "value": _ACCENT_NEG},
                    "value": _PRIMARY,
                },
                "tooltip": _tooltip(label_title, measure),
            },
        })
        return [{"title": f"{pretty_m} by {label_title}", "subtitle": "negative values shown in red", "spec": spec}]

    # Comparison: dot plot for 2–3 entities (cleaner than bars at small N),
    # horizontal bars for 4–8.
    if _COMPARE_Q.search(question) and 2 <= len(data) <= 3:
        spec = _spec_base(stepped=True)
        spec.update({
            "data": {"values": data},
            "height": {"step": 44},
            "mark": {"type": "circle", "size": 220, "color": _PRIMARY, "opacity": 0.95, "tooltip": True},
            "encoding": {
                "y": {"field": "label", "type": "nominal", "sort": sort_desc, "axis": {"title": None, "labelLimit": 200}},
                "x": {"field": "value", "type": "quantitative", "axis": _qaxis(measure), "scale": {"nice": True, "padding": 18}},
                "tooltip": _tooltip(label_title, measure),
            },
        })
        return [{"title": f"{pretty_m} compared", "subtitle": "", "spec": spec}]

    # Comparison -> clean horizontal bars (few named entities)
    if _COMPARE_Q.search(question) and 2 <= len(data) <= 8:
        spec = _spec_base(stepped=True)
        spec.update({
            "data": {"values": data},
            "height": {"step": 38},
            "mark": {"type": "bar", "tooltip": True, "color": _PRIMARY, "cornerRadiusEnd": 3},
            "encoding": {
                "y": {"field": "label", "type": "nominal", "sort": sort_desc, "axis": {"title": None, "labelLimit": 170}},
                "x": {"field": "value", "type": "quantitative", "axis": _qaxis(measure)},
                "tooltip": _tooltip(label_title, measure),
            },
        })
        return [{"title": f"{pretty_m} compared", "subtitle": "", "spec": spec}]

    # Ranking / breakdown -> rounded bars with direct value labels and a hover
    # highlight (hovered bar stays saturated, the rest dim). Feels interactive
    # the moment the cursor touches it.
    label_expr = (
        "'$' + replace(format(datum.value, '.3~s'), 'G', 'B')"
        if _is_money(measure) else "format(datum.value, ',.2~f')"
    )
    # Vega-Lite drops channel/field sorts on layered specs — sort the data
    # here and tell the axis to respect input order (sort: null).
    data = sorted(data, key=lambda d: (d["value"] is None, -(d["value"] or 0)))
    spec = _spec_base(stepped=True)
    spec.update({
        "data": {"values": data},
        "height": {"step": 32},
        "encoding": {
            "y": {"field": "label", "type": "nominal", "sort": None, "axis": {"title": None, "labelLimit": 180}},
            "x": {"field": "value", "type": "quantitative", "axis": _qaxis(measure), "scale": {"nice": True}},
            "tooltip": _tooltip(label_title, measure),
        },
        "layer": [
            {
                # Selection params must sit inside a unit spec, not the layer root.
                "params": [{
                    "name": "hover",
                    "select": {"type": "point", "on": "pointerover", "clear": "pointerout"},
                }],
                "mark": {"type": "bar", "cornerRadiusEnd": 5, "height": {"band": 0.62}, "color": _PRIMARY, "tooltip": True},
                "encoding": {
                    "opacity": {
                        "condition": {"param": "hover", "empty": True, "value": 1},
                        "value": _HOVER_DIM,
                    },
                },
            },
            {
                "mark": {"type": "text", "align": "left", "dx": 6, "fontSize": 11,
                         "color": "#6e6d64", "fontWeight": 500},
                "encoding": {"text": {"value": ""}, "x": {"field": "value", "type": "quantitative"}},
                "transform": [{"calculate": label_expr, "as": "vlabel"}],
            },
        ],
    })
    # value labels: bind text to the calculated field
    spec["layer"][1]["encoding"]["text"] = {"field": "vlabel", "type": "nominal"}
    return [{"title": f"{pretty_m} by {label_title}", "subtitle": "", "spec": spec}]


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
    measure = _pick_measure(question, numeric)
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
