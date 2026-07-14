"""Evidence-driven chart and map recommendations.

Visuals are derived only from executed query rows and catalog metadata.  The
module never asks an LLM to invent a chart, silently aggregate duplicate
geographies, or choose a unit.  If the row shape is ambiguous, it disables the
visual instead of presenting a plausible-looking but incorrect picture.
"""

from __future__ import annotations

import re
from typing import Any

from app.semantic.registry import get_dataset


_NON_MEASURE = {
    "state", "state_name", "county", "county_name", "cd_118", "fips",
    "state_fips", "county_fips", "year", "Year", "act_dt_fis_yr",
    "agency", "agency_name", "rcpt_state_name", "subawardee_state_name",
    "rcpt_cd_name", "subawardee_cd_name", "rcpt_cty_name",
    "subawardee_cty_name", "rcpt_state", "subawardee_state", "label",
    "rank", "source", "destination", "origin", "Unnamed: 0",
}
_GEO_LABEL_PRIORITY = (
    "county", "cd_118", "rcpt_cd_name", "subawardee_cd_name",
    "rcpt_cty_name", "subawardee_cty_name", "rcpt_state_name",
    "subawardee_state_name", "state", "agency", "label",
)
_YEAR_KEYS = ("Year", "year", "act_dt_fis_yr")
_MONEY_HINT = re.compile(
    r"contract|grant|payment|wage|fund|amount|asset|liabilit|revenue|expense|"
    r"spend|income|bond|opeb|pension|cash|debt(?!_ratio)|subaward|"
    r"receiv|award|inflow|outflow|disburs|dollar",
    re.I,
)
_DISTRIBUTION_Q = re.compile(
    r"distribut|histogram|spread|how .*vary|variation|range of", re.I
)
_COMPARE_Q = re.compile(r"\bcompare\b|\bvs\b|\bversus\b|\bbetween\b", re.I)
_RELATIONSHIP_Q = re.compile(
    r"correlat|relationship|association|\bversus\b|\bvs\.?\b", re.I
)
_BOTTOM_Q = re.compile(
    r"\b(bottom|lowest|smallest|least|fewest|ascending)\b", re.I
)

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
    "puerto rico", "guam", "american samoa", "virgin islands",
    "u.s. virgin islands", "united states virgin islands",
    "northern mariana islands", "commonwealth of northern mariana islands",
    "commonwealth of the northern mariana islands",
)

_PRIMARY = "#c6613f"
_ACCENT_NEG = "#2a78d6"
_SERIES_COLORS = ["#c6613f", "#2a78d6", "#558b6e", "#8b6bb1"]
_HOVER_DIM = 0.35
_MAX_RANKING_MARKS = 20


def _state_in_question(question: str) -> str | None:
    q = question.lower()
    for name in sorted(_US_STATES, key=len, reverse=True):
        if re.search(rf"\b{re.escape(name)}\b", q):
            return name.title()
    return None


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _numeric_columns(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return []
    columns: list[str] = []
    for key in rows[0]:
        if key in _NON_MEASURE:
            continue
        values = [row.get(key) for row in rows if row.get(key) is not None]
        if values and sum(_is_number(value) for value in values) >= max(1, len(values) // 2):
            columns.append(key)
    return columns


def _label_column(rows: list[dict[str, Any]], numeric: list[str]) -> str | None:
    keys = list(rows[0])
    for candidate in _GEO_LABEL_PRIORITY:
        if candidate in keys:
            return candidate
    for key in keys:
        if key not in numeric and key not in _YEAR_KEYS:
            return key
    return None


def _year_column(rows: list[dict[str, Any]]) -> str | None:
    for key in _YEAR_KEYS:
        if key in rows[0]:
            distinct = {row.get(key) for row in rows if row.get(key) is not None}
            if len(distinct) >= 2:
                return key
    return None


def _tokens(value: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", value.lower().replace("_", " ")))


def _ordered_measures(
    question: str,
    numeric: list[str],
    routing: dict[str, Any],
) -> list[str]:
    """Order returned numeric fields by the catalog-routed metrics, then text."""
    remaining = list(numeric)
    ordered: list[str] = []
    for expected in routing.get("columns") or []:
        expected_tokens = _tokens(str(expected))
        matches = sorted(
            remaining,
            key=lambda column: len(expected_tokens & _tokens(column)),
            reverse=True,
        )
        if matches and len(expected_tokens & _tokens(matches[0])) > 0:
            ordered.append(matches[0])
            remaining.remove(matches[0])
    q_tokens = _tokens(question)
    remaining.sort(
        key=lambda column: len(q_tokens & _tokens(column)),
        reverse=True,
    )
    return [*ordered, *remaining]


def _pretty_label(value: Any) -> str:
    text = str(value)
    if text and text.isupper() and any(character.isalpha() for character in text):
        return " ".join(word[:1].upper() + word[1:].lower() for word in text.split())
    return text


def _metric_profile(
    measure: str,
    routing: dict[str, Any],
    values: list[float] | None = None,
) -> dict[str, str]:
    """Resolve a display label and unit from the semantic registry.

    SQL aliases are common, so exact catalog matches are preferred and a
    conservative token match is used only when there is a single clear routed
    metric.  Name/value heuristics cover derived count aliases.
    """
    normalized = measure.casefold().replace("_", " ")
    best: tuple[int, Any] | None = None
    routed = [str(column) for column in (routing.get("columns") or [])]
    for table in routing.get("tables") or []:
        dataset = get_dataset(str(table))
        if dataset is None:
            continue
        for column, metric in dataset.metrics.items():
            candidates = {column, metric.label, *metric.synonyms}
            if any(normalized == candidate.casefold().replace("_", " ") for candidate in candidates):
                best = (100, metric)
                break
            overlap = len(_tokens(measure) & _tokens(column))
            if column in routed:
                overlap += 3
            if overlap and (best is None or overlap > best[0]):
                best = (overlap, metric)
        if best and best[0] == 100:
            break

    label = best[1].label if best else measure.replace("_", " ")
    unit = str(best[1].unit) if best else "value"
    lowered = normalized
    numeric_values = values or []
    catalog_metric_id = str(best[1].id).lower() if best else ""
    if "subaward" in lowered or "subaward" in catalog_metric_id:
        # state_flow's physical field is named subaward_amount_year even
        # though the table has no year dimension. Never expose that storage
        # artifact as a metric label.
        label = "Subaward amount"
        unit = "USD"
    elif _MONEY_HINT.search(measure):
        unit = "USD"
    elif re.search(r"population|people|persons|\bcount\b|number", lowered):
        unit = "persons"
    elif re.search(r"percent|percentage|share|\brate\b", lowered):
        unit = "percent"
    elif unit == "percent" and numeric_values and max(abs(value) for value in numeric_values) > 100:
        # A derived alias such as asian_population uses a percentage column in
        # its formula but the result is a count of people.
        unit = "persons"
    return {"label": label.replace("_", " "), "unit": unit}


def _same_unit_family(profiles: list[dict[str, str]]) -> bool:
    families = {
        "money" if profile["unit"].lower() == "usd" else profile["unit"].lower()
        for profile in profiles
    }
    return len(families) == 1


def _money(profile: dict[str, str]) -> bool:
    return profile["unit"].lower() == "usd"


def _quant_axis(
    profile: dict[str, str],
    *,
    grid: bool = True,
    title: str | None = None,
) -> dict[str, Any]:
    axis: dict[str, Any] = {"title": title, "grid": grid, "tickCount": 5}
    unit = profile["unit"].lower()
    if unit == "usd":
        axis.update({"format": "~s", "labelExpr": "'$' + replace(datum.label, 'G', 'B')"})
    elif unit == "percent":
        axis.update({"format": ",.1f", "labelExpr": "datum.label + '%'"})
    elif unit in {"persons", "people", "count", "households"}:
        axis["format"] = "~s"
    return axis


def _quant_legend(profile: dict[str, str]) -> dict[str, Any]:
    legend: dict[str, Any] = {"title": profile["label"]}
    unit = profile["unit"].lower()
    if unit == "usd":
        legend.update({"format": "~s", "labelExpr": "'$' + replace(datum.label, 'G', 'B')"})
    elif unit == "percent":
        legend.update({"format": ",.1f", "labelExpr": "datum.label + '%'"})
    elif unit in {"persons", "people", "count", "households"}:
        legend["format"] = "~s"
    return legend


def _value_tooltip(field: str, profile: dict[str, str]) -> dict[str, Any]:
    unit = profile["unit"].lower()
    title = profile["label"] + (" (%)" if unit == "percent" else "")
    format_string = "$,.0f" if unit == "usd" else ",.1f" if unit == "percent" else ",.2f"
    return {"field": field, "type": "quantitative", "title": title, "format": format_string}


def _tooltip(label_title: str, profile: dict[str, str]) -> list[dict[str, Any]]:
    return [
        {"field": "label", "type": "nominal", "title": label_title},
        _value_tooltip("value", profile),
    ]


def _value_label_expr(profile: dict[str, str]) -> str:
    unit = profile["unit"].lower()
    if unit == "usd":
        return "'$' + replace(format(datum.value, '.3~s'), 'G', 'B')"
    if unit == "percent":
        return "format(datum.value, ',.1f') + '%'"
    if unit in {"persons", "people", "count", "households"}:
        return "replace(format(datum.value, '.3~s'), 'G', 'B')"
    return "format(datum.value, ',.2~f')"


def _spec_base() -> dict[str, Any]:
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
        "width": "container",
        # Every chart has an explicit/faceted height; fitting only width avoids
        # Vega-Lite dropping fit-y for discrete axes and keeps resize behavior
        # predictable inside the chat column.
        "autosize": {"type": "fit-x", "contains": "padding"},
    }


def build_charts(
    question: str,
    routing: dict[str, Any],
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if len(rows) < 2:
        return []
    numeric = _numeric_columns(rows)
    if not numeric:
        return []
    measures = _ordered_measures(question, numeric, routing)
    measure = measures[0]
    label_col = _label_column(rows, numeric)
    year_col = _year_column(rows)

    # Agency × state matrix. A map is intentionally disabled for this shape
    # later because each state appears once per agency and cannot be colored by
    # a single unambiguous value.
    if "agency" in rows[0] and "state" in rows[0]:
        agencies = {str(row.get("agency")) for row in rows if row.get("agency") is not None}
        states = {str(row.get("state")) for row in rows if row.get("state") is not None}
        if len(agencies) >= 3 and len(states) >= 3 and 9 <= len(rows) <= 200:
            values = [float(row[measure]) for row in rows if _is_number(row.get(measure))]
            profile = _metric_profile(measure, routing, values)
            data = [
                {
                    "agency": _pretty_label(row.get("agency")),
                    "state": _pretty_label(row.get("state")),
                    "value": row.get(measure),
                }
                for row in rows
                if _is_number(row.get(measure))
            ]
            spec = _spec_base()
            spec.update({
                "data": {"values": data},
                "height": min(420, 23 * len(agencies) + 50),
                "mark": {"type": "rect", "tooltip": True, "cornerRadius": 2, "stroke": "#ffffff", "strokeWidth": 1},
                "encoding": {
                    "y": {"field": "agency", "type": "nominal", "axis": {"title": None, "labelLimit": 220}},
                    "x": {"field": "state", "type": "nominal", "axis": {"title": None, "labelAngle": -45, "labelLimit": 80}},
                    "color": {
                        "field": "value", "type": "quantitative",
                        "scale": {"scheme": "orangered"},
                        "legend": _quant_legend(profile),
                    },
                    "tooltip": [
                        {"field": "agency", "type": "nominal", "title": "Agency"},
                        {"field": "state", "type": "nominal", "title": "State"},
                        _value_tooltip("value", profile),
                    ],
                },
            })
            return [{"title": f"{profile['label']} by agency and state", "subtitle": f"{len(agencies)} agencies across {len(states)} states", "spec": spec}]

    # One or several time series. The previous implementation merged multiple
    # states into one zig-zagging line because it discarded the series label.
    if year_col:
        values = [float(row[measure]) for row in rows if _is_number(row.get(measure))]
        profile = _metric_profile(measure, routing, values)
        series_values = {
            str(row.get(label_col)) for row in rows
            if label_col and row.get(label_col) is not None
        }
        use_series = bool(label_col and 2 <= len(series_values) <= 8)
        data = sorted(
            (
                {
                    "year": str(row.get(year_col)),
                    "value": row.get(measure),
                    **({"series": _pretty_label(row.get(label_col))} if use_series and label_col else {}),
                }
                for row in rows
                if _is_number(row.get(measure))
            ),
            key=lambda item: (str(item.get("series", "")), str(item["year"])),
        )
        if len(data) >= 2:
            encoding: dict[str, Any] = {
                "x": {"field": "year", "type": "ordinal", "axis": {"title": None, "labelAngle": 0, "grid": True}},
                "y": {"field": "value", "type": "quantitative", "axis": _quant_axis(profile, title=profile["label"])},
                "tooltip": [
                    *([{"field": "series", "type": "nominal", "title": label_col.replace("_", " ").title()}] if use_series and label_col else []),
                    {"field": "year", "type": "ordinal", "title": "Year"},
                    _value_tooltip("value", profile),
                ],
            }
            if use_series:
                encoding["color"] = {
                    "field": "series", "type": "nominal", "title": None,
                    "scale": {"range": _SERIES_COLORS},
                }
            layers: list[dict[str, Any]] = []
            if not use_series:
                layers.append({"mark": {"type": "area", "opacity": 0.10, "color": _PRIMARY}})
            layers.extend([
                {"mark": {"type": "line", "strokeWidth": 2.4, **({} if use_series else {"color": _PRIMARY})}},
                {"mark": {"type": "point", "filled": True, "size": 54, **({} if use_series else {"color": _PRIMARY})}},
            ])
            spec = _spec_base()
            spec.update({
                "data": {"values": data},
                "height": 260,
                "encoding": encoding,
                "layer": layers,
            })
            subtitle = f"{len(series_values)} series" if use_series else f"{len(data)} periods"
            return [{"title": f"{profile['label']} over time", "subtitle": subtitle, "spec": spec}]

    if not label_col:
        return []
    label_title = label_col.replace("_", " ").title()

    # Two-measure geographic results are best read as a scatterplot. This is
    # especially important for cross-table questions (poverty vs debt, literacy
    # vs leverage), where choosing only the first numeric column hides half the
    # answer and grouped bars would mix incompatible units.
    if len(measures) >= 2 and len(rows) >= 3 and (
        _RELATIONSHIP_Q.search(question) or len(routing.get("tables") or []) > 1
    ):
        x_measure, y_measure = measures[:2]
        x_values = [float(row[x_measure]) for row in rows if _is_number(row.get(x_measure))]
        y_values = [float(row[y_measure]) for row in rows if _is_number(row.get(y_measure))]
        x_profile = _metric_profile(x_measure, routing, x_values)
        y_profile = _metric_profile(y_measure, routing, y_values)
        data = [
            {
                "label": _pretty_label(row.get(label_col)),
                "x": row.get(x_measure),
                "y": row.get(y_measure),
            }
            for row in rows
            if _is_number(row.get(x_measure)) and _is_number(row.get(y_measure))
        ]
        if len(data) >= 3:
            spec = _spec_base()
            spec.update({
                "data": {"values": data},
                "height": 300,
                "mark": {"type": "circle", "size": 105, "color": _PRIMARY, "opacity": 0.82, "tooltip": True, "stroke": "#ffffff", "strokeWidth": 1},
                "encoding": {
                    "x": {"field": "x", "type": "quantitative", "axis": _quant_axis(x_profile, title=x_profile["label"]), "scale": {"nice": True, "zero": False}},
                    "y": {"field": "y", "type": "quantitative", "axis": _quant_axis(y_profile, title=y_profile["label"]), "scale": {"nice": True, "zero": False}},
                    "tooltip": [
                        {"field": "label", "type": "nominal", "title": label_title},
                        _value_tooltip("x", x_profile),
                        _value_tooltip("y", y_profile),
                    ],
                },
            })
            return [{"title": f"{y_profile['label']} vs {x_profile['label']}", "subtitle": f"Each point is one {label_title.lower()}", "spec": spec}]

    # A comparison with several same-unit measures gets grouped bars so every
    # explicitly requested metric is visible instead of silently selecting one.
    if _COMPARE_Q.search(question) and 2 <= len(measures) <= 4 and 2 <= len(rows) <= 12:
        selected = measures[:4]
        profiles = [
            _metric_profile(
                selected_measure,
                routing,
                [float(row[selected_measure]) for row in rows if _is_number(row.get(selected_measure))],
            )
            for selected_measure in selected
        ]
        if _same_unit_family(profiles):
            long_data = [
                {"label": _pretty_label(row.get(label_col)), "metric": profile["label"], "value": row.get(selected_measure)}
                for row in rows
                for selected_measure, profile in zip(selected, profiles)
                if _is_number(row.get(selected_measure))
            ]
            spec = _spec_base()
            spec.update({
                "data": {"values": long_data},
                "height": {"step": 30 * len(selected)},
                "mark": {"type": "bar", "cornerRadiusEnd": 3, "tooltip": True},
                "encoding": {
                    "y": {"field": "label", "type": "nominal", "axis": {"title": None, "labelLimit": 190}},
                    "yOffset": {"field": "metric"},
                    "x": {"field": "value", "type": "quantitative", "axis": _quant_axis(profiles[0])},
                    "color": {"field": "metric", "type": "nominal", "title": None, "scale": {"range": _SERIES_COLORS}},
                    "tooltip": [
                        {"field": "label", "type": "nominal", "title": label_title},
                        {"field": "metric", "type": "nominal", "title": "Metric"},
                        _value_tooltip("value", profiles[0]),
                    ],
                },
            })
            return [{"title": f"{label_title} comparison", "subtitle": f"{len(selected)} requested measures", "spec": spec}]

    all_data: list[dict[str, Any]] = [
        {"label": _pretty_label(row.get(label_col)), "value": row.get(measure)}
        for row in rows
        if _is_number(row.get(measure))
    ]
    if len(all_data) < 2:
        return []
    values = [float(item["value"]) for item in all_data]
    profile = _metric_profile(measure, routing, values)

    # A histogram is only appropriate when the user asks about a distribution.
    # Previously every result over 30 rows became a histogram and, worse, only
    # the first 20 rows were included in it.
    if _DISTRIBUTION_Q.search(question):
        spec = _spec_base()
        spec.update({
            "data": {"values": [{"value": item["value"]} for item in all_data]},
            "height": 260,
            "mark": {"type": "bar", "tooltip": True, "color": _PRIMARY, "cornerRadius": 2},
            "encoding": {
                "x": {"field": "value", "type": "quantitative", "bin": {"maxbins": 18}, "axis": _quant_axis(profile, grid=False, title=profile["label"])},
                "y": {"aggregate": "count", "type": "quantitative", "axis": {"title": "Observations", "grid": True}},
                "tooltip": [
                    {"field": "value", "bin": True, "type": "quantitative", "title": profile["label"]},
                    {"aggregate": "count", "type": "quantitative", "title": "Observations"},
                ],
            },
        })
        return [{"title": f"Distribution of {profile['label']}", "subtitle": f"All {len(all_data)} returned observations", "spec": spec}]

    ascending = str(routing.get("sort_direction") or "").lower() == "asc" or bool(_BOTTOM_Q.search(question))
    all_data.sort(key=lambda item: float(item["value"]), reverse=not ascending)
    data = all_data[:_MAX_RANKING_MARKS]
    order = "ascending" if ascending else "descending"
    subtitle = ""
    if len(all_data) > len(data):
        subtitle = f"Showing {len(data)} of {len(all_data)} returned rows"

    # Diverging values need a zero-centered visual distinction.
    if len(data) >= 3 and min(float(item["value"]) for item in data) < 0 < max(float(item["value"]) for item in data):
        spec = _spec_base()
        spec.update({
            "data": {"values": data},
            "height": {"step": 30},
            "mark": {"type": "bar", "cornerRadiusEnd": 3, "tooltip": True},
            "encoding": {
                "y": {"field": "label", "type": "nominal", "sort": {"field": "value", "order": order}, "axis": {"title": None, "labelLimit": 190}},
                "x": {"field": "value", "type": "quantitative", "axis": _quant_axis(profile)},
                "color": {"condition": {"test": "datum.value < 0", "value": _ACCENT_NEG}, "value": _PRIMARY},
                "tooltip": _tooltip(label_title, profile),
            },
        })
        note = "Negative values are blue; positive values are terracotta."
        return [{"title": f"{profile['label']} by {label_title}", "subtitle": " · ".join(part for part in (subtitle, note) if part), "spec": spec}]

    if _COMPARE_Q.search(question) and 2 <= len(data) <= 3:
        spec = _spec_base()
        spec.update({
            "data": {"values": data},
            "height": {"step": 46},
            "mark": {"type": "circle", "size": 230, "color": _PRIMARY, "opacity": 0.95, "tooltip": True},
            "encoding": {
                "y": {"field": "label", "type": "nominal", "sort": {"field": "value", "order": order}, "axis": {"title": None, "labelLimit": 210}},
                "x": {"field": "value", "type": "quantitative", "axis": _quant_axis(profile), "scale": {"nice": True, "padding": 18}},
                "tooltip": _tooltip(label_title, profile),
            },
        })
        return [{"title": f"{profile['label']} compared", "subtitle": subtitle, "spec": spec}]

    if _COMPARE_Q.search(question) and 2 <= len(data) <= 8:
        spec = _spec_base()
        spec.update({
            "data": {"values": data},
            "height": {"step": 38},
            "mark": {"type": "bar", "tooltip": True, "color": _PRIMARY, "cornerRadiusEnd": 3},
            "encoding": {
                "y": {"field": "label", "type": "nominal", "sort": {"field": "value", "order": order}, "axis": {"title": None, "labelLimit": 180}},
                "x": {"field": "value", "type": "quantitative", "axis": _quant_axis(profile)},
                "tooltip": _tooltip(label_title, profile),
            },
        })
        return [{"title": f"{profile['label']} compared", "subtitle": subtitle, "spec": spec}]

    # Layered ranking bars with direct labels and a hover highlight.
    spec = _spec_base()
    spec.update({
        "data": {"values": data},
        "height": {"step": 32},
        "encoding": {
            "y": {"field": "label", "type": "nominal", "sort": None, "axis": {"title": None, "labelLimit": 190}},
            "x": {"field": "value", "type": "quantitative", "axis": _quant_axis(profile), "scale": {"nice": True}},
            "tooltip": _tooltip(label_title, profile),
        },
        "layer": [
            {
                "params": [{"name": "hover", "select": {"type": "point", "on": "pointerover", "clear": "pointerout"}}],
                "mark": {"type": "bar", "cornerRadiusEnd": 5, "height": {"band": 0.62}, "color": _PRIMARY, "tooltip": True},
                "encoding": {"opacity": {"condition": {"param": "hover", "empty": True, "value": 1}, "value": _HOVER_DIM}},
            },
            {
                "mark": {"type": "text", "align": "left", "dx": 6, "fontSize": 11, "color": "#6e6d64", "fontWeight": 500},
                "encoding": {"text": {"field": "vlabel", "type": "nominal"}, "x": {"field": "value", "type": "quantitative"}},
                "transform": [{"calculate": _value_label_expr(profile), "as": "vlabel"}],
            },
        ],
    })
    return [{"title": f"{profile['label']} by {label_title}", "subtitle": subtitle, "spec": spec}]


def _infer_level(routing: dict[str, Any], rows: list[dict[str, Any]]) -> str | None:
    level = str(routing.get("geography_level") or "").lower()
    if level in {"state", "county", "congress"}:
        return level
    if not rows:
        return None
    keys = rows[0].keys()
    if any(key in keys for key in ("cd_118", "rcpt_cd_name", "subawardee_cd_name")):
        return "congress"
    if any(key in keys for key in ("county", "county_name", "rcpt_cty_name", "subawardee_cty_name")):
        return "county"
    if any(key in keys for key in ("state", "state_name", "rcpt_state_name", "subawardee_state_name", "rcpt_state", "subawardee_state")):
        return "state"
    return None


def _first_text(row: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _fallback_geo_text(row: dict[str, Any], level: str, side: str) -> str | None:
    """Resolve a model-chosen geography alias without guessing ambiguously."""
    geography_hint = {
        "state": re.compile(r"state", re.I),
        "county": re.compile(r"county|cty", re.I),
        "congress": re.compile(r"district|(^|_)cd($|_)", re.I),
    }.get(level)
    if geography_hint is None:
        return None
    side_hint = (
        re.compile(r"send|source|origin|prime|rcpt", re.I)
        if side == "source"
        else re.compile(r"receiv|dest|subawardee", re.I)
        if side == "destination"
        else None
    )
    candidates = [
        (key, value.strip())
        for key, value in row.items()
        if isinstance(value, str)
        and value.strip()
        and geography_hint.search(key)
        and not re.search(r"fips|code|_id$", key, re.I)
    ]
    sided = [value for key, value in candidates if side_hint and side_hint.search(key)]
    if len(sided) == 1:
        return sided[0]
    return candidates[0][1] if len(candidates) == 1 else None


def _geo_identity(row: dict[str, Any], level: str, side: str) -> str | None:
    source: tuple[str, ...]
    destination: tuple[str, ...]
    direct: tuple[str, ...]
    if level == "state":
        source = ("source", "origin", "source_state", "rcpt_state_name", "rcpt_state")
        destination = ("destination", "destination_state", "subawardee_state_name", "subawardee_state")
        direct = ("state", "state_name", "label")
    elif level == "county":
        source = ("source_county", "origin_county", "rcpt_cty_name")
        destination = ("destination_county", "subawardee_cty_name")
        direct = ("county", "county_name", "label")
    else:
        source = ("source_district", "origin_district", "rcpt_cd_name")
        destination = ("destination_district", "subawardee_cd_name")
        direct = ("cd_118", "district", "label")
    ordered = source if side == "source" else destination if side == "destination" else direct
    identity = _first_text(row, (*ordered, *direct)) or _fallback_geo_text(row, level, side)
    if level != "county" or not identity:
        return identity
    state = _geo_state(row, side)
    return f"{state}|{identity}" if state else identity


def _geo_state(row: dict[str, Any], side: str) -> str | None:
    source = ("source_state", "rcpt_state_name", "rcpt_state")
    destination = ("destination_state", "subawardee_state_name", "subawardee_state")
    direct = ("state", "state_name", "state_abbr")
    ordered = source if side == "source" else destination if side == "destination" else direct
    return _first_text(row, (*ordered, *direct)) or _fallback_geo_text(row, "state", side)


def _focus_state(question: str, resolved: dict[str, Any]) -> str | None:
    for table_resolution in resolved.values():
        state = table_resolution.get("state") if isinstance(table_resolution, dict) else None
        if isinstance(state, dict) and state.get("value"):
            return str(state["value"]).title()
    return _state_in_question(question)


def build_map_intent(
    question: str,
    routing: dict[str, Any],
    resolved: dict[str, Any],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    disabled: dict[str, Any] = {"enabled": False, "mapType": "none"}
    if not rows:
        return disabled
    numeric = _numeric_columns(rows)
    level = _infer_level(routing, rows)
    if not numeric or level not in {"state", "county", "congress"}:
        return disabled

    tables = [str(table) for table in (routing.get("tables") or [])]
    is_flow = any(table.endswith("_flow") for table in tables)
    flow_direction = str(routing.get("flow_direction") or "none").lower()
    focus_state = _focus_state(question, resolved)
    if is_flow and flow_direction == "inflow":
        geo_side = "source" if focus_state else "destination"
    elif is_flow and flow_direction == "outflow":
        geo_side = "destination" if focus_state else "source"
    else:
        geo_side = "destination" if is_flow else "direct"

    # National county rows require an explicit state for a unique boundary
    # join (there are many Washington/Franklin/etc. counties). A state-focused
    # result is enriched with that known state before this function is called.
    if level == "county" and not focus_state and any(
        _geo_state(row, geo_side) is None for row in rows
    ):
        return {**disabled, "reason": "County rows do not include a state boundary key."}

    identities = [identity for row in rows if (identity := _geo_identity(row, level, geo_side))]
    if not identities:
        return disabled
    # A choropleth needs exactly one value per boundary. Agency×state and
    # multi-year rows used to overwrite earlier values in JavaScript Map.set().
    if len(set(identity.casefold() for identity in identities)) != len(identities):
        return {**disabled, "reason": "Multiple returned rows map to the same geography."}

    measures = _ordered_measures(question, numeric, routing)
    measure = measures[0]
    values = [float(row[measure]) for row in rows if _is_number(row.get(measure))]
    profile = _metric_profile(measure, routing, values)
    count = len(identities)

    if is_flow:
        map_type = "flow-state-focused" if focus_state else "flow-map"
    elif _COMPARE_Q.search(question) and 2 <= count <= 8:
        map_type = "atlas-comparison"
    elif focus_state and level in {"county", "congress"}:
        map_type = "single-state-ranked-subregions"
    elif focus_state and level == "state" and count == 1:
        map_type = "single-state-spotlight"
    elif re.search(r"\b(top|bottom)\s+\d+\b", question, re.I) and count <= 25:
        map_type = "top-n-highlight"
    else:
        map_type = "atlas-single-metric"

    sort_direction = "asc" if str(routing.get("sort_direction") or "").lower() == "asc" or _BOTTOM_Q.search(question) else "desc"
    title_geography = f"{geo_side} {level}" if is_flow else level
    if is_flow and focus_state and flow_direction == "inflow":
        subtitle = f"Prime-recipient origins sending subawards to {focus_state}"
    elif is_flow and focus_state and flow_direction == "outflow":
        subtitle = f"Subaward destinations receiving from {focus_state} prime recipients"
    elif is_flow:
        subtitle = f"Subaward {geo_side} geographic view"
    else:
        subtitle = (f"{focus_state} — " if focus_state else "") + "geographic view"
    intent: dict[str, Any] = {
        "enabled": True,
        "mapType": map_type,
        "level": level,
        "metric": measure,
        "metricLabel": profile["label"],
        "unit": profile["unit"],
        "sortDirection": sort_direction,
        "geoSide": geo_side,
        "title": f"{profile['label']} by {title_geography}",
        "subtitle": subtitle,
        "buttonLabel": "View map",
        "reason": f"Result has one {profile['label']} value per {level}.",
        "showLegend": True,
    }
    if focus_state and map_type != "atlas-comparison":
        intent["state"] = focus_state
    if map_type == "atlas-comparison":
        intent["comparisonIds"] = identities
        intent["comparisonLabels"] = identities
    if map_type == "top-n-highlight":
        match = re.search(r"\b(top|bottom)\s+(\d+)\b", question, re.I)
        intent["topN"] = int(match.group(2)) if match else min(count, 10)
    return intent


def enrich_rows_for_map(
    question: str,
    routing: dict[str, Any],
    resolved: dict[str, Any],
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Inject a filtered county's state only when SQL omitted it from SELECT."""
    if not rows or _infer_level(routing, rows) != "county":
        return rows
    first = rows[0]
    if any(key in first for key in ("state", "state_name", "rcpt_state", "subawardee_state", "rcpt_state_name", "subawardee_state_name")):
        return rows
    state = _focus_state(question, resolved)
    return [{**row, "state": state} for row in rows] if state else rows


def build_visuals(
    question: str,
    routing: dict[str, Any],
    resolved: dict[str, Any],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build safe visuals without ever taking down the answer pipeline."""
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
