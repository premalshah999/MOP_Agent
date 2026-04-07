"""Auto-generate Vega-Lite chart specs from query results."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

import pandas as pd

_log = logging.getLogger(__name__)

# Maximum rows to include in inline chart data
CHART_MAX_ROWS = int(os.getenv("CHART_MAX_ROWS", "50"))


def generate_chart_spec(
    df: pd.DataFrame,
    question: str,
    sql: str | None = None,
) -> Optional[dict[str, Any]]:
    """Generate a Vega-Lite spec for the given DataFrame, or None if no chart is appropriate."""
    if df.empty or len(df.columns) < 2:
        return None

    try:
        return _auto_chart(df, question)
    except Exception as exc:
        _log.debug("Chart generation failed: %s", exc)
        return None


def _detect_columns(df: pd.DataFrame) -> tuple[Optional[str], list[str]]:
    """Detect the label (categorical) column and numeric columns."""
    _SKIP = {"fips", "county_fips", "year", "Year", "act_dt_fis_yr"}
    _LABEL_PRIORITY = [
        "state", "county", "cd_118", "agency",
        "subawardee_state_name", "rcpt_state_name",
        "subawardee_cty_name", "rcpt_cty_name",
    ]

    label_col = None
    for candidate in _LABEL_PRIORITY:
        for col in df.columns:
            if col.lower() == candidate or col.lower().endswith(candidate):
                label_col = col
                break
        if label_col:
            break

    # Fallback: first string column
    if not label_col:
        for col in df.columns:
            if col not in _SKIP and df[col].dtype == "object":
                label_col = col
                break

    num_cols = [
        c for c in df.columns
        if c != label_col and c not in _SKIP and pd.api.types.is_numeric_dtype(df[c])
    ]

    return label_col, num_cols


def _is_time_series(df: pd.DataFrame) -> Optional[str]:
    """Check if a column looks like a year/time column for line charts."""
    for col in ["year", "Year", "act_dt_fis_yr", "fiscal_year"]:
        if col in df.columns:
            return col
    return None


def _to_chart_data(df: pd.DataFrame, cols: list[str], max_rows: int = CHART_MAX_ROWS) -> list[dict[str, Any]]:
    """Convert DataFrame to Vega-Lite compatible data records."""
    subset = df[cols].head(max_rows)
    records = []
    for _, row in subset.iterrows():
        record = {}
        for c in cols:
            val = row[c]
            if pd.isna(val):
                record[c] = None
            elif hasattr(val, "item"):
                record[c] = val.item()  # numpy scalar → Python native
            else:
                record[c] = val
        records.append(record)
    return records


def _auto_chart(df: pd.DataFrame, question: str) -> Optional[dict[str, Any]]:
    """Auto-detect the best chart type based on data shape."""
    label_col, num_cols = _detect_columns(df)

    if not num_cols:
        return None

    time_col = _is_time_series(df)
    q = question.lower()

    # Single value — no chart needed
    if len(df) == 1 and len(num_cols) == 1:
        return None

    # Time series → line chart
    if time_col and len(df) > 2:
        primary_num = num_cols[0]
        cols_needed = [time_col, primary_num]
        if label_col and label_col != time_col:
            cols_needed.append(label_col)

        spec: dict[str, Any] = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container",
            "height": 260,
            "data": {"values": _to_chart_data(df, cols_needed)},
            "mark": {"type": "line", "point": True},
            "encoding": {
                "x": {"field": time_col, "type": "ordinal", "title": time_col},
                "y": {"field": primary_num, "type": "quantitative", "title": primary_num},
            },
        }
        if label_col and label_col != time_col and df[label_col].nunique() <= 8:
            spec["encoding"]["color"] = {"field": label_col, "type": "nominal"}
        return spec

    # Correlation (2 numeric cols) → scatter plot
    if len(num_cols) >= 2 and any(k in q for k in ["correlation", "relationship", "scatter", "vs"]):
        cols_needed = [num_cols[0], num_cols[1]]
        if label_col:
            cols_needed.append(label_col)
        spec = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container",
            "height": 260,
            "data": {"values": _to_chart_data(df, cols_needed)},
            "mark": {"type": "point", "filled": True, "opacity": 0.7},
            "encoding": {
                "x": {"field": num_cols[0], "type": "quantitative", "title": num_cols[0]},
                "y": {"field": num_cols[1], "type": "quantitative", "title": num_cols[1]},
            },
        }
        if label_col:
            spec["encoding"]["tooltip"] = [
                {"field": label_col, "type": "nominal"},
                {"field": num_cols[0], "type": "quantitative"},
                {"field": num_cols[1], "type": "quantitative"},
            ]
        return spec

    # Ranking / categorical → horizontal bar chart
    if label_col and num_cols and 2 <= len(df) <= 50:
        primary_num = num_cols[0]
        chart_df = df.sort_values(primary_num, ascending=False).head(CHART_MAX_ROWS)
        cols_needed = [label_col, primary_num]

        spec = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "width": "container",
            "height": max(len(chart_df) * 22, 120),
            "data": {"values": _to_chart_data(chart_df, cols_needed)},
            "mark": {"type": "bar", "cornerRadiusEnd": 3},
            "encoding": {
                "y": {
                    "field": label_col,
                    "type": "nominal",
                    "sort": "-x",
                    "title": None,
                },
                "x": {
                    "field": primary_num,
                    "type": "quantitative",
                    "title": primary_num,
                },
                "tooltip": [
                    {"field": label_col, "type": "nominal"},
                    {"field": primary_num, "type": "quantitative", "format": ",.2f"},
                ],
            },
        }
        return spec

    return None
