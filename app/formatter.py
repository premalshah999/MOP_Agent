"""Result formatting — compute statistics, build evidence, generate LLM answer."""

from __future__ import annotations

import json
import os
from typing import Any, Optional

import numpy as np
import pandas as pd
from openai import OpenAI

from app.prompts import FORMATTER_SYSTEM, build_formatter_prompt


# ---------------------------------------------------------------------------
# LLM client (shared config)
# ---------------------------------------------------------------------------
def _get_client() -> OpenAI:
    return OpenAI(
        api_key=os.getenv("DEEPSEEK_API_KEY"),
        base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
        timeout=float(os.getenv("DEEPSEEK_TIMEOUT", "45")),
    )


FORMATTER_MAX_TOKENS = int(os.getenv("FORMATTER_MAX_TOKENS", "2000"))
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")


# ---------------------------------------------------------------------------
# Dataframe cleaning
# ---------------------------------------------------------------------------
def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace([np.inf, -np.inf], np.nan)
    # Drop columns that are entirely null
    df = df.dropna(axis=1, how="all")
    return df


# ---------------------------------------------------------------------------
# Column detection
# ---------------------------------------------------------------------------
_LABEL_CANDIDATES = [
    "county", "cd_118", "agency", "subawardee_state_name", "rcpt_state_name",
    "subawardee_cty_name", "rcpt_cty_name", "subawardee_cd_name", "rcpt_cd_name",
    "state",
]

_SKIP_COLUMNS = {
    "fips", "county_fips", "year", "Year", "act_dt_fis_yr",
    "prime_awardee_stcd118", "subawardee_stcd118",
    "rcpt_st_cd", "subawardee_st_cd",
}


def _detect_label_col(df: pd.DataFrame) -> Optional[str]:
    cols = [c.lower() for c in df.columns]
    for candidate in _LABEL_CANDIDATES:
        for i, c in enumerate(cols):
            if c == candidate or c.endswith(candidate):
                return df.columns[i]
    # Fallback: any column ending in "name"
    for col in df.columns:
        if col.lower().endswith("name"):
            return col
    return None


def _numeric_columns(df: pd.DataFrame, label_col: Optional[str]) -> list[str]:
    result = []
    for col in df.columns:
        if col == label_col or col in _SKIP_COLUMNS:
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            result.append(col)
    return result


# ---------------------------------------------------------------------------
# Compute statistics from the DataFrame
# ---------------------------------------------------------------------------
def compute_statistics(df: pd.DataFrame) -> dict[str, Any]:
    label_col = _detect_label_col(df)
    num_cols = _numeric_columns(df, label_col)

    stats: dict[str, Any] = {
        "row_count": len(df),
        "columns": list(df.columns),
        "label_column": label_col,
        "numeric_columns": num_cols,
        "metrics": {},
    }

    for col in num_cols:
        series = df[col].dropna()
        if series.empty:
            continue

        metric: dict[str, Any] = {
            "min": float(series.min()),
            "max": float(series.max()),
            "mean": round(float(series.mean()), 4),
            "median": round(float(series.median()), 4),
        }

        # Attach entity labels for min/max
        if label_col:
            min_idx = series.idxmin()
            max_idx = series.idxmax()
            metric["min_entity"] = str(df.at[min_idx, label_col])
            metric["max_entity"] = str(df.at[max_idx, label_col])

        stats["metrics"][col] = metric

    # Top 3 and bottom 3 rows (by first numeric column)
    if num_cols and label_col:
        primary = num_cols[0]
        sorted_df = df.dropna(subset=[primary]).sort_values(primary, ascending=False)
        top_rows = sorted_df.head(3)
        bottom_rows = sorted_df.tail(3)

        stats["top_3"] = [
            {label_col: str(row[label_col]), primary: _fmt_num(row[primary])}
            for _, row in top_rows.iterrows()
        ]
        stats["bottom_3"] = [
            {label_col: str(row[label_col]), primary: _fmt_num(row[primary])}
            for _, row in bottom_rows.iterrows()
        ]

    # Correlation if exactly 2 numeric columns
    if len(num_cols) == 2:
        s1 = df[num_cols[0]].dropna()
        s2 = df[num_cols[1]].dropna()
        common = s1.index.intersection(s2.index)
        if len(common) >= 5:
            corr = float(np.corrcoef(df.loc[common, num_cols[0]], df.loc[common, num_cols[1]])[0, 1])
            stats["correlation"] = {
                "columns": num_cols,
                "r": round(corr, 4),
                "sample_size": len(common),
            }

    return stats


def _fmt_num(value: Any) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "null"
    if isinstance(value, float):
        if abs(value) >= 1_000_000:
            return f"{value:,.0f}"
        if abs(value) >= 1:
            return f"{value:,.2f}"
        return f"{value:.4f}"
    return str(value)


# ---------------------------------------------------------------------------
# Build compact evidence text from statistics
# ---------------------------------------------------------------------------
def build_evidence_text(df: pd.DataFrame, stats: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"{stats['row_count']} rows. Columns: {', '.join(stats['columns'])}")

    label = stats.get("label_column")

    for col, m in stats.get("metrics", {}).items():
        parts = [f"min={_fmt_num(m['min'])}"]
        if m.get("min_entity"):
            parts[-1] += f" ({m['min_entity']})"
        parts.append(f"max={_fmt_num(m['max'])}")
        if m.get("max_entity"):
            parts[-1] += f" ({m['max_entity']})"
        parts.append(f"mean={_fmt_num(m['mean'])}")
        parts.append(f"median={_fmt_num(m['median'])}")
        lines.append(f"{col}: {', '.join(parts)}")

    if "top_3" in stats and label:
        primary = stats["numeric_columns"][0]
        top_str = ", ".join(f"{r[label]} ({r[primary]})" for r in stats["top_3"])
        lines.append(f"Top 3: {top_str}")
        bottom_str = ", ".join(f"{r[label]} ({r[primary]})" for r in stats["bottom_3"])
        lines.append(f"Bottom 3: {bottom_str}")

    if "correlation" in stats:
        c = stats["correlation"]
        lines.append(f"Correlation between {c['columns'][0]} and {c['columns'][1]}: r={c['r']}, n={c['sample_size']}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Build data preview for the LLM
# ---------------------------------------------------------------------------
def _build_preview(df: pd.DataFrame, max_rows: int = 12) -> str:
    preview_df = df.head(max_rows)
    try:
        return preview_df.to_string(index=False, max_colwidth=40)
    except Exception:
        return str(preview_df)


# ---------------------------------------------------------------------------
# Generate answer via LLM
# ---------------------------------------------------------------------------
def _generate_answer(question: str, evidence_text: str, preview: str, sql: str | None) -> str:
    user_prompt = build_formatter_prompt(question, evidence_text, preview, sql)

    client = _get_client()
    response = client.chat.completions.create(
        model=os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
        max_tokens=FORMATTER_MAX_TOKENS,
        temperature=0,
        messages=[
            {"role": "system", "content": FORMATTER_SYSTEM},
            {"role": "user", "content": user_prompt},
        ],
    )
    return (response.choices[0].message.content or "").strip()


# ---------------------------------------------------------------------------
# Fallback answer (no LLM available)
# ---------------------------------------------------------------------------
def _fallback_answer(df: pd.DataFrame, stats: dict[str, Any]) -> str:
    """Generate a basic answer from statistics when LLM is unavailable."""
    parts: list[str] = []
    label = stats.get("label_column")

    for col, m in stats.get("metrics", {}).items():
        if m.get("max_entity"):
            parts.append(f"**{m['max_entity']}** leads with {col} of {_fmt_num(m['max'])}.")
        parts.append(f"Across {stats['row_count']} entries, the mean {col} is {_fmt_num(m['mean'])} and the median is {_fmt_num(m['median'])}.")

    if "correlation" in stats:
        c = stats["correlation"]
        parts.append(f"The correlation between {c['columns'][0]} and {c['columns'][1]} is {c['r']:.2f} (n={c['sample_size']}).")

    return " ".join(parts) if parts else f"Query returned {stats['row_count']} rows across columns: {', '.join(stats['columns'])}."


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def format_result(question: str, df: pd.DataFrame, sql: str | None = None) -> str:
    df = _clean_df(df)
    if df.empty:
        return "No data found matching your query."

    stats = compute_statistics(df)
    evidence_text = build_evidence_text(df, stats)
    preview = _build_preview(df)

    if not os.getenv("DEEPSEEK_API_KEY"):
        return _fallback_answer(df, stats)

    try:
        return _generate_answer(question, evidence_text, preview, sql)
    except Exception:
        return _fallback_answer(df, stats)
