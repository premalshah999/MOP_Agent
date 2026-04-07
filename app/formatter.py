"""Result formatting — compute statistics, build evidence, generate LLM answer."""

from __future__ import annotations

import json
import os
import re
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
USE_LLM_FORMATTER = os.getenv("USE_LLM_FORMATTER", "0").strip().lower() in {"1", "true", "yes"}


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


def _is_year_like(column_name: str) -> bool:
    lowered = column_name.lower()
    return lowered in {"year", "period", "fiscal_year", "act_dt_fis_yr"} or lowered.endswith("_year")


def _choose_primary_metric(question: str, df: pd.DataFrame, label_col: Optional[str]) -> Optional[str]:
    preferred = [
        "spending_total",
        "total_federal_amount",
        "total_amount",
        "total_flow",
        "debt_ratio",
        "revenue_per_capita",
        "financial_literacy",
    ]
    lowered_map = {col.lower(): col for col in df.columns}
    for col in preferred:
        if col in lowered_map:
            return lowered_map[col]

    q = re.sub(r"[^a-z0-9]+", " ", question.lower())
    best_col = None
    best_score = -1
    for col in _numeric_columns(df, label_col):
        tokens = set(re.sub(r"[^a-z0-9]+", " ", col.lower()).split())
        score = len(tokens & set(q.split()))
        if score > best_score:
            best_col = col
            best_score = score
    if best_col:
        return best_col

    numeric_cols = _numeric_columns(df, label_col)
    return numeric_cols[0] if numeric_cols else None


def _grounded_summary(question: str, df: pd.DataFrame, stats: dict[str, Any]) -> str:
    label_col = stats.get("label_column")
    primary = _choose_primary_metric(question, df, label_col)
    if primary is None:
        return _fallback_answer(df, stats)

    if any(_is_year_like(col) for col in df.columns):
        year_col = next(col for col in df.columns if _is_year_like(col))
        ordered = df.sort_values(year_col)
        first = ordered.iloc[0]
        last = ordered.iloc[-1]
        return (
            f"{year_col} runs from {first[year_col]} to {last[year_col]}. "
            f"{primary} moves from {_fmt_num(first[primary])} to {_fmt_num(last[primary])} "
            f"across {len(ordered)} periods."
        )

    if len(df) == 1:
        row = df.iloc[0]
        pieces: list[str] = []
        if label_col:
            pieces.append(f"{row[label_col]} is the matching result.")
        for col in _numeric_columns(df, label_col)[:5]:
            pieces.append(f"{col} = {_fmt_num(row[col])}")
        return " ".join(pieces)

    if label_col:
        ascending = any(token in question.lower() for token in ["lowest", "least", "bottom", "smallest"])
        sorted_df = df.dropna(subset=[primary]).sort_values(primary, ascending=ascending)
        top = sorted_df.iloc[0]
        second = sorted_df.iloc[1] if len(sorted_df) > 1 else None
        third = sorted_df.iloc[2] if len(sorted_df) > 2 else None
        tail = sorted_df.iloc[-1]
        top_five = sorted_df.head(5)
        top_five_text = ", ".join(f"{row[label_col]} ({_fmt_num(row[primary])})" for _, row in top_five.iterrows())
        parts = [f"{top[label_col]} leads on {primary} at {_fmt_num(top[primary])}."]
        if second is not None:
            parts.append(f"Next is {second[label_col]} at {_fmt_num(second[primary])}.")
        if third is not None:
            parts.append(f"Third is {third[label_col]} at {_fmt_num(third[primary])}.")
        parts.append(f"The top returned entries are {top_five_text}.")
        parts.append(
            f"Across the {stats['row_count']} rows in this result, values range from "
            f"{_fmt_num(tail[primary])} for {tail[label_col]} to {_fmt_num(top[primary])} for {top[label_col]}."
        )
        metric_stats = stats.get("metrics", {}).get(primary)
        if metric_stats:
            parts.append(
                f"Across {stats['row_count']} rows, the mean is {_fmt_num(metric_stats['mean'])} "
                f"and the median is {_fmt_num(metric_stats['median'])}."
            )
        if second is not None:
            try:
                gap = float(top[primary]) - float(second[primary])
                parts.append(
                    f"The gap between first and second place is {_fmt_num(gap)}, which helps show "
                    "whether the leader is only slightly ahead or clearly separated from the pack."
                )
            except Exception:
                pass
        return " ".join(parts)

    return _fallback_answer(df, stats)


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
    grounded = _grounded_summary(question, df, stats)
    if not USE_LLM_FORMATTER:
        return grounded

    evidence_text = build_evidence_text(df, stats)
    preview = _build_preview(df)

    if not os.getenv("DEEPSEEK_API_KEY"):
        return grounded

    try:
        return _generate_answer(question, evidence_text, preview, sql)
    except Exception:
        return grounded
