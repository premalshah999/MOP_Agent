"""Result formatting — compute statistics, build evidence, generate LLM answer."""

from __future__ import annotations

import json
import os
import re
import sys
from typing import Any, Optional

import numpy as np
import pandas as pd

from app.llm import llm_available, llm_complete, llm_reasoner_model
from app.prompts import FORMATTER_SYSTEM, build_formatter_prompt
from app.query_frame import infer_query_frame


# ---------------------------------------------------------------------------
# LLM client (shared config)
# ---------------------------------------------------------------------------
def _get_client():
    return None


FORMATTER_MAX_TOKENS = int(os.getenv("FORMATTER_MAX_TOKENS", "2000"))
FORMATTER_MODEL = os.getenv(
    "FORMATTER_MODEL",
    llm_reasoner_model(),
)


def _env_flag(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes"}


def _running_under_test() -> bool:
    argv = " ".join(sys.argv).lower()
    return "pytest" in argv or "unittest" in argv


def _use_llm_formatter() -> bool:
    if _running_under_test() and not _env_flag("ALLOW_LLM_FORMATTER_IN_TESTS", "0"):
        return False
    return _env_flag("USE_LLM_FORMATTER", "0")


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
_HELPER_DETAIL_COLUMNS = {
    "metric_rank",
    "rank",
    "percentile",
    "list_position",
    "total_states",
    "total_counties",
    "total_districts",
    "national_average",
    "national_median",
    "sample_size",
    "row_count",
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


def _format_entity_label(value: Any) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "Unknown"
    text = str(value).strip()
    if not text:
        return "Unknown"
    if re.fullmatch(r"[A-Z]{2}-\d{1,2}", text):
        return text
    if text.isupper() and len(text) <= 5 and " " not in text:
        return text
    if text.upper() == text and any(ch.isalpha() for ch in text):
        return text.title()
    if text.lower() == text and any(ch.isalpha() for ch in text):
        return text.title()
    return text


def _metric_display_name(metric: str, question: str | None = None) -> str:
    normalized = metric.lower()
    mapping = {
        "spending_total": "default federal spending",
        "total_flow": "total fund flow",
        "total_liabilities": "total liabilities",
        "total_assets": "total assets",
        "current_ratio": "current ratio",
        "debt_ratio": "debt ratio",
        "resident_wage": "resident wage",
        "black": "Black population share",
        "white": "White population share",
        "asian": "Asian population share",
        "hispanic": "Hispanic population share",
        "below poverty": "poverty rate",
    }
    if normalized in mapping:
        return mapping[normalized]
    return metric.replace("_", " ")


def _metric_label(metric: str) -> str:
    return _metric_display_name(metric).title()


def _section(label: str, content: str) -> str:
    return f"**{label}:** {content}"


def _is_money_metric(metric: str) -> bool:
    lowered = metric.lower()
    if "ratio" in lowered:
        return False
    money_tokens = (
        "contracts",
        "grants",
        "wage",
        "payments",
        "liabilities",
        "assets",
        "revenue",
        "expenses",
        "cash_flow",
        "cash flow",
        "bonds",
        "amount",
        "spending",
        "flow",
        "position",
    )
    return any(token in lowered for token in money_tokens)


def _is_count_metric(metric: str) -> bool:
    lowered = metric.lower()
    count_tokens = ("employees", "residents", "population", "household", "count")
    return any(token in lowered for token in count_tokens) and not _is_money_metric(metric)


def _is_percent_metric(metric: str, value: Any) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    lowered = metric.lower()
    percent_tokens = (
        "poverty",
        "education",
        "owner",
        "renter",
        "white",
        "black",
        "asian",
        "hispanic",
        "satisfied",
        "risk_averse",
        "risk averse",
    )
    return any(token in lowered for token in percent_tokens)


def _format_currency(value: float) -> str:
    sign = "-" if value < 0 else ""
    absolute = abs(float(value))
    for unit, divisor in (("T", 1_000_000_000_000), ("B", 1_000_000_000), ("M", 1_000_000), ("K", 1_000)):
        if absolute >= divisor:
            return f"{sign}${absolute / divisor:,.2f}{unit}"
    if absolute >= 100:
        return f"{sign}${absolute:,.0f}"
    return f"{sign}${absolute:,.2f}"


def _ordinal(value: int) -> str:
    if 10 <= (value % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def _format_metric_value(metric: str, value: Any) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "null"
    if _is_money_metric(metric):
        return _format_currency(float(value))
    if _is_percent_metric(metric, value):
        numeric = float(value)
        if abs(numeric) <= 1.5:
            return f"{numeric * 100:.1f}%"
        return f"{numeric:.1f}%"
    if _is_count_metric(metric):
        return f"{int(round(float(value))):,}"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value)


def _row_label(row: pd.Series, df: pd.DataFrame, label_col: Optional[str]) -> str:
    cols = set(df.columns)
    if {"rcpt_state_name", "subawardee_state_name"} <= cols:
        origin = _format_entity_label(row["rcpt_state_name"])
        destination = _format_entity_label(row["subawardee_state_name"])
        suffix = " (internal)" if origin == destination else ""
        return f"{origin} -> {destination}{suffix}"
    if {"rcpt_cty_name", "subawardee_cty_name"} <= cols:
        origin = _format_entity_label(row["rcpt_cty_name"])
        destination = _format_entity_label(row["subawardee_cty_name"])
        return f"{origin} -> {destination}"
    if {"rcpt_cd_name", "subawardee_cd_name"} <= cols:
        origin = _format_entity_label(row["rcpt_cd_name"])
        destination = _format_entity_label(row["subawardee_cd_name"])
        return f"{origin} -> {destination}"
    if label_col and label_col in row.index:
        return _format_entity_label(row[label_col])
    return "This result"


def _entity_group_name(df: pd.DataFrame, label_col: Optional[str]) -> str:
    cols = set(df.columns)
    if {"rcpt_state_name", "subawardee_state_name"} <= cols or {"rcpt_cty_name", "subawardee_cty_name"} <= cols or {"rcpt_cd_name", "subawardee_cd_name"} <= cols:
        return "flow pairs"
    if label_col in {"rcpt_state_name", "subawardee_state_name"}:
        return "states"
    if label_col in {"rcpt_cty_name", "subawardee_cty_name"}:
        return "counties"
    if label_col in {"rcpt_cd_name", "subawardee_cd_name"}:
        return "districts"
    if label_col == "state":
        return "states"
    if label_col == "county":
        return "counties"
    if label_col == "agency":
        return "agencies"
    if label_col == "cd_118":
        return "districts"
    return "rows"


def _component_columns(df: pd.DataFrame, primary: str, label_col: Optional[str]) -> list[str]:
    return [
        col
        for col in _numeric_columns(df, label_col)
        if col != primary and col not in _HELPER_DETAIL_COLUMNS
    ]


def _as_float(value: Any) -> float | None:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        return float(value)
    except Exception:
        return None


def _share_percent(part: float, whole: float) -> str:
    if whole == 0:
        return "0.0%"
    return f"{(part / whole) * 100:.1f}%"


def _component_breakdown_text(row: pd.Series, primary: str, component_cols: list[str]) -> str | None:
    if len(component_cols) < 2:
        return None

    values: list[tuple[str, float]] = []
    for col in component_cols:
        numeric = _as_float(row[col])
        if numeric is None:
            continue
        values.append((col, numeric))

    if len(values) < 2:
        return None

    total = _as_float(row[primary])
    if total is None or total <= 0:
        total = sum(value for _, value in values if value > 0)
    if total <= 0:
        return None

    values.sort(key=lambda item: item[1], reverse=True)
    top_values = values[:3]
    phrases = [
        f"**{_metric_label(col)}** ({_format_metric_value(col, value)}, {_share_percent(value, total)})"
        for col, value in top_values
    ]
    if len(phrases) == 2:
        return f"The largest components are {phrases[0]} and {phrases[1]}."
    return f"The largest components are {phrases[0]}, {phrases[1]}, and {phrases[2]}."


def _ranking_concentration_text(df: pd.DataFrame, primary: str) -> str | None:
    series = df[primary].dropna()
    if len(series) < 3:
        return None
    total = float(series.sum())
    if total <= 0:
        return None
    top_three = float(series.head(3).sum())
    return (
        f"The top 3 together account for **{_format_metric_value(primary, top_three)}**, "
        f"or **{_share_percent(top_three, total)}** of the total represented by these returned rows."
    )


def _definition_note(primary: str, question: str | None = None) -> str | None:
    lowered = primary.lower()
    question_lower = (question or "").lower()

    if primary == "spending_total":
        return (
            "**Definition:** I interpreted broad federal spending using the dashboard default of "
            "**Contracts + Grants + Resident Wage**. It does **not** include **Direct Payments**, "
            "**Employees**, **Federal Residents**, or **Employees Wage** unless you ask for them."
        )

    if "per 1000" in lowered or "per_1000" in lowered or "per_capita" in lowered or "per capita" in lowered:
        return "**Definition:** This is a stored **normalized** dashboard metric, so it reflects **relative exposure** rather than a raw total."

    if primary in {"Debt_Ratio", "Current_Ratio"}:
        return "**Definition:** This is a **ratio**, so it should be interpreted as a relative fiscal indicator rather than a dollar total."

    if primary in {"financial_literacy", "financial_constraint", "alternative_financing", "satisfied", "risk_averse"}:
        return "**Definition:** This is a **FINRA survey-derived score/share**, not an administrative spending total."

    if "spending" in question_lower and primary in {"Contracts", "Grants", "Resident Wage", "Direct Payments"}:
        return f"**Definition:** This answer uses the **{_metric_label(primary)}** channel specifically, rather than the default composite spending definition."

    return None


def _comparison_note(question: str, sorted_df: pd.DataFrame, primary: str, label_col: Optional[str], ascending: bool) -> str | None:
    if not label_col or len(sorted_df) < 2:
        return None
    q = question.lower()
    if not any(token in q for token in ("compare", "versus", " vs ", "against")):
        return None

    lead = sorted_df.iloc[0]
    trail = sorted_df.iloc[-1]
    lead_label = _row_label(lead, sorted_df, label_col)
    trail_label = _row_label(trail, sorted_df, label_col)
    try:
        gap = abs(float(lead[primary]) - float(trail[primary]))
    except Exception:
        return None

    direction = "lower than" if ascending else "ahead of"
    return (
        f"**Interpretation:** This is a descriptive comparison on **{_metric_display_name(primary, question)}**. "
        f"**{lead_label}** is {direction} **{trail_label}** by **{_format_metric_value(primary, gap)}**."
    )


def _scope_note(sql: str | None, primary: str) -> str | None:
    if not sql:
        return None
    sql_lower = sql.lower()
    if " from gov_congress" in sql_lower:
        return "**Scope:** This answer uses the **processed congress-level government finance** file for district comparisons in **FY2023**."
    if " from acs_congress" in sql_lower:
        return "**Scope:** This answer uses the **processed congress-level ACS** file for district comparisons."
    if " from finra_congress" in sql_lower:
        return "**Scope:** This answer uses the **processed congress-level FINRA** file, which is currently available only for **2021**."
    if " from contract_congress" in sql_lower:
        if "year = '2024'" in sql_lower:
            return "**Scope:** This answer uses the **processed congress-level federal spending** file for **2024**."
        if "year = '2020-2024'" in sql_lower:
            return "**Scope:** This answer uses the **processed congress-level federal spending** file for the **2020-2024** aggregate period."
        return "**Scope:** This answer uses the **processed congress-level federal spending** file."
    if " from gov_" in sql_lower:
        return "**Scope:** This answer is based on the **FY2023 government finance** dataset."
    if " from contract_" in sql_lower or " from spending_state_agency" in sql_lower or " from spending_state " in sql_lower:
        if "year = '2024'" in sql_lower:
            if primary == "spending_total":
                return "**Scope:** This answer uses the **2024** federal spending data and the dashboard default of **Contracts + Grants + Resident Wage**."
            return "**Scope:** This answer uses the **2024** federal spending data."
        if "year = '2020-2024'" in sql_lower:
            return "**Scope:** This answer uses the **2020-2024 aggregate** federal spending period, not single-year 2024."
    if " from state_flow" in sql_lower:
        return "**Scope:** This comes from the **state fund-flow** table, so it ranks state-to-state subcontract flow pairs rather than federal spending totals."
    if " from county_flow" in sql_lower or " from congress_flow" in sql_lower:
        return "**Scope:** This comes from the **fund-flow** tables, so it reflects subcontract movement rather than direct federal spending totals."
    return None


def _top_n_block(sorted_df: pd.DataFrame, df: pd.DataFrame, label_col: str, primary: str, n: int = 5) -> str:
    rows = sorted_df.head(n)
    lines = []
    for index, (_, row) in enumerate(rows.iterrows(), start=1):
        lines.append(f"{index}. **{_row_label(row, df, label_col)}** - {_format_metric_value(primary, row[primary])}")
    return "\n".join(lines)


def _ordered_subset_block(rows_df: pd.DataFrame, source_df: pd.DataFrame, label_col: str, primary: str) -> str:
    lines = []
    for index, (_, row) in enumerate(rows_df.iterrows(), start=1):
        lines.append(f"{index}. **{_row_label(row, source_df, label_col)}** - {_format_metric_value(primary, row[primary])}")
    return "\n".join(lines)


def _ordered_ranked_subset_block(rows_df: pd.DataFrame, source_df: pd.DataFrame, label_col: str, primary: str) -> str:
    lines = []
    for _, row in rows_df.iterrows():
        rank_value = _as_float(row.get("metric_rank")) or _as_float(row.get("rank"))
        rank_prefix = f"{_ordinal(int(rank_value))}: " if rank_value is not None else ""
        lines.append(
            f"- {rank_prefix}**{_row_label(row, source_df, label_col)}** - {_format_metric_value(primary, row[primary])}"
        )
    return "\n".join(lines)


def _ranking_answer_lead(question: str, primary: str, top_label: str, top_value: str, ascending: bool, frame) -> str:
    if frame.family == "flow" and primary == "total_flow" and frame.primary_state:
        focus_state = " ".join(part.capitalize() for part in frame.primary_state.split())
        if frame.flow_direction == "inflow":
            return f"**Answer:** **{top_label}** sends the most subcontract inflow into **{focus_state}**, at **{top_value}**."
        if frame.flow_direction == "outflow":
            return f"**Answer:** **{top_label}** receives the most subcontract outflow from **{focus_state}**, at **{top_value}**."
        return f"**Answer:** **{top_label}** is the largest subcontract flow counterpart for **{focus_state}**, at **{top_value}**."

    if primary == "spending_total" and frame.family in {"agency", "breakdown"} and frame.primary_state:
        focus_state = " ".join(part.capitalize() for part in frame.primary_state.split())
        return f"**Answer:** **{top_label}** leads **{focus_state}** on default federal spending, at **{top_value}**."

    metric_name = _metric_display_name(primary, question)
    if ascending:
        return f"**Answer:** **{top_label}** has the lowest {metric_name}, at **{top_value}**."
    return f"**Answer:** **{top_label}** has the highest {metric_name}, at **{top_value}**."


def _interpretation_for_single_row(row: pd.Series, primary: str, question: str) -> str | None:
    rank_value = _as_float(row.get("metric_rank")) or _as_float(row.get("rank"))
    total_entities = _as_float(row.get("total_states")) or _as_float(row.get("total_counties")) or _as_float(row.get("total_districts"))
    national_average = _as_float(row.get("national_average"))
    current_value = _as_float(row.get(primary))

    notes: list[str] = []

    if rank_value is not None and total_entities:
        share = rank_value / total_entities
        if share <= 0.2:
            notes.append("That places it in the top fifth of the ranking.")
        elif share <= 0.5:
            notes.append("That places it in the upper half of the ranking.")
        elif share <= 0.8:
            notes.append("That places it in the lower half of the ranking.")
        else:
            notes.append("That places it near the bottom of the ranking.")

    if national_average is not None and current_value is not None:
        delta = current_value - national_average
        if abs(delta) > 0:
            direction = "above" if delta > 0 else "below"
            notes.append(
                f"It is **{_format_metric_value(primary, abs(delta))}** {direction} the national average of **{_format_metric_value(primary, national_average)}**."
            )

    if not notes:
        return None
    return " ".join(notes)


def _leaderboard_context_answer(question: str, df: pd.DataFrame, label_col: str, primary: str, sql: str | None, frame) -> str | None:
    if "row_kind" not in df.columns:
        return None

    focus_rows = df[df["row_kind"] == "focus"]
    nearby_rows = df[df["row_kind"] == "nearby"]
    top_rows = df[df["row_kind"] == "top"]
    bottom_rows = df[df["row_kind"] == "bottom"]
    if focus_rows.empty or (top_rows.empty and bottom_rows.empty):
        return None

    focus = focus_rows.iloc[0]
    entity = _row_label(focus, df, label_col)
    primary_name = _metric_display_name(primary, question)
    primary_value = _format_metric_value(primary, focus[primary])
    rank_value = _as_float(focus.get("metric_rank")) or _as_float(focus.get("rank"))
    total_entities = _as_float(focus.get("total_states")) or _as_float(focus.get("total_counties")) or _as_float(focus.get("total_districts"))

    if rank_value is not None and total_entities is not None:
        lead = _section(
            "Answer",
            f"**{entity}** ranks **{_ordinal(int(rank_value))}** out of **{int(total_entities)}** on **{primary_name}**, at **{primary_value}**.",
        )
    else:
        lead = _section("Answer", f"**{entity}** has **{primary_name}** of **{primary_value}**.")

    lines = [lead]
    definition = _definition_note(primary, question)
    if definition:
        lines.append(definition)

    nearby_ordered = nearby_rows.sort_values("metric_rank") if "metric_rank" in nearby_rows.columns else nearby_rows
    top_ordered = top_rows.sort_values("list_position") if "list_position" in top_rows.columns else top_rows.sort_values("metric_rank")
    bottom_ordered = bottom_rows.sort_values("list_position") if "list_position" in bottom_rows.columns else bottom_rows.sort_values("metric_rank", ascending=False)

    if not nearby_ordered.empty:
        lines.append(
            f"**Around {entity}:**\n"
            + _ordered_ranked_subset_block(nearby_ordered, df, label_col, primary)
        )
    if not top_ordered.empty:
        lines.append(f"**Top {len(top_ordered)}:**\n" + _ordered_subset_block(top_ordered, df, label_col, primary))
    if not bottom_ordered.empty:
        lines.append(f"**Bottom {len(bottom_ordered)}:**\n" + _ordered_subset_block(bottom_ordered, df, label_col, primary))

    context_bits: list[str] = []
    national_average = _as_float(focus.get("national_average"))
    if national_average is not None:
        context_bits.append(f"National average: **{_format_metric_value(primary, national_average)}**")
    if rank_value is not None and total_entities is not None:
        percentile = (rank_value / total_entities) * 100
        context_bits.append(
            f"That places **{entity}** at **{_ordinal(int(rank_value))}** nationally within this dataset slice, or roughly the **{percentile:.0f}th percentile from the top**"
        )
    if not top_ordered.empty:
        leader = top_ordered.iloc[0]
        context_bits.append(
            f"The national leader in this slice is **{_row_label(leader, df, label_col)}** at **{_format_metric_value(primary, leader[primary])}**"
        )
    if not bottom_ordered.empty:
        tail = bottom_ordered.iloc[0]
        context_bits.append(
            f"The lowest state in this slice is **{_row_label(tail, df, label_col)}** at **{_format_metric_value(primary, tail[primary])}**"
        )
    if context_bits:
        lines.append(_section("Context", ". ".join(context_bits) + "."))

    interpretation = _interpretation_for_single_row(focus, primary, question)
    if interpretation:
        extra_notes: list[str] = [interpretation]
        if nearby_ordered.shape[0] >= 2:
            above_rows = nearby_ordered[nearby_ordered["metric_rank"] < rank_value] if rank_value is not None and "metric_rank" in nearby_ordered.columns else nearby_ordered.head(1)
            below_rows = nearby_ordered[nearby_ordered["metric_rank"] > rank_value] if rank_value is not None and "metric_rank" in nearby_ordered.columns else nearby_ordered.tail(1)
            if not above_rows.empty:
                above = above_rows.iloc[-1]
                extra_notes.append(
                    f"The closest higher-ranked peer shown here is **{_row_label(above, df, label_col)}** at **{_format_metric_value(primary, above[primary])}**."
                )
            if not below_rows.empty:
                below = below_rows.iloc[0]
                extra_notes.append(
                    f"The closest lower-ranked peer shown here is **{_row_label(below, df, label_col)}** at **{_format_metric_value(primary, below[primary])}**."
                )
        lines.append(_section("Interpretation", " ".join(extra_notes)))

    follow_ups = _follow_up_suggestions(question, focus_rows, label_col, primary, frame)
    if follow_ups:
        lines.append("**You could ask next:**\n" + "\n".join(f"- {item}" for item in follow_ups))
    scope = _scope_note(sql, primary)
    if scope:
        lines.append(scope)
    return "\n\n".join(lines)


def _interpretation_for_ranking(
    sorted_df: pd.DataFrame,
    primary: str,
    top: pd.Series,
    second: pd.Series | None,
    metric_stats: dict[str, Any] | None,
) -> str | None:
    notes: list[str] = []
    top_value = _as_float(top.get(primary))
    second_value = _as_float(second.get(primary)) if second is not None else None
    mean_value = _as_float(metric_stats.get("mean")) if metric_stats else None

    if top_value is not None and second_value is not None and second_value != 0:
        ratio = top_value / second_value
        if ratio >= 1.5:
            notes.append("The leader is materially ahead of the next result, so this is not a tight race.")
        else:
            notes.append("The top of the ranking is relatively tight rather than dominated by one outlier.")

    if top_value is not None and mean_value is not None and mean_value != 0:
        mean_ratio = top_value / mean_value
        if mean_ratio >= 2:
            notes.append("The top result also sits well above the returned-set average, which suggests a concentrated distribution.")

    if len(sorted_df) >= 5 and not _is_percent_metric(primary, top_value):
        total = float(sorted_df[primary].dropna().sum())
        top_three = float(sorted_df[primary].dropna().head(3).sum())
        if total > 0:
            notes.append(
                f"The top 3 together account for **{_format_metric_value(primary, top_three)}**, or **{_share_percent(top_three, total)}** of the total represented by these returned rows."
            )

    if not notes:
        return None
    return " ".join(notes)


def _follow_up_suggestions(question: str, df: pd.DataFrame, label_col: Optional[str], primary: str, frame) -> list[str]:
    suggestions: list[str] = []
    metric_name = _metric_display_name(primary, question)

    if label_col and not df.empty:
        top_label = _row_label(df.iloc[0], df, label_col)
        if frame.family == "gov":
            suggestions.append(f"How does **{top_label}** compare with the national average on **{metric_name}**?")
        elif frame.family == "contract":
            suggestions.append(f"Show the top 10 places on **{metric_name}** instead of just the leaders.")
        elif frame.family == "flow" and frame.primary_state:
            focus_state = " ".join(part.capitalize() for part in frame.primary_state.split())
            suggestions.append(f"Break down that subcontract flow for **{focus_state}** by agency.")
        elif frame.family == "agency" and frame.primary_state:
            focus_state = " ".join(part.capitalize() for part in frame.primary_state.split())
            suggestions.append(f"Show the same ranking for **{focus_state}**, but by **contracts only**.")

    if frame.primary_state and frame.geo_level in {"county", "congress"}:
        state_label = " ".join(part.capitalize() for part in frame.primary_state.split())
        suggestions.append(f"Compare the top subregions within **{state_label}** on **{metric_name}**.")

    if primary == "spending_total":
        suggestions.append("Break that total into **Contracts**, **Grants**, and **Resident Wage**.")
    elif "Per 1000" not in primary and "per_1000" not in primary.lower() and frame.family in {"contract", "gov"}:
        suggestions.append(f"Show the same result using a **relative** version of **{metric_name}** if available.")

    if frame.family == "flow":
        suggestions.append("Show the largest **external** flows only, excluding internal same-place flows.")

    deduped: list[str] = []
    for suggestion in suggestions:
        if suggestion not in deduped:
            deduped.append(suggestion)
    return deduped[:3]


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


def _grounded_summary(question: str, df: pd.DataFrame, stats: dict[str, Any], sql: str | None = None) -> str:
    label_col = stats.get("label_column")
    primary = _choose_primary_metric(question, df, label_col)
    if primary is None:
        return _fallback_answer(df, stats)
    frame = infer_query_frame(question)

    if any(_is_year_like(col) for col in df.columns):
        year_col = next(col for col in df.columns if _is_year_like(col))
        ordered = df.sort_values(year_col)
        first = ordered.iloc[0]
        last = ordered.iloc[-1]
        lines = [_section(
            "Answer",
            f"{year_col} runs from {first[year_col]} to {last[year_col]}. "
            f"{_metric_display_name(primary, question)} moves from {_fmt_num(first[primary])} "
            f"to {_fmt_num(last[primary])} across {len(ordered)} periods.",
        )]
        definition = _definition_note(primary, question)
        if definition:
            lines.append(definition)
        scope = _scope_note(sql, primary)
        if scope:
            lines.append(scope)
        return "\n\n".join(lines)

    if len(df) == 1:
        row = df.iloc[0]
        entity = _row_label(row, df, label_col)
        primary_name = _metric_display_name(primary, question)
        primary_value = _format_metric_value(primary, row[primary])
        component_cols = _component_columns(df, primary, label_col)
        rank_value = _as_float(row.get("metric_rank")) or _as_float(row.get("rank"))
        total_entities = _as_float(row.get("total_states")) or _as_float(row.get("total_counties")) or _as_float(row.get("total_districts"))

        if rank_value is not None and total_entities is not None:
            lead = _section(
                "Answer",
                f"**{entity}** ranks **{_ordinal(int(rank_value))}** out of **{int(total_entities)}** on "
                f"**{primary_name}**, at **{primary_value}**.",
            )
        elif primary == "spending_total":
            lead = _section("Answer", f"**{entity}** receives about **{primary_value}** in default federal spending.")
        else:
            lead = _section("Answer", f"**{entity}** has **{primary_name}** of **{primary_value}**.")

        detail_parts = []
        for col in _numeric_columns(df, label_col):
            if col == primary or col in _HELPER_DETAIL_COLUMNS:
                continue
            detail_parts.append(f"**{_metric_label(col)}:** {_format_metric_value(col, row[col])}")

        lines = [lead]
        definition = _definition_note(primary, question)
        if definition:
            lines.append(definition)
        if detail_parts:
            lines.append(_section("Breakdown", ", ".join(detail_parts[:4]) + "."))
        component_text = _component_breakdown_text(row, primary, component_cols)
        if component_text:
            lines.append(_section("Composition", component_text))
        context_bits: list[str] = []
        national_average = _as_float(row.get("national_average"))
        national_median = _as_float(row.get("national_median"))
        if national_average is not None:
            context_bits.append(f"National average: **{_format_metric_value(primary, national_average)}**")
        if national_median is not None:
            context_bits.append(f"National median: **{_format_metric_value(primary, national_median)}**")
        if context_bits:
            lines.append(_section("Context", ". ".join(context_bits) + "."))
        interpretation = _interpretation_for_single_row(row, primary, question)
        if interpretation:
            lines.append(_section("Interpretation", interpretation))
        follow_ups = _follow_up_suggestions(question, df, label_col, primary, frame)
        if follow_ups:
            lines.append("**You could ask next:**\n" + "\n".join(f"- {item}" for item in follow_ups))
        scope = _scope_note(sql, primary)
        if scope:
            lines.append(scope)
        return "\n\n".join(lines)

    if label_col:
        leaderboard_answer = _leaderboard_context_answer(question, df, label_col, primary, sql, frame)
        if leaderboard_answer:
            return leaderboard_answer

        ascending = any(token in question.lower() for token in ["lowest", "least", "bottom", "smallest"])
        sorted_df = df.dropna(subset=[primary]).sort_values(primary, ascending=ascending)
        top = sorted_df.iloc[0]
        second = sorted_df.iloc[1] if len(sorted_df) > 1 else None
        third = sorted_df.iloc[2] if len(sorted_df) > 2 else None
        tail = sorted_df.iloc[-1]
        top_five = sorted_df.head(5)
        entity_group = _entity_group_name(df, label_col)
        primary_name = _metric_display_name(primary, question)
        top_label = _row_label(top, df, label_col)
        tail_label = _row_label(tail, df, label_col)
        top_value = _format_metric_value(primary, top[primary])
        tail_value = _format_metric_value(primary, tail[primary])
        top_five_text = _top_n_block(sorted_df, df, label_col, primary, n=min(5, len(top_five)))

        lead = _ranking_answer_lead(question, primary, top_label, top_value, ascending, frame)

        follow_parts: list[str] = []
        if second is not None:
            follow_parts.append(f"**{_row_label(second, df, label_col)}** - **{_format_metric_value(primary, second[primary])}**")
        if third is not None:
            follow_parts.append(f"**{_row_label(third, df, label_col)}** - **{_format_metric_value(primary, third[primary])}**")

        lines = [lead]
        definition = _definition_note(primary, question)
        if definition:
            lines.append(definition)
        if follow_parts:
            lines.append(_section("Next up", "; ".join(follow_parts) + "."))
        component_cols = _component_columns(df, primary, label_col)
        leader_profile = _component_breakdown_text(top, primary, component_cols)
        if leader_profile:
            if leader_profile.startswith("The "):
                leader_profile = "the " + leader_profile[4:]
            lines.append(_section("Leader profile", f"For **{top_label}**, {leader_profile}"))
        comparison_note = _comparison_note(question, sorted_df, primary, label_col, ascending)
        if comparison_note:
            lines.append(comparison_note)
        lines.append("**Top 5:**\n" + top_five_text)
        metric_stats = stats.get("metrics", {}).get(primary)
        context_bits = [
            f"Across the {stats['row_count']} returned {entity_group}, values range from **{tail_value}** for **{tail_label}** "
            f"to **{top_value}** for **{top_label}**."
        ]
        if second is not None:
            try:
                gap = float(top[primary]) - float(second[primary])
                context_bits.append(
                    f"The gap between first and second place is **{_format_metric_value(primary, gap)}**."
                )
            except Exception:
                pass
        if " (internal)" in top_label:
            context_bits.append("The top result is an internal flow, meaning the origin and destination are the same place.")
        if metric_stats and frame.family not in {"flow", "agency"}:
            context_bits.append(
                f"For the returned set, the mean is **{_format_metric_value(primary, metric_stats['mean'])}** and the median is "
                f"**{_format_metric_value(primary, metric_stats['median'])}**."
            )
        lines.append(_section("Context", " ".join(context_bits)))
        interpretation = _interpretation_for_ranking(sorted_df, primary, top, second, metric_stats)
        if interpretation:
            lines.append(_section("Interpretation", interpretation))
        follow_ups = _follow_up_suggestions(question, sorted_df, label_col, primary, frame)
        if follow_ups:
            lines.append("**You could ask next:**\n" + "\n".join(f"- {item}" for item in follow_ups))
        scope = _scope_note(sql, primary)
        if scope:
            lines.append(scope)
        return "\n\n".join(lines)

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
def _generate_answer(question: str, evidence_text: str, preview: str, sql: str | None, grounded_draft: str) -> str:
    user_prompt = build_formatter_prompt(question, evidence_text, preview, sql, grounded_draft)
    return llm_complete(
        [
            {"role": "system", "content": FORMATTER_SYSTEM},
            {"role": "user", "content": user_prompt},
        ],
        model=FORMATTER_MODEL,
        max_tokens=FORMATTER_MAX_TOKENS,
        temperature=0,
    ).strip()


def _normalize_generated_answer(answer: str) -> str:
    return (
        answer.replace("→", "->")
        .replace("—", "-")
        .replace("–", "-")
        .strip()
    )


def _required_markers_from_grounded(grounded: str) -> list[str]:
    markers = [
        "**Definition:**",
        "**Breakdown:**",
        "**Composition:**",
        "**Leader profile:**",
        "**Top 5:**",
        "**Context:**",
        "**Interpretation:**",
        "**You could ask next:**",
        "**Scope:**",
    ]
    return [marker for marker in markers if marker in grounded]


def _should_fallback_to_grounded(generated: str, grounded: str) -> bool:
    normalized = _normalize_generated_answer(generated)
    grounded_lower = grounded.lower()
    normalized_lower = normalized.lower()

    for marker in _required_markers_from_grounded(grounded):
        if marker not in normalized:
            return True

    phrase_guards = [
        "default federal spending",
        "top 3 together account for",
        "->",
    ]
    for phrase in phrase_guards:
        if phrase in grounded_lower and phrase not in normalized_lower:
            return True

    return False


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
    grounded = _grounded_summary(question, df, stats, sql=sql)
    if not _use_llm_formatter():
        return grounded

    if not llm_available():
        return grounded

    evidence_text = build_evidence_text(df, stats)
    preview = _build_preview(df)

    try:
        generated = _generate_answer(question, evidence_text, preview, sql, grounded)
        normalized = _normalize_generated_answer(generated)
        if _should_fallback_to_grounded(normalized, grounded):
            return grounded
        return normalized
    except Exception:
        return grounded
