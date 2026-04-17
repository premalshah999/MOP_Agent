from __future__ import annotations

from typing import Any

import pandas as pd

from app.metadata_utils import default_year_value
from app.query_frame import QueryFrame, STATE_TO_POSTAL, infer_query_frame


_NONE = {"enabled": False, "mapType": "none"}


def _humanize_metric(metric: str | None) -> str:
    if not metric:
        return "Metric"
    if metric == "spending_total":
        return "Default federal spending"
    return metric.replace("_", " ").strip()


def _table_dataset_level(table_name: str | None) -> tuple[str | None, str | None]:
    if not table_name:
        return None, None
    if table_name.startswith("acs_"):
        return "census", table_name.removeprefix("acs_")
    if table_name.startswith("gov_"):
        return "gov_spending", table_name.removeprefix("gov_")
    if table_name.startswith("finra_"):
        return "finra", table_name.removeprefix("finra_")
    if table_name.startswith("contract_"):
        return "contract_static", table_name.removeprefix("contract_")
    if table_name == "spending_state":
        return "spending_breakdown", "state"
    if table_name == "spending_state_agency":
        return "contract_agency", "state"
    if table_name == "state_flow":
        return "fund_flow", "state"
    if table_name == "county_flow":
        return "fund_flow", "county"
    if table_name == "congress_flow":
        return "fund_flow", "congress"
    return None, None


def _default_year_for_table(table_name: str | None) -> str | None:
    if not table_name:
        return None
    if table_name.startswith("gov_"):
        return "Fiscal Year 2023"
    default = default_year_value(table_name)
    return str(default) if default is not None else None


def _first_numeric_column(df: pd.DataFrame) -> str | None:
    helper_columns = {
        "row_count",
        "rank",
        "percentile",
        "list_position",
        "sample_size",
        "state_fips",
        "county_fips",
        "fips",
    }
    for column in df.columns:
        if column in helper_columns:
            continue
        if pd.api.types.is_numeric_dtype(df[column]):
            return str(column)
    return None


def _geo_columns_present(df: pd.DataFrame) -> bool:
    cols = set(df.columns)
    return bool({"state", "county", "cd_118", "rcpt_state_name", "subawardee_state_name"} & cols)


def _focus_ids(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return []
    row = df.iloc[0]
    for column in ("state_fips", "county_fips", "fips", "cd_118", "state", "county", "rcpt_state_name", "subawardee_state_name"):
        value = row.get(column)
        if value is not None and str(value).strip():
            return [str(value)]
    return []


def _unique_geo_count(df: pd.DataFrame, level: str | None) -> int:
    if df.empty:
        return 0
    if level == "state" and "state" in df.columns:
        return int(df["state"].dropna().astype(str).str.lower().nunique())
    if level == "county":
        if {"state", "county"}.issubset(df.columns):
            return int((df["state"].astype(str).str.lower() + "::" + df["county"].astype(str).str.lower()).nunique())
        if "county" in df.columns:
            return int(df["county"].dropna().astype(str).str.lower().nunique())
    if level == "congress" and "cd_118" in df.columns:
        return int(df["cd_118"].dropna().astype(str).str.upper().nunique())
    return 0


def _single_agency(df: pd.DataFrame) -> str | None:
    if "agency" not in df.columns:
        return None
    agencies = [str(value).strip() for value in df["agency"].dropna().tolist() if str(value).strip()]
    unique = sorted(set(agencies))
    if len(unique) == 1:
        return unique[0]
    return None


def _has_multi_agency_non_geo_shape(df: pd.DataFrame, level: str | None) -> bool:
    if "agency" not in df.columns:
        return False
    agency_count = int(df["agency"].dropna().astype(str).nunique())
    if agency_count <= 1:
        return False
    return _unique_geo_count(df, level) <= 1


def _comparison_ids(frame: QueryFrame, level: str | None) -> list[str]:
    if level != "state":
        return []
    ids: list[str] = []
    for state_name in frame.state_names:
        postal = STATE_TO_POSTAL.get(state_name)
        if postal and postal not in ids:
            ids.append(postal)
    return ids


def _comparison_labels(frame: QueryFrame, level: str | None) -> list[str]:
    if level != "state":
        return []
    labels: list[str] = []
    for state_name in frame.state_names:
        label = _state_title(state_name)
        if label and label not in labels:
            labels.append(label)
    return labels


def _state_title(state_name: str | None) -> str | None:
    if not state_name:
        return None
    return " ".join(part.capitalize() for part in state_name.split())


def _level_label(level: str | None) -> str:
    if level == "county":
        return "counties"
    if level == "congress":
        return "districts"
    return "states"


def _resolve_metric(frame: QueryFrame, df: pd.DataFrame) -> str | None:
    if frame.metric_hint:
        return frame.metric_hint
    if "spending_total" in df.columns:
        return "spending_total"
    return _first_numeric_column(df)


def _resolve_map_type(frame: QueryFrame, level: str | None, df: pd.DataFrame) -> str:
    if frame.family == "flow":
        if level in {"county", "congress"} and frame.primary_state:
            return "single-state-ranked-subregions"
        return "top-n-highlight"
    if level in {"county", "congress"} and frame.primary_state:
        return "single-state-ranked-subregions" if frame.intent in {"ranking", "share", "compare"} else "atlas-within-state"
    if level == "state" and len(frame.state_names) >= 2:
        return "atlas-comparison"
    if level == "state" and frame.primary_state and frame.intent == "lookup":
        return "single-state-spotlight"
    if frame.intent in {"ranking", "share"}:
        return "top-n-highlight"
    return "atlas-single-metric"


def _map_is_useful(frame: QueryFrame, dataset: str, level: str | None, metric: str | None, df: pd.DataFrame) -> bool:
    if dataset not in {"census", "gov_spending", "finra", "contract_static", "spending_breakdown", "contract_agency", "fund_flow"}:
        return False
    if level not in {"state", "county", "congress"}:
        return False
    if metric is None:
        return False
    if frame.intent not in {"ranking", "compare", "lookup", "share"}:
        return False
    if dataset == "fund_flow":
        if frame.wants_pair_ranking or frame.wants_internal_flow or frame.wants_displayed_flow:
            return False
        if len(df.index) < 2:
            return False
        return True
    if _has_multi_agency_non_geo_shape(df, level):
        return False
    if dataset == "contract_agency" and not _single_agency(df):
        return False
    return True


def _title_for_map_type(map_type: str, metric_label: str, level: str | None, state_label: str | None, top_n: int | None) -> str:
    if map_type == "single-state-spotlight" and state_label:
        return f"{state_label} · {metric_label}"
    if map_type in {"single-state-ranked-subregions", "atlas-within-state"} and state_label:
        return f"{state_label} · {metric_label} by {_level_label(level)}"
    if map_type == "atlas-comparison":
        return f"Comparison map · {metric_label}"
    if map_type == "top-n-highlight" and top_n:
        return f"Top {top_n} {_level_label(level)} · {metric_label}"
    if map_type == "agency-choropleth":
        return f"Agency geography · {metric_label}"
    return f"{metric_label} · Map"


def _default_view_for_map_type(map_type: str) -> str:
    if map_type == "atlas-comparison":
        return "comparison"
    if map_type == "single-state-spotlight":
        return "state-zoom"
    if map_type in {"single-state-ranked-subregions", "atlas-within-state"}:
        return "subdivision"
    return "heat"


def _button_label_for_map_type(map_type: str, level: str | None) -> str:
    if map_type == "atlas-comparison":
        return "Open comparison map"
    if map_type == "single-state-spotlight":
        return "Open state map"
    if map_type in {"single-state-ranked-subregions", "atlas-within-state"}:
        return "Open district map" if level == "congress" else "Open county map"
    if map_type == "top-n-highlight":
        return "Open heat map"
    return "Open map view"


def _reason_for_map_type(map_type: str, state_label: str | None) -> str:
    if map_type == "single-state-spotlight" and state_label:
        return f"This answer centers on {state_label}, so the map zooms into that state while keeping the national backdrop visible."
    if map_type in {"single-state-ranked-subregions", "atlas-within-state"} and state_label:
        return f"This answer is about subregions within {state_label}, so the map focuses on that state instead of the entire country."
    if map_type == "atlas-comparison":
        return "This answer compares a few places directly, so the map highlights those geographies against the broader distribution."
    if map_type == "top-n-highlight":
        return "This answer returns a ranked set of places, so the map emphasizes the leaders while keeping the full distribution visible."
    if map_type == "agency-choropleth":
        return "This answer is geographically meaningful for one agency-specific metric, so the map shows where that agency stands out."
    return "This answer has a geographic result, so opening the map gives quick spatial context."


def build_map_intent(question: str, df: pd.DataFrame, table_names: list[str] | None = None) -> dict[str, Any]:
    if df.empty or not _geo_columns_present(df):
        return dict(_NONE)

    table_name = table_names[0] if table_names else None
    dataset, level = _table_dataset_level(table_name)
    if dataset is None or level is None:
        return dict(_NONE)

    frame = infer_query_frame(question)
    metric = _resolve_metric(frame, df)
    if not metric or not _map_is_useful(frame, dataset, level, metric, df):
        return dict(_NONE)

    state_label = _state_title(frame.primary_state)
    map_type = _resolve_map_type(frame, level, df)
    year_label = frame.period_label or _default_year_for_table(table_name)
    metric_label = _humanize_metric(metric)
    top_n = min(len(df.index), 10) if frame.intent == "ranking" else None

    subtitle_bits = []
    if dataset == "census":
        subtitle_bits.append("Census")
    elif dataset == "gov_spending":
        subtitle_bits.append("Government Finances")
    elif dataset == "finra":
        subtitle_bits.append("FINRA")
    elif dataset == "contract_static":
        subtitle_bits.append("Federal Spending")
    elif dataset == "spending_breakdown":
        subtitle_bits.append("Federal Spending Breakdown")
    elif dataset == "contract_agency":
        subtitle_bits.append("Federal Spending by Agency")
    elif dataset == "fund_flow":
        subtitle_bits.append("Fund Flow")
    if year_label:
        subtitle_bits.append(str(year_label))

    return {
        "enabled": True,
        "mapType": map_type,
        "defaultView": _default_view_for_map_type(map_type),
        "buttonLabel": _button_label_for_map_type(map_type, level),
        "dataset": dataset,
        "level": level,
        "year": year_label,
        "metric": metric,
        "agency": _single_agency(df),
        "state": state_label,
        "focusIds": _focus_ids(df),
        "comparisonIds": _comparison_ids(frame, level),
        "comparisonLabels": _comparison_labels(frame, level),
        "topN": top_n,
        "title": _title_for_map_type(map_type, metric_label, level, state_label, top_n),
        "subtitle": " · ".join(subtitle_bits),
        "reason": _reason_for_map_type(map_type, state_label),
        "showLegend": True,
    }
