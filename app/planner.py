from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache

from app.db import execute_query
from app.metadata_utils import (
    agency_spending_total_expression,
    available_tables,
    column_match_score,
    count_columns,
    default_year_filter,
    default_spending_component_columns,
    default_spending_total_expression,
    monetary_columns,
    quote_identifier,
    table_columns,
    table_year_column,
)
from app.query_frame import QueryFrame, infer_query_frame
from app.sql_utils import ranking_top_k


@dataclass(frozen=True)
class QueryPlan:
    table_names: list[str]
    sql: str
    reason: str


_FEDERAL_TOKENS = {
    "federal spending",
    "federal money",
    "federal funding",
    "federal dollars",
    "spending",
    "money",
    "funding",
    "contracts",
    "contract",
    "grants",
    "grant",
    "direct payments",
    "resident wage",
    "employees wage",
    "agency",
    "agencies",
    "department",
}
_GOV_TOKENS = {
    "debt",
    "liabilities",
    "liability",
    "assets",
    "revenue",
    "expenses",
    "pension",
    "opeb",
    "current ratio",
    "debt ratio",
    "net position",
}
_ACS_TOKENS = {
    "poverty",
    "population",
    "income",
    "education",
    "hispanic",
    "black",
    "white",
    "asian",
    "household",
    "renter",
    "owner occupied",
}
_FINRA_TOKENS = {
    "financial literacy",
    "financial constraint",
    "alternative financing",
    "risk averse",
    "satisfied",
    "finra",
}
_FLOW_TOKENS = {"flow", "flows", "subaward", "subawardee", "awardee"}
_COMPLEX_TOKENS = {
    "correlation",
    "correlate",
    "relationship",
    "join",
    "versus",
    " vs ",
    "compared to",
    "across datasets",
}
_STOPWORDS = {
    "what",
    "which",
    "show",
    "list",
    "rank",
    "ranking",
    "top",
    "bottom",
    "highest",
    "lowest",
    "most",
    "least",
    "biggest",
    "smallest",
    "tell",
    "me",
    "the",
    "in",
    "for",
    "of",
    "and",
    "to",
    "by",
    "from",
    "with",
    "on",
    "a",
    "an",
    "is",
    "are",
    "did",
    "does",
    "have",
    "has",
}
_COMPONENT_COLUMNS = {
    "Contracts": {"contract", "contracts"},
    "Grants": {"grant", "grants"},
    "Resident Wage": {"resident wage", "wage"},
    "Direct Payments": {"direct payment", "direct payments", "payments"},
    "Employees Wage": {"employees wage", "employee wage", "payroll"},
    "Employees": {"employee", "employees", "employment", "jobs", "workforce"},
    "Federal Residents": {"federal residents", "residents"},
}


def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def _contains_any(question: str, phrases: set[str]) -> bool:
    q = question.lower()
    return any(phrase in q for phrase in phrases)


def _extract_state_names(question: str) -> list[str]:
    return list(infer_query_frame(question).state_names)


def _order_direction(question: str) -> str:
    q = question.lower()
    if any(token in q for token in ["lowest", "least", "smallest", "bottom"]):
        return "ASC"
    return "DESC"


def _leaderboard_rank_direction(question: str) -> str:
    q = question.lower()
    explicit_ascending = any(token in q for token in ["lowest", "least", "smallest"])
    explicit_descending = any(token in q for token in ["highest", "most", "largest", "top"])

    if explicit_ascending and not explicit_descending:
        return "ASC"
    return "DESC"


def _is_ranking(question: str) -> bool:
    q = question.lower()
    return any(token in q for token in ["top", "bottom", "highest", "lowest", "most", "least", "rank", "leading", "largest", "smallest"])


def _is_trend(question: str) -> bool:
    q = question.lower()
    return any(token in q for token in ["trend", "over time", "changed", "change in", "from 20", "between 20", "by year", "time series"])


def _is_compare(question: str, states: list[str]) -> bool:
    q = question.lower()
    return len(states) >= 2 or any(token in q for token in ["compare", "versus", " vs ", "against"])


def _is_position_lookup(question: str, frame: QueryFrame) -> bool:
    q = question.lower()
    if len(frame.state_names) != 1:
        return False
    return any(token in q for token in ["where does", "where is", "stand", "rank"])


def _wants_rank_leaderboards(question: str) -> bool:
    q = question.lower()
    return "top" in q and "bottom" in q


def _frame_table_name(frame: QueryFrame) -> str | None:
    geo = frame.geo_level
    family = frame.family
    loaded = available_tables()

    if family == "acs":
        return f"acs_{'county' if geo == 'county' else 'congress' if geo == 'congress' else 'state'}"
    if family == "gov":
        return f"gov_{'county' if geo == 'county' else 'congress' if geo == 'congress' else 'state'}"
    if family == "finra":
        return f"finra_{'county' if geo == 'county' else 'congress' if geo == 'congress' else 'state'}"
    if family == "contract":
        return f"contract_{'county' if geo == 'county' else 'congress' if geo == 'congress' else 'state'}"
    if family == "agency":
        if geo == "county":
            return "contract_county_agency"
        if geo == "congress":
            return "contract_cd_agency"
        return "spending_state_agency"
    if family == "breakdown":
        if geo in {"county", "congress"}:
            return None
        return "spending_state_agency" if frame.wants_agency_dimension or frame.wants_jobs_metric else "spending_state"
    if family == "flow":
        if frame.intent == "trend" and geo not in {"county", "congress"}:
            return "county_flow"
        return "county_flow" if geo == "county" else "congress_flow" if geo == "congress" else "state_flow"
    return None if family is None else next((name for name in (family,) if name in loaded), None)


def _data_not_available_plan(table_name: str | None, reason: str) -> QueryPlan:
    fallback_table = table_name if table_name and table_name in available_tables() else "spending_state_agency"
    return QueryPlan([fallback_table], "SELECT 'DATA_NOT_AVAILABLE' AS message", reason)


def _explicit_year_filter(table_name: str, frame: QueryFrame) -> str | None:
    if not frame.period_label:
        return None

    year_col = table_year_column(table_name)
    if not year_col:
        return None

    label = frame.period_label
    if year_col == "Year" and table_name.startswith(("acs_", "finra_")) and label.isdigit():
        return f"{quote_identifier(year_col)} = {label}"
    if table_name.startswith("gov_"):
        if label == "2023":
            label = "Fiscal Year 2023"
        return f"{quote_identifier(year_col)} = '{label}'"
    return f"{quote_identifier(year_col)} = '{label}'"


def _needs_default_spending_total(question: str, table_name: str) -> bool:
    q = question.lower()
    if table_name not in {"contract_state", "contract_county", "contract_congress", "spending_state", "spending_state_agency"}:
        return False
    broad_money_tokens = ["spending", "federal money", "federal funding", "federal dollars", "money", "funding", "total spending"]
    if not any(token in q for token in broad_money_tokens):
        return False
    return not any(any(alias in q for alias in aliases) for aliases in _COMPONENT_COLUMNS.values())


def _selected_geography_table(prefix: str, geo: str | None) -> str:
    suffix = "state"
    if geo == "county":
        suffix = "county"
    elif geo == "congress":
        suffix = "congress"
    return f"{prefix}_{suffix}"


@lru_cache(maxsize=16)
def _distinct_values(table_name: str, column_name: str) -> tuple[str, ...]:
    if table_name not in available_tables() or column_name not in table_columns(table_name):
        return ()
    df = execute_query(
        f"SELECT DISTINCT {quote_identifier(column_name)} AS value "
        f"FROM {table_name} WHERE {quote_identifier(column_name)} IS NOT NULL ORDER BY 1"
    )
    return tuple(str(v) for v in df["value"].tolist())


def _match_agency(question: str, table_name: str = "spending_state_agency", column_name: str = "agency") -> str | None:
    q = question.lower()
    agencies = _distinct_values(table_name, column_name)
    for agency in sorted(agencies, key=len, reverse=True):
        if agency.lower() in q:
            return agency

    q_tokens = set(_normalize(question).split()) - _STOPWORDS
    best_match = None
    best_score = 0
    for agency in agencies:
        tokens = set(_normalize(agency).split()) - {"department", "of", "administration", "national"}
        score = len(q_tokens & tokens)
        if score > best_score:
            best_match = agency
            best_score = score
    return best_match if best_score >= 1 else None


def _metric_alias(column_name: str) -> str:
    return _normalize(column_name).replace(" ", "_")


def _metric_expr_for_component(table_name: str, column_name: str, question: str, frame: QueryFrame | None = None) -> tuple[str, str] | None:
    cols = set(table_columns(table_name))
    if column_name not in cols:
        return None

    q = question.lower()
    per_capita = frame.wants_relative if frame else ("per capita" in q or "per 1000" in q)
    per_1000_column = f"{column_name} Per 1000"
    target_column = per_1000_column if per_capita and per_1000_column in cols else column_name
    return quote_identifier(target_column), _metric_alias(target_column)


def _resolve_metric(table_name: str, question: str, frame: QueryFrame | None = None) -> tuple[str, str] | None:
    if (frame and frame.metric_hint == "spending_total") or _needs_default_spending_total(question, table_name):
        expr = (
            agency_spending_total_expression(table_name, alias="spending_total")
            if table_name == "spending_state_agency"
            else default_spending_total_expression(table_name, alias="spending_total")
        )
        alias = "spending_total"
        if expr:
            raw_expr = expr.rsplit(" AS ", 1)[0]
            if raw_expr.startswith("(") and raw_expr.endswith(")"):
                raw_expr = raw_expr[1:-1]
            return raw_expr, alias

    if frame and frame.metric_hint:
        metric = _metric_expr_for_component(table_name, frame.metric_hint, question, frame=frame)
        if metric:
            return metric

    q = question.lower()
    for column_name, aliases in _COMPONENT_COLUMNS.items():
        if any(alias in q for alias in aliases):
            metric = _metric_expr_for_component(table_name, column_name, question, frame=frame)
            if metric:
                return metric

    best_column = None
    best_score = 0.0
    for column_name in table_columns(table_name):
        if column_name in {"state", "county", "fips", "state_fips", "county_fips", "cd_118", "agency", "year", "Year", "act_dt_fis_yr"}:
            continue
        score = column_match_score(table_name, column_name, question)
        if "per capita" not in q and "per 1000" not in q and column_name.endswith("Per 1000"):
            score -= 2.0
        if best_column is None or score > best_score:
            best_column = column_name
            best_score = score

    if best_column and best_score > 0:
        return quote_identifier(best_column), _metric_alias(best_column)
    return None


def _render_metric(metric_expr: str, metric_alias: str, table_name: str) -> str:
    plain_name = metric_alias.replace("_", " ")
    if metric_alias in {"employees", "federal_residents"} or plain_name in {"employees", "federal residents"}:
        return f"{metric_expr} AS {metric_alias}"
    if metric_expr in {quote_identifier(col) for col in count_columns(table_name)}:
        return f"{metric_expr} AS {metric_alias}"
    return f"ROUND({metric_expr}, 2) AS {metric_alias}"


def _trend_aggregate(metric_alias: str, metric_expr: str) -> str:
    avg_like_tokens = {"ratio", "literacy", "constraint", "satisfied", "averse", "poverty", "income", "per", "median"}
    if any(token in metric_alias for token in avg_like_tokens):
        return f"AVG({metric_expr})"
    return f"SUM({metric_expr})"


def _state_filter_clause(table_name: str, state_names: list[str], question: str, frame: QueryFrame | None = None) -> list[str]:
    if not state_names:
        if frame and frame.geo_level == "congress" and frame.state_postal and "cd_118" in set(table_columns(table_name)):
            return [f"UPPER(cd_118) LIKE '{frame.state_postal}-%'"]
        return []

    table_cols = set(table_columns(table_name))
    lower_states = [state.lower() for state in state_names]
    quoted_states = ", ".join(f"'{state}'" for state in lower_states)

    if "cd_118" in table_cols and frame and frame.geo_level == "congress" and frame.state_postal:
        return [f"UPPER(cd_118) LIKE '{frame.state_postal}-%'"]

    if "state" in table_cols:
        if len(lower_states) == 1:
            return [f"LOWER(state) = '{lower_states[0]}'"]
        return [f"LOWER(state) IN ({quoted_states})"]

    if table_name == "state_flow":
        q = question.lower()
        clauses: list[str] = []
        if frame and len(lower_states) >= 2 and frame.flow_direction == "inflow":
            clauses.append(f"LOWER(subawardee_state_name) = '{lower_states[0]}'")
            clauses.append(f"LOWER(rcpt_state_name) = '{lower_states[1]}'")
            return clauses
        if frame and len(lower_states) >= 2 and frame.flow_direction == "outflow":
            clauses.append(f"LOWER(rcpt_state_name) = '{lower_states[0]}'")
            clauses.append(f"LOWER(subawardee_state_name) = '{lower_states[1]}'")
            return clauses
        if len(lower_states) >= 2:
            clauses.append(f"LOWER(rcpt_state_name) = '{lower_states[0]}'")
            clauses.append(f"LOWER(subawardee_state_name) = '{lower_states[1]}'")
            return clauses
        if frame and frame.flow_direction == "inflow":
            return [f"LOWER(subawardee_state_name) = '{lower_states[0]}'"]
        if frame and frame.flow_direction == "outflow":
            return [f"LOWER(rcpt_state_name) = '{lower_states[0]}'"]
        if " to " in q or "into " in q:
            return [f"LOWER(subawardee_state_name) = '{lower_states[0]}'"]
        return [f"LOWER(rcpt_state_name) = '{lower_states[0]}'"]

    if table_name in {"county_flow", "congress_flow"}:
        q = question.lower()
        if frame and len(lower_states) >= 2 and frame.flow_direction == "inflow":
            return [f"LOWER(subawardee_state) = '{lower_states[0]}'", f"LOWER(rcpt_state) = '{lower_states[1]}'"]
        if frame and len(lower_states) >= 2 and frame.flow_direction == "outflow":
            return [f"LOWER(rcpt_state) = '{lower_states[0]}'", f"LOWER(subawardee_state) = '{lower_states[1]}'"]
        if frame and frame.flow_direction == "inflow":
            return [f"LOWER(subawardee_state) = '{lower_states[0]}'"]
        if frame and frame.flow_direction == "outflow":
            return [f"LOWER(rcpt_state) = '{lower_states[0]}'"]
        if " to " in q or "into " in q:
            return [f"LOWER(subawardee_state) = '{lower_states[0]}'"]
        return [f"LOWER(rcpt_state) = '{lower_states[0]}'"]

    return []


def _default_filters(
    table_name: str,
    question: str,
    *,
    include_default_year: bool = True,
    include_state: bool = True,
    frame: QueryFrame | None = None,
) -> list[str]:
    filters: list[str] = []
    states = list(frame.state_names) if frame else _extract_state_names(question)
    if include_state:
        filters.extend(_state_filter_clause(table_name, states, question, frame=frame))

    if table_name == "contract_county":
        filters.append("county_fips > 1000")

    explicit_year = _explicit_year_filter(table_name, frame) if frame else None
    if explicit_year:
        filters.append(explicit_year)
    elif include_default_year:
        year_filter = default_year_filter(table_name)
        if year_filter:
            filters.append(year_filter)
    return filters


def _family_hits(question: str) -> set[str]:
    hits: set[str] = set()
    if _contains_any(question, _FLOW_TOKENS):
        hits.add("flow")
    if _contains_any(question, _FINRA_TOKENS):
        hits.add("finra")
    if _contains_any(question, _GOV_TOKENS):
        hits.add("gov")
    if _contains_any(question, _ACS_TOKENS):
        hits.add("acs")
    if _contains_any(question, _FEDERAL_TOKENS):
        hits.add("federal")
    return hits


def _choose_table(question: str, frame: QueryFrame | None = None) -> str | None:
    active_frame = frame or infer_query_frame(question)
    table_name = _frame_table_name(active_frame)
    if not table_name:
        return None
    return table_name if table_name in available_tables() else table_name


def _build_where_clause(filters: list[str]) -> str:
    return f" WHERE {' AND '.join(filters)}" if filters else ""


def _plan_standard_table(question: str, table_name: str, frame: QueryFrame) -> QueryPlan | None:
    if table_name not in available_tables():
        return None

    metric = _resolve_metric(table_name, question, frame=frame)
    if not metric:
        return None
    metric_expr, metric_alias = metric

    trend = frame.intent == "trend"
    compare = frame.intent == "compare"
    ranking = frame.intent == "ranking"
    position_lookup = _is_position_lookup(question, frame)
    direction = _order_direction(question)
    filters = _default_filters(table_name, question, include_default_year=not trend, frame=frame)
    where_clause = _build_where_clause(filters)

    if trend:
        year_col = table_year_column(table_name)
        if not year_col:
            return None
        aggregate_nationally = not frame.state_names and "agency" not in table_columns(table_name)
        if aggregate_nationally:
            agg_expr = _trend_aggregate(metric_alias, metric_expr)
            sql = (
                f"SELECT {quote_identifier(year_col)} AS period, ROUND({agg_expr}, 2) AS {metric_alias} "
                f"FROM {table_name}{where_clause} GROUP BY {quote_identifier(year_col)} ORDER BY {quote_identifier(year_col)}"
            )
        else:
            sql = (
                f"SELECT {quote_identifier(year_col)} AS period, {_render_metric(metric_expr, metric_alias, table_name)} "
                f"FROM {table_name}{where_clause} ORDER BY {quote_identifier(year_col)}"
            )
        return QueryPlan([table_name], sql, "deterministic_trend")

    label_columns: list[str] = []
    table_cols = set(table_columns(table_name))
    if "county" in table_cols:
        label_columns.extend(["county", "state"])
    elif "cd_118" in table_cols:
        label_columns.append("cd_118")
        if "state" in table_cols and compare:
            label_columns.append("state")
    elif "state" in table_cols:
        label_columns.append("state")

    if not label_columns:
        return None

    if position_lookup and "state" in table_cols:
        ranking_filters = _default_filters(
            table_name,
            question,
            include_default_year=True,
            include_state=False,
            frame=frame,
        )
        ranking_where = _build_where_clause(ranking_filters)
        state_value = frame.state_names[0].lower()
        if _wants_rank_leaderboards(question):
            k = ranking_top_k(question)
            leaderboard_direction = _leaderboard_rank_direction(question)
            sql = (
                "WITH ranked AS ("
                f"SELECT state, {_render_metric(metric_expr, metric_alias, table_name)}, "
                f"RANK() OVER (ORDER BY {metric_expr} {leaderboard_direction}) AS metric_rank, "
                "COUNT(*) OVER () AS total_states, "
                f"ROUND(AVG({metric_expr}) OVER (), 2) AS national_average "
                f"FROM {table_name}{ranking_where}"
                "), focus AS ("
                f"SELECT 'focus' AS row_kind, state, {metric_alias}, metric_rank, total_states, national_average, 0 AS list_position "
                f"FROM ranked WHERE LOWER(state) = '{state_value}'"
                "), nearby_rows AS ("
                f"SELECT 'nearby' AS row_kind, ranked.state, ranked.{metric_alias}, ranked.metric_rank, ranked.total_states, ranked.national_average, "
                "ROW_NUMBER() OVER (ORDER BY ranked.metric_rank ASC) AS list_position "
                "FROM ranked CROSS JOIN focus "
                "WHERE ranked.metric_rank BETWEEN focus.metric_rank - 2 AND focus.metric_rank + 2 "
                f"AND LOWER(ranked.state) <> '{state_value}'"
                "), top_rows AS ("
                f"SELECT 'top' AS row_kind, state, {metric_alias}, metric_rank, total_states, national_average, metric_rank AS list_position "
                f"FROM ranked WHERE metric_rank <= {k} AND LOWER(state) <> '{state_value}'"
                "), bottom_rows AS ("
                f"SELECT 'bottom' AS row_kind, state, {metric_alias}, metric_rank, total_states, national_average, "
                f"(total_states - metric_rank + 1) AS list_position "
                f"FROM ranked WHERE metric_rank > total_states - {k} AND LOWER(state) <> '{state_value}'"
                ") "
                "SELECT * FROM ("
                "SELECT * FROM focus "
                "UNION ALL SELECT * FROM nearby_rows "
                "UNION ALL SELECT * FROM top_rows "
                "UNION ALL SELECT * FROM bottom_rows"
                ") AS combined "
                "ORDER BY CASE row_kind WHEN 'focus' THEN 0 WHEN 'nearby' THEN 1 WHEN 'top' THEN 2 ELSE 3 END, list_position ASC"
            )
            return QueryPlan([table_name], sql, "deterministic_position_lookup_with_leaderboards")

        sql = (
            "WITH ranked AS ("
            f"SELECT state, {_render_metric(metric_expr, metric_alias, table_name)}, "
            f"RANK() OVER (ORDER BY {metric_expr} {direction}) AS metric_rank, "
            "COUNT(*) OVER () AS total_states, "
            f"ROUND(AVG({metric_expr}) OVER (), 2) AS national_average "
            f"FROM {table_name}{ranking_where}"
            ") "
            f"SELECT state, {metric_alias}, metric_rank, total_states, national_average "
            f"FROM ranked WHERE LOWER(state) = '{state_value}'"
        )
        return QueryPlan([table_name], sql, "deterministic_position_lookup")

    select_parts = list(dict.fromkeys(label_columns))
    if metric_alias == "spending_total":
        component_cols = default_spending_component_columns(table_name)
        select_parts.extend(
            f"ROUND({quote_identifier(col)}, 2) AS {_metric_alias(col)}"
            for col in component_cols
        )
    select_cols = ", ".join(select_parts)
    order_by = metric_alias
    sql = f"SELECT {select_cols}, {_render_metric(metric_expr, metric_alias, table_name)} FROM {table_name}{where_clause}"

    if compare or ranking or len(frame.state_names) == 0:
        sql += f" ORDER BY {order_by} {direction}"
        if ranking:
            sql += f" LIMIT {ranking_top_k(question)}"

    return QueryPlan([table_name], sql, "deterministic_single_table")


def _plan_spending_state_agency(question: str, frame: QueryFrame, table_name: str | None = None) -> QueryPlan | None:
    selected_table = table_name or _frame_table_name(frame) or "spending_state_agency"
    if selected_table not in available_tables():
        return _data_not_available_plan("spending_state_agency", "missing_runtime_agency_geo_table")

    state_names = list(frame.state_names)
    matched_agency = _match_agency(question, table_name=selected_table)
    metric = _resolve_metric(selected_table, question, frame=frame)
    if not metric:
        return None
    metric_expr, metric_alias = metric

    base_filters = _default_filters(selected_table, question, frame=frame)
    direction = _order_direction(question)
    if frame.intent == "share" and state_names:
        top_k = 1 if "top agency" in frame.normalized_question else ranking_top_k(question)
        share_sql = (
            "WITH base AS ("
            f"SELECT agency, ROUND({metric_expr}, 2) AS {metric_alias} "
            f"FROM {selected_table}{_build_where_clause(base_filters)}"
            "), ranked AS ("
            f"SELECT agency, {metric_alias}, ROW_NUMBER() OVER (ORDER BY {metric_alias} DESC) AS rnk, "
            f"SUM({metric_alias}) OVER () AS total_metric "
            "FROM base"
            ") "
            "SELECT "
            f"MAX(CASE WHEN rnk = 1 THEN agency END) AS top_agency, "
            f"ROUND(SUM(CASE WHEN rnk <= {top_k} THEN {metric_alias} ELSE 0 END), 2) AS top_k_metric, "
            "ROUND(MAX(total_metric), 2) AS total_metric, "
            f"ROUND(100.0 * SUM(CASE WHEN rnk <= {top_k} THEN {metric_alias} ELSE 0 END) / NULLIF(MAX(total_metric), 0), 2) AS share_pct "
            "FROM ranked"
        )
        return QueryPlan([selected_table], share_sql, "deterministic_agency_share")

    if metric_alias == "spending_total":
        money_cols = default_spending_component_columns(selected_table)
    else:
        money_cols = [col for col in monetary_columns(selected_table) if col in table_columns(selected_table)]
    rendered_money_cols = ", ".join(f"ROUND({quote_identifier(col)}, 2) AS {_metric_alias(col)}" for col in money_cols)

    if state_names:
        filters = list(base_filters)
        if matched_agency:
            escaped_agency = matched_agency.replace("'", "''")
            filters.append(f"agency = '{escaped_agency}'")
            where_clause = _build_where_clause(filters)
            sql = (
                f"SELECT agency, state, year, {rendered_money_cols}, {_render_metric(metric_expr, metric_alias, selected_table)} "
                f"FROM {selected_table}{where_clause}"
            )
            return QueryPlan([selected_table], sql, "deterministic_agency_detail")

        where_clause = _build_where_clause(filters)
        select_parts = ["agency"]
        if metric_alias == "spending_total" and rendered_money_cols:
            select_parts.append(rendered_money_cols)
        select_parts.append(_render_metric(metric_expr, metric_alias, selected_table))
        sql = (
            f"SELECT {', '.join(select_parts)} FROM {selected_table}{where_clause} "
            f"ORDER BY {metric_alias} {direction} LIMIT {ranking_top_k(question)}"
        )
        return QueryPlan([selected_table], sql, "deterministic_agency_breakdown")

    group_metric = f"SUM({metric_expr})"
    sql = (
        f"SELECT agency, ROUND({group_metric}, 2) AS {metric_alias} "
        f"FROM {selected_table}{_build_where_clause(base_filters)} "
        f"GROUP BY agency ORDER BY {metric_alias} {direction} LIMIT {ranking_top_k(question)}"
    )
    return QueryPlan([selected_table], sql, "deterministic_agency_aggregate")


def _plan_breakdown(question: str, frame: QueryFrame) -> QueryPlan | None:
    if frame.geo_level in {"county", "congress"}:
        return _data_not_available_plan("spending_state", "breakdown_state_only")

    if frame.wants_agency_dimension or frame.wants_jobs_metric:
        return _plan_spending_state_agency(question, frame, table_name="spending_state_agency")

    table_name = "spending_state"
    if table_name not in available_tables():
        return None
    return _plan_standard_table(question, table_name, frame)


def _plan_flow(question: str, table_name: str, frame: QueryFrame) -> QueryPlan | None:
    if table_name not in available_tables():
        return None

    direction = _order_direction(question)

    if table_name in {"county_flow", "congress_flow"} and frame.intent == "trend":
        amount_col = "subaward_amount"
        filters = _default_filters(table_name, question, include_default_year=False, frame=frame)
        sql = (
            f"SELECT act_dt_fis_yr AS fiscal_year, ROUND(SUM({amount_col}), 2) AS total_flow "
            f"FROM {table_name}{_build_where_clause(filters)} "
            "GROUP BY act_dt_fis_yr ORDER BY fiscal_year"
        )
        return QueryPlan([table_name], sql, "deterministic_flow_trend")

    if table_name == "state_flow":
        amount_col = "subaward_amount_year"
        filters = _default_filters(table_name, question, include_default_year=False, frame=frame)
        q = question.lower()
        states = list(frame.state_names)
        if frame.wants_internal_flow and frame.primary_state:
            sql = (
                f"SELECT ROUND(SUM({amount_col}), 2) AS total_flow "
                f"FROM {table_name}{_build_where_clause(filters + ['LOWER(rcpt_state_name) = LOWER(subawardee_state_name)'])}"
            )
            return QueryPlan([table_name], sql, "deterministic_flow_internal")

        if frame.wants_agency_dimension:
            sql = (
                f"SELECT agency_name, ROUND(SUM({amount_col}), 2) AS total_flow "
                f"FROM {table_name}{_build_where_clause(filters)} "
                "GROUP BY agency_name ORDER BY total_flow "
                f"{direction} LIMIT {ranking_top_k(question)}"
            )
            return QueryPlan([table_name], sql, "deterministic_flow_agency_breakdown")

        if frame.wants_industry_dimension:
            sql = (
                f"SELECT naics_2digit_title, ROUND(SUM({amount_col}), 2) AS total_flow "
                f"FROM {table_name}{_build_where_clause(filters + ['naics_2digit_title IS NOT NULL'])} "
                "GROUP BY naics_2digit_title ORDER BY total_flow "
                f"{direction} LIMIT {ranking_top_k(question)}"
            )
            return QueryPlan([table_name], sql, "deterministic_flow_industry_breakdown")

        if states:
            if frame.wants_pair_ranking or frame.wants_displayed_flow:
                pair_filters = list(filters)
                if frame.wants_displayed_flow:
                    pair_filters.append("LOWER(rcpt_state_name) <> LOWER(subawardee_state_name)")
                sql = (
                    f"SELECT rcpt_state_name, subawardee_state_name, ROUND(SUM({amount_col}), 2) AS total_flow "
                    f"FROM {table_name}{_build_where_clause(pair_filters)} "
                    "GROUP BY rcpt_state_name, subawardee_state_name "
                    f"ORDER BY total_flow {direction} LIMIT {ranking_top_k(question)}"
                )
                return QueryPlan([table_name], sql, "deterministic_flow_state_pairs_scoped")

            if frame.flow_direction == "inflow" or " to " in q or "into " in q:
                sql = (
                    f"SELECT rcpt_state_name, ROUND(SUM({amount_col}), 2) AS total_flow "
                    f"FROM {table_name}{_build_where_clause(filters)} "
                    "GROUP BY rcpt_state_name ORDER BY total_flow "
                    f"{direction} LIMIT {ranking_top_k(question)}"
                )
            else:
                sql = (
                    f"SELECT subawardee_state_name, ROUND(SUM({amount_col}), 2) AS total_flow "
                    f"FROM {table_name}{_build_where_clause(filters)} "
                    "GROUP BY subawardee_state_name ORDER BY total_flow "
                    f"{direction} LIMIT {ranking_top_k(question)}"
                )
            return QueryPlan([table_name], sql, "deterministic_flow_state_breakdown")

        sql = (
            f"SELECT rcpt_state_name, subawardee_state_name, ROUND(SUM({amount_col}), 2) AS total_flow "
            f"FROM {table_name}{_build_where_clause(['LOWER(rcpt_state_name) <> LOWER(subawardee_state_name)'] if frame.wants_displayed_flow else [])} "
            "GROUP BY rcpt_state_name, subawardee_state_name "
            f"ORDER BY total_flow {direction} LIMIT {ranking_top_k(question)}"
        )
        return QueryPlan([table_name], sql, "deterministic_flow_pairs")

    amount_col = "subaward_amount"
    filters = _default_filters(table_name, question, include_default_year=frame.period_label is None, frame=frame)
    pair_left = "rcpt_cty_name" if table_name == "county_flow" else "rcpt_cd_name"
    pair_right = "subawardee_cty_name" if table_name == "county_flow" else "subawardee_cd_name"

    if frame.wants_agency_dimension:
        sql = (
            f"SELECT agency_name, ROUND(SUM({amount_col}), 2) AS total_flow "
            f"FROM {table_name}{_build_where_clause(filters)} "
            "GROUP BY agency_name ORDER BY total_flow "
            f"{direction} LIMIT {ranking_top_k(question)}"
        )
        return QueryPlan([table_name], sql, "deterministic_flow_nonstate_agency")

    if frame.wants_industry_dimension and table_name == "congress_flow":
        sql = (
            f"SELECT naics_2digit_title, ROUND(SUM({amount_col}), 2) AS total_flow "
            f"FROM {table_name}{_build_where_clause(filters + ['naics_2digit_title IS NOT NULL'])} "
            "GROUP BY naics_2digit_title ORDER BY total_flow "
            f"{direction} LIMIT {ranking_top_k(question)}"
        )
        return QueryPlan([table_name], sql, "deterministic_flow_nonstate_industry")

    if frame.wants_pair_ranking or not frame.state_names:
        pair_filters = list(filters)
        if frame.wants_displayed_flow:
            pair_filters.append(f"LOWER({pair_left}) <> LOWER({pair_right})")
        sql = (
            f"SELECT {pair_left}, {pair_right}, ROUND(SUM({amount_col}), 2) AS total_flow "
            f"FROM {table_name}{_build_where_clause(pair_filters)} "
            f"GROUP BY {pair_left}, {pair_right} ORDER BY total_flow {direction} LIMIT {ranking_top_k(question)}"
        )
        return QueryPlan([table_name], sql, "deterministic_flow_nonstate_pairs")

    label_col = pair_right if frame.flow_direction == "inflow" else pair_left
    sql = (
        f"SELECT {label_col}, ROUND(SUM({amount_col}), 2) AS total_flow "
        f"FROM {table_name}{_build_where_clause(filters)} "
        f"GROUP BY {label_col} ORDER BY total_flow {direction} LIMIT {ranking_top_k(question)}"
    )
    return QueryPlan([table_name], sql, "deterministic_flow_nonstate_breakdown")


def plan_query(question: str) -> QueryPlan | None:
    q = question.lower()
    if any(token in q for token in _COMPLEX_TOKENS):
        return None

    frame = infer_query_frame(question)
    if frame.family is None:
        return None

    table_name = _choose_table(question, frame=frame)
    if not table_name:
        return None

    if frame.family == "breakdown":
        return _plan_breakdown(question, frame)
    if frame.family == "agency" or table_name in {"spending_state_agency", "contract_county_agency", "contract_cd_agency"}:
        return _plan_spending_state_agency(question, frame, table_name=table_name)
    if frame.family == "flow" or table_name in {"state_flow", "county_flow", "congress_flow"}:
        return _plan_flow(question, table_name, frame)
    if table_name not in available_tables():
        return None
    return _plan_standard_table(question, table_name, frame)
