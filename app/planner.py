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
from app.router import US_STATE_NAMES, detect_geo_level
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
    "Employees": {"employee", "employees", "employment"},
    "Federal Residents": {"federal residents", "residents"},
}


def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def _contains_any(question: str, phrases: set[str]) -> bool:
    q = question.lower()
    return any(phrase in q for phrase in phrases)


def _extract_state_names(question: str) -> list[str]:
    q = question.lower()
    found: list[str] = []
    for state in sorted(US_STATE_NAMES, key=len, reverse=True):
        if re.search(rf"\b{re.escape(state)}\b", q):
            found.append(state)
    return found


def _order_direction(question: str) -> str:
    q = question.lower()
    if any(token in q for token in ["lowest", "least", "smallest", "bottom"]):
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


def _metric_expr_for_component(table_name: str, column_name: str, question: str) -> tuple[str, str] | None:
    cols = set(table_columns(table_name))
    if column_name not in cols:
        return None

    q = question.lower()
    per_capita = "per capita" in q or "per 1000" in q
    per_1000_column = f"{column_name} Per 1000"
    target_column = per_1000_column if per_capita and per_1000_column in cols else column_name
    return quote_identifier(target_column), _metric_alias(target_column)


def _resolve_metric(table_name: str, question: str) -> tuple[str, str] | None:
    if _needs_default_spending_total(question, table_name):
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

    q = question.lower()
    for column_name, aliases in _COMPONENT_COLUMNS.items():
        if any(alias in q for alias in aliases):
            metric = _metric_expr_for_component(table_name, column_name, question)
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


def _state_filter_clause(table_name: str, state_names: list[str], question: str) -> list[str]:
    if not state_names:
        return []

    table_cols = set(table_columns(table_name))
    lower_states = [state.lower() for state in state_names]
    quoted_states = ", ".join(f"'{state}'" for state in lower_states)

    if "state" in table_cols:
        if len(lower_states) == 1:
            return [f"LOWER(state) = '{lower_states[0]}'"]
        return [f"LOWER(state) IN ({quoted_states})"]

    if table_name == "state_flow":
        q = question.lower()
        clauses: list[str] = []
        if len(lower_states) >= 2:
            clauses.append(f"LOWER(rcpt_state_name) = '{lower_states[0]}'")
            clauses.append(f"LOWER(subawardee_state_name) = '{lower_states[1]}'")
            return clauses
        if " to " in q or "into " in q:
            return [f"LOWER(subawardee_state_name) = '{lower_states[0]}'"]
        return [f"LOWER(rcpt_state_name) = '{lower_states[0]}'"]

    if table_name in {"county_flow", "congress_flow"}:
        q = question.lower()
        if " to " in q or "into " in q:
            return [f"LOWER(subawardee_state) = '{lower_states[0]}'"]
        return [f"LOWER(rcpt_state) = '{lower_states[0]}'"]

    return []


def _default_filters(table_name: str, question: str, *, include_default_year: bool = True) -> list[str]:
    filters: list[str] = []
    states = _extract_state_names(question)
    filters.extend(_state_filter_clause(table_name, states, question))

    if table_name == "contract_county":
        filters.append("county_fips > 1000")

    if include_default_year:
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


def _choose_table(question: str) -> str | None:
    q = question.lower()
    geo = detect_geo_level(question)

    if _contains_any(question, _FLOW_TOKENS):
        if _is_trend(question):
            table_name = "county_flow" if geo != "congress" else "congress_flow"
        else:
            if geo == "county":
                table_name = "county_flow"
            elif geo == "congress":
                table_name = "congress_flow"
            else:
                table_name = "state_flow"
        return table_name if table_name in available_tables() else None

    if any(token in q for token in ["agency", "agencies", "department"]) and _contains_any(question, _FEDERAL_TOKENS):
        return "spending_state_agency" if "spending_state_agency" in available_tables() else None

    if _contains_any(question, _GOV_TOKENS):
        table_name = _selected_geography_table("gov", geo)
        return table_name if table_name in available_tables() else None

    if _contains_any(question, _FINRA_TOKENS):
        table_name = _selected_geography_table("finra", geo)
        return table_name if table_name in available_tables() else None

    if _contains_any(question, _ACS_TOKENS):
        table_name = _selected_geography_table("acs", geo)
        return table_name if table_name in available_tables() else None

    if _contains_any(question, _FEDERAL_TOKENS):
        table_name = _selected_geography_table("contract", geo)
        return table_name if table_name in available_tables() else None

    return None


def _build_where_clause(filters: list[str]) -> str:
    return f" WHERE {' AND '.join(filters)}" if filters else ""


def _plan_standard_table(question: str, table_name: str) -> QueryPlan | None:
    metric = _resolve_metric(table_name, question)
    if not metric:
        return None
    metric_expr, metric_alias = metric

    trend = _is_trend(question)
    compare = _is_compare(question, _extract_state_names(question))
    ranking = _is_ranking(question)
    direction = _order_direction(question)
    filters = _default_filters(table_name, question, include_default_year=not trend)
    where_clause = _build_where_clause(filters)

    if trend:
        year_col = table_year_column(table_name)
        if not year_col:
            return None
        aggregate_nationally = not _extract_state_names(question) and "agency" not in table_columns(table_name)
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

    if compare or ranking or len(_extract_state_names(question)) == 0:
        sql += f" ORDER BY {order_by} {direction}"
        if ranking:
            sql += f" LIMIT {ranking_top_k(question)}"

    return QueryPlan([table_name], sql, "deterministic_single_table")


def _plan_spending_state_agency(question: str) -> QueryPlan | None:
    table_name = "spending_state_agency"
    if table_name not in available_tables():
        return None

    state_names = _extract_state_names(question)
    matched_agency = _match_agency(question)
    metric = _resolve_metric(table_name, question)
    if not metric:
        return None
    metric_expr, metric_alias = metric

    base_filters = _default_filters(table_name, question)
    direction = _order_direction(question)
    if metric_alias == "spending_total":
        money_cols = default_spending_component_columns(table_name)
    else:
        money_cols = [col for col in monetary_columns(table_name) if col in table_columns(table_name)]
    rendered_money_cols = ", ".join(f"ROUND({quote_identifier(col)}, 2) AS {_metric_alias(col)}" for col in money_cols)

    if state_names:
        filters = list(base_filters)
        if matched_agency:
            escaped_agency = matched_agency.replace("'", "''")
            filters.append(f"agency = '{escaped_agency}'")
            where_clause = _build_where_clause(filters)
            sql = (
                f"SELECT agency, state, year, {rendered_money_cols}, {_render_metric(metric_expr, metric_alias, table_name)} "
                f"FROM {table_name}{where_clause}"
            )
            return QueryPlan([table_name], sql, "deterministic_agency_detail")

        where_clause = _build_where_clause(filters)
        select_parts = ["agency"]
        if metric_alias == "spending_total" and rendered_money_cols:
            select_parts.append(rendered_money_cols)
        select_parts.append(_render_metric(metric_expr, metric_alias, table_name))
        sql = (
            f"SELECT {', '.join(select_parts)} FROM {table_name}{where_clause} "
            f"ORDER BY {metric_alias} {direction} LIMIT {ranking_top_k(question)}"
        )
        return QueryPlan([table_name], sql, "deterministic_agency_breakdown")

    group_metric = f"SUM({metric_expr})"
    sql = (
        f"SELECT agency, ROUND({group_metric}, 2) AS {metric_alias} "
        f"FROM {table_name}{_build_where_clause(base_filters)} "
        f"GROUP BY agency ORDER BY {metric_alias} {direction} LIMIT {ranking_top_k(question)}"
    )
    return QueryPlan([table_name], sql, "deterministic_agency_aggregate")


def _plan_flow(question: str, table_name: str) -> QueryPlan | None:
    direction = _order_direction(question)

    if table_name in {"county_flow", "congress_flow"} and _is_trend(question):
        amount_col = "subaward_amount"
        filters = _default_filters(table_name, question, include_default_year=False)
        sql = (
            f"SELECT act_dt_fis_yr AS fiscal_year, ROUND(SUM({amount_col}), 2) AS total_flow "
            f"FROM {table_name}{_build_where_clause(filters)} "
            "GROUP BY act_dt_fis_yr ORDER BY fiscal_year"
        )
        return QueryPlan([table_name], sql, "deterministic_flow_trend")

    if table_name == "state_flow":
        amount_col = "subaward_amount_year"
        filters = _default_filters(table_name, question, include_default_year=False)
        q = question.lower()
        states = _extract_state_names(question)
        if "agency" in q or "department" in q:
            sql = (
                f"SELECT agency_name, ROUND(SUM({amount_col}), 2) AS total_flow "
                f"FROM {table_name}{_build_where_clause(filters)} "
                "GROUP BY agency_name ORDER BY total_flow "
                f"{direction} LIMIT {ranking_top_k(question)}"
            )
            return QueryPlan([table_name], sql, "deterministic_flow_agency_breakdown")

        if states:
            if " to " in q or "into " in q:
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
            f"FROM {table_name} GROUP BY rcpt_state_name, subawardee_state_name "
            f"ORDER BY total_flow {direction} LIMIT {ranking_top_k(question)}"
        )
        return QueryPlan([table_name], sql, "deterministic_flow_pairs")

    return None


def plan_query(question: str) -> QueryPlan | None:
    q = question.lower()
    if any(token in q for token in _COMPLEX_TOKENS):
        return None

    hits = _family_hits(question)
    if len(hits - {"federal"}) > 1:
        return None

    table_name = _choose_table(question)
    if not table_name:
        return None

    if table_name == "spending_state_agency":
        return _plan_spending_state_agency(question)
    if table_name in {"state_flow", "county_flow", "congress_flow"}:
        return _plan_flow(question, table_name)
    return _plan_standard_table(question, table_name)
