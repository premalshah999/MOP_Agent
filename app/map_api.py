from __future__ import annotations

from typing import Any

from app.db import execute_query
from app.metadata_utils import (
    agency_spending_total_expression,
    default_spending_total_expression,
    default_year_value,
    quote_identifier,
    table_columns,
)
from app.query_frame import STATE_TO_POSTAL


_DATASET_TABLES: dict[tuple[str, str], str] = {
    ("census", "state"): "acs_state",
    ("census", "county"): "acs_county",
    ("census", "congress"): "acs_congress",
    ("gov_spending", "state"): "gov_state",
    ("gov_spending", "county"): "gov_county",
    ("gov_spending", "congress"): "gov_congress",
    ("finra", "state"): "finra_state",
    ("finra", "county"): "finra_county",
    ("finra", "congress"): "finra_congress",
    ("contract_static", "state"): "contract_state",
    ("contract_static", "county"): "contract_county",
    ("contract_static", "congress"): "contract_congress",
    ("contract_agency", "state"): "spending_state_agency",
    ("spending_breakdown", "state"): "spending_state",
}


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _normalize_state_name(value: str) -> str:
    return value.strip().lower()


def resolve_map_table(dataset: str, level: str) -> str | None:
    return _DATASET_TABLES.get((dataset, level))


def _metric_select_expr(table_name: str, variable: str) -> str:
    if variable == "spending_total":
        expr = (
            agency_spending_total_expression(table_name, alias="spending_total")
            if table_name == "spending_state_agency"
            else default_spending_total_expression(table_name, alias="spending_total")
        )
        if not expr:
            raise ValueError(f"{table_name} does not support spending_total.")
        return expr

    if variable not in table_columns(table_name):
        raise ValueError(f"{variable} is not available in {table_name}.")
    return f"{quote_identifier(variable)} AS {quote_identifier(variable)}"


def _default_year_label(table_name: str) -> str | None:
    default = default_year_value(table_name)
    if default is not None:
        return str(default)
    if table_name.startswith("gov_"):
        return "Fiscal Year 2023"
    return None


def fetch_map_values(
    *,
    dataset: str,
    level: str,
    variable: str,
    year: str | None = None,
    state: str | None = None,
    agency: str | None = None,
) -> list[dict[str, Any]]:
    table_name = resolve_map_table(dataset, level)
    if not table_name:
        raise ValueError(f"{dataset} does not support {level}-level map values.")

    if dataset == "contract_agency" and not agency:
        raise ValueError("contract_agency map requests require an agency filter.")

    table_cols = table_columns(table_name)
    select_cols: list[str] = []

    if level == "state":
        for col in ("state", "state_fips"):
            if col in table_cols:
                select_cols.append(quote_identifier(col))
    elif level == "county":
        for col in ("county", "state", "county_fips", "fips", "state_fips"):
            if col in table_cols:
                select_cols.append(quote_identifier(col))
    elif level == "congress":
        for col in ("cd_118", "state", "state_fips"):
            if col in table_cols:
                select_cols.append(quote_identifier(col))

    year_column = "year" if "year" in table_cols else "Year" if "Year" in table_cols else None
    if year_column:
        select_cols.append(quote_identifier(year_column))

    select_cols.append(_metric_select_expr(table_name, variable))

    where_clauses: list[str] = []
    year_value = year or _default_year_label(table_name)
    if year_value and year_column:
        if year_column == "Year" and year_value.isdigit():
            where_clauses.append(f"{quote_identifier(year_column)} = {year_value}")
        else:
            where_clauses.append(f"{quote_identifier(year_column)} = {_sql_string(year_value)}")

    if state:
        normalized_state = _normalize_state_name(state)
        if "state" in table_cols:
            where_clauses.append(f"LOWER(state) = {_sql_string(normalized_state)}")
        elif level == "congress" and "cd_118" in table_cols:
            postal = STATE_TO_POSTAL.get(normalized_state)
            if not postal:
                raise ValueError(f"Unknown state for congressional filter: {state}")
            where_clauses.append(f"UPPER(cd_118) LIKE {_sql_string(postal + '-%')}")
        else:
            raise ValueError(f"{table_name} does not support state filtering.")

    if agency:
        if "agency" not in table_cols:
            raise ValueError(f"{table_name} does not expose agency filtering.")
        where_clauses.append(f"agency = {_sql_string(agency)}")

    sql = f"SELECT {', '.join(select_cols)} FROM {table_name}"
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    sql += f" ORDER BY {quote_identifier(variable)} DESC NULLS LAST" if variable != "spending_total" else " ORDER BY spending_total DESC NULLS LAST"

    df = execute_query(sql)
    return df.to_dict(orient="records")
