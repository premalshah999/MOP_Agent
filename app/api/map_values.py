from __future__ import annotations

from typing import Any

from app.duckdb.connection import execute_select
from app.semantic.registry import get_dataset, quote_identifier


_DATASET_MAP = {
    "census": "acs",
    "gov_spending": "gov",
    "finra": "finra",
    "contract_static": "contract",
    "contract_agency": "spending",
    "fund_flow": "state",
}


def fetch_values(dataset: str, level: str, variable: str, year: str | None = None, state: str | None = None) -> list[dict[str, Any]]:
    prefix = _DATASET_MAP.get(dataset, dataset)
    table_id = f"{prefix}_{level}"
    definition = get_dataset(table_id)
    if not definition:
        return []
    if variable not in definition.columns and variable not in definition.metrics:
        return []
    metric_sql = definition.metrics[variable].sql if variable in definition.metrics else quote_identifier(variable)
    filters: list[str] = []
    def esc(value: object) -> str:
        return str(value).replace("'", "''")

    if year and definition.year_column:
        filters.append(f"CAST({quote_identifier(definition.year_column)} AS VARCHAR) = '{esc(year)}'")
    elif definition.default_year is not None and definition.year_column:
        filters.append(f"CAST({quote_identifier(definition.year_column)} AS VARCHAR) = '{esc(definition.default_year)}'")
    if state and "state" in definition.columns:
        filters.append(f"LOWER(CAST({quote_identifier('state')} AS VARCHAR)) = LOWER('{esc(state)}')")
    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
    select_parts = [
        f"{quote_identifier(definition.label_column)} AS label",
        f"{metric_sql} AS metric_value",
        f"{metric_sql} AS {quote_identifier(variable)}",
    ]
    for column in ("state", "county", "cd_118", "agency", "agency_name"):
        if column in definition.columns:
            alias = "agency" if column == "agency_name" else column
            select_parts.append(f"{quote_identifier(column)} AS {quote_identifier(alias)}")
    sql = f"SELECT {', '.join(select_parts)} FROM {definition.view_name} {where_sql}"
    return execute_select(sql, max_rows=5000)
