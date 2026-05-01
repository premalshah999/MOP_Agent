from __future__ import annotations

from typing import Any

from app.schemas.query_plan import Filter, QueryPlan, QuerySpec
from app.semantic.models import DatasetDefinition, MetricDefinition
from app.semantic.registry import get_dataset, quote_identifier


class SqlGenerationError(ValueError):
    pass


def _sql_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _column_for(dataset: DatasetDefinition, field: str) -> str:
    if field == "year":
        if not dataset.year_column:
            raise SqlGenerationError(f"Dataset `{dataset.id}` has no year column.")
        return dataset.year_column
    if field in dataset.dimensions:
        return dataset.dimensions[field].column
    if field in dataset.columns:
        return field
    if field == dataset.label_column:
        return dataset.label_column
    raise SqlGenerationError(f"Unknown field `{field}` for dataset `{dataset.id}`.")


def _dimension_column(dataset: DatasetDefinition, query: QuerySpec) -> str:
    dimension = query.dimensions[0] if query.dimensions else dataset.label_column
    return _column_for(dataset, dimension)


def _is_dimension_filter(dataset: DatasetDefinition, query: QuerySpec, filter_: Filter) -> bool:
    if filter_.operator not in {"=", "IN"}:
        return False
    try:
        return _column_for(dataset, filter_.field) == _dimension_column(dataset, query)
    except SqlGenerationError:
        return False


def _label_filter_sql(filter_: Filter) -> str:
    if filter_.operator == "IN":
        values = filter_.value if isinstance(filter_.value, list) else [filter_.value]
        rendered = ", ".join(_sql_literal(str(value).lower()) for value in values)
        return f"LOWER(label) IN ({rendered})"
    return f"LOWER(label) = LOWER({_sql_literal(filter_.value)})"


def _filter_sql(query: QuerySpec, filter_: Filter) -> str:
    dataset = get_dataset(query.dataset)
    if not dataset:
        raise SqlGenerationError(f"Unknown dataset `{query.dataset}`.")
    column = _column_for(dataset, filter_.field)
    quoted = quote_identifier(column)
    if filter_.operator == "IN":
        values = filter_.value if isinstance(filter_.value, list) else [filter_.value]
        if all(isinstance(value, str) for value in values):
            rendered = ", ".join(_sql_literal(str(value).lower()) for value in values)
            return f"LOWER(CAST({quoted} AS VARCHAR)) IN ({rendered})"
        return f"{quoted} IN ({', '.join(_sql_literal(value) for value in values)})"
    if isinstance(filter_.value, str) and filter_.operator == "LIKE":
        return f"UPPER(CAST({quoted} AS VARCHAR)) LIKE UPPER({_sql_literal(filter_.value)})"
    if isinstance(filter_.value, str) and filter_.operator == "=":
        return f"LOWER(CAST({quoted} AS VARCHAR)) = LOWER({_sql_literal(filter_.value)})"
    return f"{quoted} {filter_.operator} {_sql_literal(filter_.value)}"


def _metric_expr(metric: MetricDefinition) -> str:
    agg = metric.aggregation.lower()
    if agg == "avg":
        return f"AVG({metric.sql})"
    if agg == "max":
        return f"MAX({metric.sql})"
    if agg == "min":
        return f"MIN({metric.sql})"
    return f"SUM({metric.sql})"


def _base_query(dataset: DatasetDefinition, query: QuerySpec, metric: MetricDefinition) -> str:
    label_column = quote_identifier(_dimension_column(dataset, query))
    where_parts = [_filter_sql(query, filter_) for filter_ in query.filters]
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    metric_expr = _metric_expr(metric)
    return f"""
WITH aggregated AS (
    SELECT
        CAST({label_column} AS VARCHAR) AS label,
        {metric_expr} AS metric_value
    FROM {dataset.view_name}
    {where_sql}
    GROUP BY 1
)
SELECT
    ROW_NUMBER() OVER (ORDER BY metric_value {query.order}, label ASC) AS rank,
    label,
    metric_value
FROM aggregated
WHERE metric_value IS NOT NULL
ORDER BY {"label ASC" if query.operation == "trend" else "rank ASC"}
{f"LIMIT {query.limit}" if query.limit else ""}
""".strip()


def _position_query(dataset: DatasetDefinition, query: QuerySpec, metric: MetricDefinition) -> str:
    label_column = quote_identifier(_dimension_column(dataset, query))
    focus_filters = [filter_ for filter_ in query.filters if _is_dimension_filter(dataset, query, filter_)]
    base_filters = [filter_ for filter_ in query.filters if filter_ not in focus_filters]
    where_parts = [_filter_sql(query, filter_) for filter_ in base_filters]
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    focus_sql = " OR ".join(_label_filter_sql(filter_) for filter_ in focus_filters) or "TRUE"
    metric_expr = _metric_expr(metric)
    return f"""
WITH aggregated AS (
    SELECT
        CAST({label_column} AS VARCHAR) AS label,
        {metric_expr} AS metric_value
    FROM {dataset.view_name}
    {where_sql}
    GROUP BY 1
),
ranked AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY metric_value {query.order}, label ASC) AS rank,
        label,
        metric_value,
        COUNT(*) OVER () AS total_count,
        AVG(metric_value) OVER () AS peer_average,
        MAX(metric_value) OVER () AS peer_max,
        MIN(metric_value) OVER () AS peer_min
    FROM aggregated
    WHERE metric_value IS NOT NULL
)
SELECT
    rank,
    label,
    metric_value,
    total_count,
    peer_average,
    peer_max,
    peer_min
FROM ranked
WHERE {focus_sql}
ORDER BY rank ASC
""".strip()


def generate_sql(plan: QueryPlan) -> list[dict[str, str]]:
    sql_items: list[dict[str, str]] = []
    for query in plan.queries:
        dataset = get_dataset(query.dataset)
        if not dataset:
            raise SqlGenerationError(f"Unknown dataset `{query.dataset}`.")
        if not query.metric or query.metric not in dataset.metrics:
            raise SqlGenerationError(f"Unknown metric `{query.metric}` for dataset `{dataset.id}`.")

        metric = dataset.metrics[query.metric]
        sql = _position_query(dataset, query, metric) if query.operation == "position" else _base_query(dataset, query, metric)
        sql_items.append({"name": query.name, "sql": sql})
    return sql_items
