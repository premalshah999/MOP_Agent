"""Meaning-level validation for generated analytical SQL.

The existing validator protects the database.  This one protects the answer:
it checks the SQL against the typed analysis contract and grounded entities
before execution.  It is intentionally provider-neutral and never generates a
query or an answer itself.
"""

from __future__ import annotations

import re
from typing import Any

import sqlglot
from sqlglot import exp

from app.core.analysis_contract import AnalysisContract
from app.semantic.registry import get_dataset
from app.semantic.value_resolver import RESOLVABLE_COLUMNS
from app.sql.validator import SqlValidationError


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", str(value).lower())).strip()


def _parse(sql: str) -> exp.Expression:
    try:
        return sqlglot.parse_one(sql, read="duckdb")
    except Exception as exc:
        raise SqlValidationError(f"SQL parser rejected semantic validation: {exc}") from exc


def _physical_tables(tree: exp.Expression) -> set[str]:
    ctes = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    return {table.name.lower() for table in tree.find_all(exp.Table) if table.name.lower() not in ctes}


def _column_literals(tree: exp.Expression, column: str) -> set[str]:
    """String/numeric literals used to constrain a column."""
    found: set[str] = set()
    predicate_types = (exp.EQ, exp.In, exp.Between)
    for predicate in tree.find_all(predicate_types):
        columns = {c.name.lower() for c in predicate.find_all(exp.Column)}
        if column.lower() not in columns:
            continue
        for literal in predicate.find_all(exp.Literal):
            found.add(str(literal.this))
    return found


def _positive_dimension_filters(tree: exp.Expression) -> list[tuple[str, str]]:
    filters: list[tuple[str, str]] = []
    for predicate in tree.find_all((exp.EQ, exp.In)):
        columns = list(predicate.find_all(exp.Column))
        literals = [x for x in predicate.find_all(exp.Literal) if x.is_string]
        if not columns or not literals:
            continue
        column = columns[0].name.lower()
        if column not in {x.lower() for x in RESOLVABLE_COLUMNS}:
            continue
        filters.extend((column, str(literal.this)) for literal in literals)
    return filters


def _grounded_values(resolved: dict[str, Any], column: str) -> set[str]:
    values: set[str] = set()
    for table_entities in resolved.values():
        if not isinstance(table_entities, dict):
            continue
        info = table_entities.get(column)
        if not isinstance(info, dict):
            continue
        for value in info.get("values") or [info.get("value")]:
            if value is not None:
                values.add(_norm(value))
    return values


def semantic_sql_problems(
    sql: str,
    question: str,
    contract: AnalysisContract,
    resolved: dict[str, Any] | None = None,
    *,
    enforce_shape: bool = True,
) -> list[str]:
    tree = _parse(sql)
    resolved = resolved or {}
    problems: list[str] = []

    expected_views = {
        ds.view_name.lower()
        for table in contract.tables
        if (ds := get_dataset(table)) is not None
    }
    used_views = _physical_tables(tree)
    unexpected = sorted(used_views - expected_views)
    if unexpected:
        problems.append(
            "query uses table(s) outside the routing contract: " + ", ".join(unexpected)
        )

    referenced_columns = {column.name.casefold() for column in tree.find_all(exp.Column)}
    missing_metrics = [
        metric
        for metric in contract.metric_columns
        if metric.casefold() not in referenced_columns
    ]
    if missing_metrics:
        problems.append(
            "query does not use required metric column(s): " + ", ".join(missing_metrics)
        )

    # Positive filters on entity-like columns must trace to an entity grounded
    # from the user's text.  This blocks invented agencies and geographies.
    q_norm = _norm(question)
    for column, literal in _positive_dimension_filters(tree):
        literal_norm = _norm(literal)
        grounded = _grounded_values(resolved, column)
        if literal_norm not in grounded and not re.search(rf"\b{re.escape(literal_norm)}\b", q_norm):
            problems.append(
                f"filter {column}={literal!r} is not grounded in an entity named by the user"
            )

    # A single-period query must use the requested year, or the registry's
    # declared latest/default year.  No silent multi-year sums.
    for table in contract.tables:
        dataset = get_dataset(table)
        if dataset is None or not dataset.year_column:
            continue
        constrained = {_norm(x) for x in _column_literals(tree, dataset.year_column)}
        available = {_norm(x) for x in dataset.available_years}
        if contract.requested_period and _norm(contract.requested_period) in available:
            expected_values = [contract.requested_period]
        elif len(set(contract.requested_years)) > 1:
            expected_values = list(dict.fromkeys(contract.requested_years))
        elif contract.operation == "trend":
            expected_values = []
        else:
            expected = contract.explicit_year if contract.explicit_year is not None else dataset.default_year
            expected_values = [expected] if expected is not None else []
        missing_years = [value for value in expected_values if _norm(value) not in constrained]
        if missing_years:
            problems.append(
                f"{table} must constrain {dataset.year_column!r} to "
                f"{', '.join(str(x) for x in missing_years)}; "
                "otherwise the result changes across multiple years"
            )

    sql_lower = sql.lower()
    if any(table.endswith("_flow") for table in contract.tables):
        if contract.flow_direction == "inflow" and "subawardee_" not in sql_lower:
            problems.append("inflow/receives must aggregate or filter on the subawardee side")
        if contract.flow_direction == "outflow" and "rcpt_" not in sql_lower:
            problems.append("outflow/sends must aggregate or filter on the prime-recipient rcpt side")

    if enforce_shape and contract.operation == "correlation":
        if not re.search(r"\bcorr\s*\(", sql_lower):
            problems.append("a correlation question must compute CORR over the requested observations")
        if tree.args.get("limit") is not None:
            problems.append("correlation must not be computed from an arbitrary top-N slice")

    if enforce_shape and contract.top_k is not None:
        limit = tree.args.get("limit")
        limit_values = [int(x.this) for x in limit.find_all(exp.Literal) if str(x.this).isdigit()] if limit else []
        if contract.top_k not in limit_values:
            problems.append(f"the user requested {contract.top_k} results, so LIMIT must equal {contract.top_k}")

    if enforce_shape and contract.operation == "comparison":
        for column in RESOLVABLE_COLUMNS:
            named = _grounded_values(resolved, column)
            if len(named) < 2:
                continue
            sql_norm = _norm(sql)
            missing = sorted(value for value in named if not re.search(rf"\b{re.escape(value)}\b", sql_norm))
            if missing:
                problems.append("comparison omits named entities: " + ", ".join(missing))

    if len(used_views) > 1:
        joins = list(tree.find_all(exp.Join))
        if any(join.args.get("on") is None and join.args.get("using") is None for join in joins):
            problems.append("cross-dataset queries require an explicit join key; cartesian joins are not allowed")

    # Stable ordering prevents the same tied ranking from changing between runs.
    if enforce_shape and contract.operation == "ranking":
        order = tree.args.get("order")
        if order is None:
            problems.append("ranking queries require an explicit ORDER BY")
        else:
            ordered = list(order.expressions)
            if contract.top_k and contract.top_k > 1 and len(ordered) < 2:
                problems.append("rankings need a label tie-breaker after the measure for stable ordering")
            if ordered and contract.sort_direction in {"asc", "desc"}:
                actual = "desc" if bool(ordered[0].args.get("desc")) else "asc"
                if actual != contract.sort_direction:
                    problems.append(
                        f"ranking sort direction is {actual}, but the user requested {contract.sort_direction}"
                    )

    return list(dict.fromkeys(problems))


def validate_semantic_sql(
    sql: str,
    question: str,
    contract: AnalysisContract,
    resolved: dict[str, Any] | None = None,
    *,
    enforce_shape: bool = True,
) -> None:
    problems = semantic_sql_problems(
        sql, question, contract, resolved, enforce_shape=enforce_shape
    )
    if problems:
        raise SqlValidationError("semantic contract: " + "; ".join(problems))


def stabilize_verified_ranking_sql(sql: str, contract: AnalysisContract) -> str:
    """Add a deterministic label tie-breaker to a blessed ranking query.

    Older repository entries predate the repeatability contract and commonly
    order only by the metric.  The first selected field in those hand-reviewed
    queries is their label, so adding it after the metric cannot change the
    ranking semantics; it only fixes the order of exact ties.
    """
    if contract.operation != "ranking":
        return sql
    tree = _parse(sql)
    order = tree.args.get("order")
    if order is None or len(order.expressions) != 1 or not tree.expressions:
        return sql
    first_projection = tree.expressions[0]
    label = first_projection.alias_or_name
    if not label:
        return sql
    order.append("expressions", exp.Ordered(this=exp.column(label), desc=False))
    return tree.sql(dialect="duckdb")


def normalize_generated_sql(sql: str, contract: AnalysisContract) -> str:
    """Normalize harmless model-chosen output aliases that leak API drift."""
    if contract.operation != "correlation":
        return sql
    tree = _parse(sql)
    if len(tree.expressions) != 1:
        return sql
    projection = tree.expressions[0]
    # Keep the full calculation and force one provider-neutral response field.
    tree.set("expressions", [projection.unalias().as_("correlation")])
    return tree.sql(dialect="duckdb")
