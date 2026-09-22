"""Meaning-level validation for generated analytical SQL.

The existing validator protects the database.  This one protects the answer:
it checks the SQL against the typed analysis contract and grounded entities
before execution.  It is intentionally provider-neutral and never generates a
query or an answer itself.
"""

from __future__ import annotations

import re
from typing import Any, cast

import sqlglot
from sqlglot import exp

from app.core.analysis_plan import AnalysisContract
from app.semantic.registry import get_dataset
from app.semantic.value_resolver import RESOLVABLE_COLUMNS
from app.sql.validator import SqlValidationError

# These are denominator contracts from the curated ACS dictionary, not answer
# heuristics. The validator never chooses a metric or writes SQL; it only blocks
# arithmetic that gives a precise-looking number a statistically false meaning.
_ACS_PERCENT_DENOMINATORS: dict[str, str | None] = {
    "age 18-65": "total population",
    "white": "total population",
    "black": "total population",
    "asian": "total population",
    "hispanic": "total population",
    "education >= high school": None,
    "education >= bachelor's": None,
    "education >= graduate": None,
    "income >$50k": "# of household",
    "income >$100k": "# of household",
    "income >$200k": "# of household",
    "below poverty": None,
    "owner occupied": None,
    "renter occupied": None,
}
_ACS_COUNT_DENOMINATORS = {"total population", "# of household"}
_FINRA_NONCOUNT_METRICS = {
    "financial_constraint",
    "alternative_financing",
    "financial_literacy",
    "satisfied",
    "risk_averse",
}


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", str(value).lower())).strip()


def _parse(sql: str) -> exp.Expression:
    try:
        return cast(exp.Expression, sqlglot.parse_one(sql, read="duckdb"))
    except Exception as exc:
        raise SqlValidationError(f"SQL parser rejected semantic validation: {exc}") from exc


def _physical_tables(tree: exp.Expression) -> set[str]:
    ctes = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    return {
        table.name.lower() for table in tree.find_all(exp.Table) if table.name.lower() not in ctes
    }


def _column_literals(tree: exp.Expression, column: str) -> set[str]:
    """String/numeric literals used to constrain a column."""
    found: set[str] = set()
    predicates: list[exp.Expression] = []
    predicates.extend(tree.find_all(exp.EQ))
    predicates.extend(tree.find_all(exp.In))
    predicates.extend(tree.find_all(exp.Between))
    for predicate in predicates:
        columns = {c.name.lower() for c in predicate.find_all(exp.Column)}
        if column.lower() not in columns:
            continue
        for literal in predicate.find_all(exp.Literal):
            found.add(str(literal.this))
    return found


def _positive_dimension_filters(tree: exp.Expression) -> list[tuple[str, str]]:
    filters: list[tuple[str, str]] = []
    predicates: list[exp.Expression] = []
    predicates.extend(tree.find_all(exp.EQ))
    predicates.extend(tree.find_all(exp.In))
    for predicate in predicates:
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


def _required_grounded_entity_problems(sql: str, resolved: dict[str, Any]) -> list[str]:
    """Require every explicitly resolved entity scope to appear in the SQL."""

    sql_norm = _norm(sql)
    problems: list[str] = []
    for table_entities in resolved.values():
        if not isinstance(table_entities, dict):
            continue
        for column, info in table_entities.items():
            if not isinstance(info, dict):
                continue
            values = [
                str(value)
                for value in (info.get("values") or [info.get("value")])
                if value not in (None, "")
            ]
            if not values:
                continue
            required: list[str]
            district_codes = {
                match.group(1).casefold()
                for value in values
                if (match := re.match(r"^([A-Za-z]{2})[- ]", value.strip()))
            }
            if len(district_codes) == 1 and len(values) > 1:
                required = list(district_codes)
            else:
                required = [_norm(value) for value in values]
            missing = [
                value
                for value in required
                if value and not re.search(rf"\b{re.escape(value)}\b", sql_norm)
            ]
            if missing:
                problems.append(f"query omits the named {column} scope: {', '.join(missing)}")
    return problems


def _logical_table_aliases(tree: exp.Expression, contract: AnalysisContract) -> dict[str, str]:
    """Map SQL aliases/view names to logical catalog table names."""
    view_to_table = {
        dataset.view_name.casefold(): table
        for table in contract.tables
        if (dataset := get_dataset(table)) is not None
    }
    aliases: dict[str, str] = {}
    for node in tree.find_all(exp.Table):
        logical = view_to_table.get(node.name.casefold())
        if logical is None:
            continue
        aliases[node.alias_or_name.casefold()] = logical
        aliases[node.name.casefold()] = logical
    return aliases


def _single_effective_year(contract: AnalysisContract, table: str) -> str | int | None:
    """Return the one period a non-trend table must use, when defined."""
    dataset = get_dataset(table)
    if dataset is None or not dataset.year_column or contract.operation == "trend":
        return None
    planned = contract.period_by_table.get(table)
    if planned not in (None, "") and not isinstance(planned, list):
        return planned
    available = {_norm(value) for value in dataset.available_years}
    if contract.requested_period and _norm(contract.requested_period) in available:
        return contract.requested_period
    if contract.explicit_year is not None:
        return contract.explicit_year
    if len(set(contract.requested_years)) == 1:
        return contract.requested_years[0]
    return dataset.default_year


def _incompatible_year_joins(tree: exp.Expression, contract: AnalysisContract) -> list[str]:
    """Find joins that equate year columns whose required periods differ."""
    aliases = _logical_table_aliases(tree, contract)
    problems: list[str] = []
    for predicate in tree.find_all(exp.EQ):
        columns = list(predicate.find_all(exp.Column))
        if len(columns) != 2:
            continue
        left, right = columns
        left_table = aliases.get(left.table.casefold()) if left.table else None
        right_table = aliases.get(right.table.casefold()) if right.table else None
        if not left_table or not right_table or left_table == right_table:
            continue
        left_dataset = get_dataset(left_table)
        right_dataset = get_dataset(right_table)
        if left_dataset is None or right_dataset is None:
            continue
        if not left_dataset.year_column or not right_dataset.year_column:
            continue
        if left.name.casefold() != left_dataset.year_column.casefold():
            continue
        if right.name.casefold() != right_dataset.year_column.casefold():
            continue
        left_year = _single_effective_year(contract, left_table)
        right_year = _single_effective_year(contract, right_table)
        if left_year is None or right_year is None or _norm(left_year) == _norm(right_year):
            continue
        problems.append(
            f"do not join {left_table}.{left_dataset.year_column} to "
            f"{right_table}.{right_dataset.year_column}: their required catalog "
            f"periods differ ({left_year} vs {right_year}); filter each table to "
            "its own period and join only on the shared geography"
        )
    return problems


def _acs_derivation_problems(tree: exp.Expression, contract: AnalysisContract) -> list[str]:
    if not any(table.startswith("acs_") for table in contract.tables):
        return []
    problems: list[str] = []
    if contract.result_unit == "persons":
        planned = {metric.casefold() for metric in contract.metric_columns}
        planned_percentages = planned & set(_ACS_PERCENT_DENOMINATORS)
        for percentage in planned_percentages:
            denominator = _ACS_PERCENT_DENOMINATORS[percentage]
            if denominator is None:
                problems.append(
                    f"{percentage!r} cannot produce result_unit=persons because "
                    "its matching count denominator is not loaded"
                )
                continue
            formula_operands = {operand.casefold() for operand in contract.formula.operands}
            if (
                contract.formula.operator != "multiply"
                or {percentage, denominator} - formula_operands
                or abs(contract.formula.scale - 0.01) > 1e-12
            ):
                problems.append(
                    f"ACS result_unit=persons for {percentage!r} must use the "
                    f"typed formula {denominator!r} * {percentage!r} with scale 0.01"
                )
    for multiplication in tree.find_all(exp.Mul):
        columns = {column.name.casefold() for column in multiplication.find_all(exp.Column)}
        percentages = columns & set(_ACS_PERCENT_DENOMINATORS)
        denominators = columns & _ACS_COUNT_DENOMINATORS
        if len(percentages) > 1:
            problems.append(
                "ACS percentage columns are separate marginal estimates and cannot "
                "be multiplied to infer a joint demographic subgroup"
            )
        for percentage in percentages:
            expected = _ACS_PERCENT_DENOMINATORS[percentage]
            if not denominators:
                continue
            if expected is None:
                problems.append(
                    f"{percentage!r} has no matching count denominator in the curated "
                    "ACS table, so it cannot be converted to a count"
                )
            elif denominators != {expected}:
                problems.append(
                    f"{percentage!r} must use {expected!r} as its denominator, not "
                    f"{', '.join(sorted(denominators))}"
                )
    return problems


def _finra_count_derivation_problems(tree: exp.Expression, contract: AnalysisContract) -> list[str]:
    """Block survey aggregates being expanded into unsupported headcounts.

    The runtime FINRA files contain shares/indices but no respondent count,
    survey weights, or population denominator. Multiplying a FINRA value by an
    ACS population creates a precise-looking estimate with no documented
    statistical basis.
    """
    if not any(table.startswith("finra_") for table in contract.tables):
        return []
    problems: list[str] = []
    for multiplication in tree.find_all(exp.Mul):
        columns = {column.name.casefold() for column in multiplication.find_all(exp.Column)}
        finra_metrics = columns & _FINRA_NONCOUNT_METRICS
        population_denominators = columns & _ACS_COUNT_DENOMINATORS
        if finra_metrics and population_denominators:
            problems.append(
                "FINRA shares and indices have no loaded population or survey-weight "
                "denominator and cannot be converted to resident counts using ACS population"
            )
    return problems


def _correlation_sample_problems(tree: exp.Expression) -> list[str]:
    """Require sample evidence that matches every coefficient in a matrix."""
    correlations = list(tree.find_all(exp.Corr))
    if len(correlations) <= 1:
        return []
    counts = list(tree.find_all(exp.Count))
    if len(counts) >= len(correlations):
        return []

    # One shared sample is valid only for complete-case analysis across every
    # matrix variable. Resolve simple CTE aliases back to physical columns.
    alias_sources: dict[str, str] = {}
    for alias in tree.find_all(exp.Alias):
        if isinstance(alias.this, exp.Column) and alias.alias:
            alias_sources[alias.alias.casefold()] = alias.this.name.casefold()
    required: list[set[str]] = []
    for correlation in correlations:
        for column in correlation.find_all(exp.Column):
            name = column.name.casefold()
            required.append({name, alias_sources.get(name, name)})
    non_null: set[str] = set()
    for negation in tree.find_all(exp.Not):
        predicate = negation.this
        if not isinstance(predicate, exp.Is) or not isinstance(predicate.expression, exp.Null):
            continue
        if isinstance(predicate.this, exp.Column):
            non_null.add(predicate.this.name.casefold())
    if required and all(candidates & non_null for candidates in required) and counts:
        return []
    return [
        "a correlation matrix must return a paired sample count for every "
        "coefficient, or apply one complete-case non-null filter across every "
        "matrix variable and return that shared sample_size"
    ]


def _atomic_result_sql_problems(
    tree: exp.Expression,
    contract: AnalysisContract,
) -> list[str]:
    """Validate SQL properties implied by one atomic result contract.

    This does not decide what the question means. The language planner already
    selected ``statistic`` and ``result_scope``. These checks prevent SQL from
    quietly changing that typed decision into one row per observation, which
    previously produced county-level ``COUNT=1`` rows for scalar counts and
    null within-county correlations for a correlation across counties.
    """

    problems: list[str] = []
    root_group = tree.args.get("group") if isinstance(tree, exp.Select) else None
    atomic_statistics = {
        "sum",
        "average",
        "median",
        "count",
        "count_distinct",
        "correlation",
        "derived",
    }
    unconstrained_group = False
    if root_group is not None:
        for grouped_expression in root_group.expressions:
            columns = list(grouped_expression.find_all(exp.Column))
            if columns and any(len(_column_literals(tree, column.name)) != 1 for column in columns):
                unconstrained_group = True
                break
    if (
        contract.result_scope == "single"
        and contract.statistic in atomic_statistics
        and contract.operation != "ranking"
        and unconstrained_group
    ):
        problems.append(
            "a single-result analysis must not GROUP BY the repeated observation "
            "grain in the final SELECT; aggregate across those observations and "
            "return one result row"
        )

    if contract.statistic == "count_distinct":
        distinct_counts = [
            count for count in tree.find_all(exp.Count) if isinstance(count.this, exp.Distinct)
        ]
        if not distinct_counts:
            problems.append(
                "a count_distinct plan must use COUNT(DISTINCT <geography key>), "
                "not COUNT(*), COUNT per group, or a geography listing"
            )

    required_aggregate: type[exp.Expression] | None = {
        "sum": exp.Sum,
        "average": exp.Avg,
        "median": exp.Median,
        "count": exp.Count,
    }.get(contract.statistic)
    if required_aggregate is not None and not any(tree.find_all(required_aggregate)):
        problems.append(
            f"a {contract.statistic} plan must compute the corresponding SQL "
            "aggregate instead of returning unaggregated geography rows"
        )
    return problems


def result_shape_problems(
    rows: list[dict[str, Any]],
    contract: AnalysisContract,
) -> list[str]:
    """Check executed evidence against the planner-selected result shape.

    SQL syntax checks alone cannot detect a query that technically contains an
    aggregate but returns it once per observation. This post-execution check is
    deliberately small and provider-neutral: it verifies row cardinality only
    when the typed plan explicitly promises one atomic result.
    """

    if not rows:
        return []
    if contract.result_scope == "single" and len(rows) != 1:
        return [
            f"the analysis contract requires one result row, but the query returned {len(rows)}"
        ]
    return []


def validate_result_shape(
    rows: list[dict[str, Any]],
    contract: AnalysisContract,
) -> None:
    problems = result_shape_problems(rows, contract)
    if problems:
        raise SqlValidationError("result shape: " + "; ".join(problems))


def _formula_alias_sources(tree: exp.Expression) -> dict[str, str]:
    """Resolve simple/one-source SQL aliases back to semantic metric ids."""
    output: dict[str, str] = {}
    for alias in tree.find_all(exp.Alias):
        if not alias.alias:
            continue
        sources = {
            column.name.casefold()
            for column in alias.this.find_all(exp.Column)
            if column.name.casefold() != alias.alias.casefold()
        }
        if isinstance(alias.this, exp.Column):
            sources.add(alias.this.name.casefold())
        if len(sources) == 1:
            output[alias.alias.casefold()] = next(iter(sources))
    return output


def _expression_metric_names(
    expression: exp.Expression,
    alias_sources: dict[str, str],
) -> set[str]:
    names: set[str] = set()
    for column in expression.find_all(exp.Column):
        name = column.name.casefold()
        names.add(name)
        if name in alias_sources:
            names.add(alias_sources[name])
    return names


def _formula_problems(
    tree: exp.Expression,
    contract: AnalysisContract,
) -> list[str]:
    """Validate the planner's typed formula without interpreting prose."""
    formula = contract.formula
    operator = formula.operator
    operands = [operand.casefold() for operand in formula.operands]
    if operator in {"none", "identity", "net_flow"}:
        return []
    if operator in {"subtract", "divide"} and len(operands) != 2:
        return [f"semantic plan {operator} formula must contain exactly two ordered operands"]

    alias_sources = _formula_alias_sources(tree)
    expression_type: type[exp.Expression]
    if operator == "subtract":
        expression_type = exp.Sub
    elif operator == "divide":
        expression_type = exp.Div
    elif operator == "add":
        expression_type = exp.Add
    elif operator == "multiply":
        expression_type = exp.Mul
    else:
        return []

    for expression in tree.find_all(expression_type):
        if operator in {"subtract", "divide"}:
            left = _expression_metric_names(expression.this, alias_sources)
            right = _expression_metric_names(expression.expression, alias_sources)
            if operands[0] in left and operands[1] in right:
                if operator == "divide" and formula.scale == 100.0:
                    has_scale = any(
                        literal.this in {"100", "100.0"} for literal in tree.find_all(exp.Literal)
                    )
                    if not has_scale:
                        return ["percentage formula must apply the semantic plan scale of 100"]
                return []
        else:
            referenced = _expression_metric_names(expression, alias_sources)
            if set(operands).issubset(referenced):
                if operator == "multiply" and formula.scale != 1.0:
                    literals = {
                        float(literal.this)
                        for literal in tree.find_all(exp.Literal)
                        if not literal.is_string
                        and re.fullmatch(r"-?\d+(?:\.\d+)?", str(literal.this))
                    }
                    reciprocal = 1.0 / formula.scale if formula.scale != 0 else None
                    if not any(
                        abs(value - formula.scale) <= 1e-12
                        or (reciprocal is not None and abs(value - reciprocal) <= 1e-9)
                        for value in literals
                    ):
                        return [
                            f"SQL does not apply the semantic plan formula scale of {formula.scale}"
                        ]
                return []
    ordered = " then ".join(operands) if operands else "the planned operands"
    return [f"SQL does not implement the semantic plan formula {operator} using {ordered}"]


def _predicate_problems(
    tree: exp.Expression,
    contract: AnalysisContract,
    resolved: dict[str, Any],
) -> list[str]:
    """Require the planner's typed row-selection comparison."""
    predicate = contract.predicate
    if predicate.operator == "none":
        return []
    compares_literal = len(predicate.operands) == 1 and predicate.comparison_value is not None
    if len(predicate.operands) != 2 and not compares_literal:
        return [
            f"semantic plan {predicate.operator} predicate must contain two ordered "
            "measure operands or one measure operand plus comparison_value"
        ]
    expression_types: dict[str, type[exp.Expression]] = {
        "gt": exp.GT,
        "gte": exp.GTE,
        "lt": exp.LT,
        "lte": exp.LTE,
        "eq": exp.EQ,
        "neq": exp.NEQ,
    }
    expected_type = expression_types[predicate.operator]
    left_operand = predicate.operands[0].casefold()
    right_operand = predicate.operands[1].casefold() if len(predicate.operands) == 2 else None
    alias_sources = _formula_alias_sources(tree)
    for expression in tree.find_all(expected_type):
        left = _expression_metric_names(expression.this, alias_sources)
        if left_operand not in left:
            continue
        if compares_literal:
            literal = expression.expression
            if isinstance(literal, exp.Literal):
                expected = predicate.comparison_value
                if literal.is_string:
                    # Entity resolution canonicalizes user-facing aliases before
                    # SQL generation (for example, ``MD``/``Maryland`` becomes
                    # the stored value ``MARYLAND``).  Treat that grounded value
                    # as equivalent to the planner's original wording.  This is
                    # deliberately limited to the predicate column and values
                    # resolved from the user's question; it does not make the
                    # validator accept arbitrary alternate literals.
                    accepted = {_norm(expected)}
                    accepted.update(_grounded_values(resolved, predicate.operands[0]))
                    if _norm(literal.this) in accepted:
                        return []
                try:
                    if (
                        not literal.is_string
                        and abs(float(literal.this) - float(expected)) <= 1e-12
                    ):
                        return []
                except (TypeError, ValueError):
                    pass
        else:
            right = _expression_metric_names(expression.expression, alias_sources)
            if right_operand is not None and right_operand in right:
                return []
    target = predicate.comparison_value if compares_literal else predicate.operands[1]
    return [
        "SQL does not implement the semantic plan row predicate "
        f"{predicate.operands[0]} {predicate.operator} {target}"
    ]


def _predicate_unit_problems(contract: AnalysisContract) -> list[str]:
    """Reject row comparisons whose operands have incompatible units."""

    predicate = contract.predicate
    if predicate.operator == "none" or len(predicate.operands) != 2:
        return []

    def family(unit: str) -> str:
        normalized = unit.casefold()
        if normalized in {"usd", "dollars", "currency"}:
            return "currency"
        if normalized in {"person", "persons", "people", "count"}:
            return "count"
        if "0-1" in normalized and any(
            term in normalized for term in ("index", "share", "proportion", "ratio")
        ):
            return "bounded_0_1"
        return normalized

    operand_families: list[set[str]] = []
    for operand in predicate.operands:
        families: set[str] = set()
        for table in contract.tables:
            dataset = get_dataset(table)
            metric = dataset.metrics.get(operand) if dataset is not None else None
            if metric is not None:
                families.add(family(metric.unit))
        operand_families.append(families)
    if (
        operand_families[0]
        and operand_families[1]
        and operand_families[0].isdisjoint(operand_families[1])
    ):
        return [
            "row predicates cannot directly compare measures with incompatible "
            f"units: {predicate.operands[0]} versus {predicate.operands[1]}"
        ]
    return []


def _explicit_full_ranking_problems(
    tree: exp.Expression,
    contract: AnalysisContract,
) -> list[str]:
    """Honor the result scope selected once by the language planner."""
    if contract.operation != "ranking" or contract.result_scope != "full":
        return []
    if tree.args.get("limit") is not None:
        return ["the semantic plan requests the full ranking scope and must not add LIMIT"]
    return []


def _flow_direction_filter_problems(
    tree: exp.Expression,
    contract: AnalysisContract,
    resolved: dict[str, Any],
) -> list[str]:
    """Require a named flow destination/origin to be filtered on the right side.

    Merely mentioning a subawardee column somewhere in a query is insufficient:
    a query can otherwise filter Maryland on the origin side, group the
    destination side, and falsely label the result "Maryland inflow."  This is
    validation only; it does not choose or generate the query.
    """
    if contract.flow_direction not in {"inflow", "outflow"}:
        return []
    if not any(table.endswith("_flow") for table in contract.tables):
        return []

    correct_prefix = "subawardee_" if contract.flow_direction == "inflow" else "rcpt_"
    side_columns = {
        str(column).casefold()
        for column in RESOLVABLE_COLUMNS
        if str(column).casefold().startswith(correct_prefix)
    }
    grounded: set[str] = set()
    for column in side_columns:
        grounded.update(_grounded_values(resolved, column))
    if not grounded:
        return []  # national ranking/breakdown with no named geography

    filters = _positive_dimension_filters(tree)
    if any(
        column.startswith(correct_prefix) and _norm(literal) in grounded
        for column, literal in filters
    ):
        return []
    side = (
        "subawardee/destination"
        if contract.flow_direction == "inflow"
        else "prime-recipient/origin"
    )
    return [f"named {contract.flow_direction} geography must be filtered on the {side} side"]


def _net_flow_problems(
    tree: exp.Expression,
    contract: AnalysisContract,
    resolved: dict[str, Any],
) -> list[str]:
    if not any(table.endswith("_flow") for table in contract.tables):
        return []
    if contract.formula.operator != "net_flow":
        return []
    referenced = {column.name.casefold() for column in tree.find_all(exp.Column)}
    has_origin = any(
        column.startswith("rcpt_") or column.startswith("prime_awardee_") for column in referenced
    )
    has_destination = any(column.startswith("subawardee_") for column in referenced)
    problems: list[str] = []
    if not has_origin or not has_destination or not any(tree.find_all(exp.Sub)):
        problems.append(
            "net subaward flow must compute destination inflow minus origin outflow "
            "using both subawardee_* and rcpt_* sides"
        )
        return problems

    filters = _positive_dimension_filters(tree)
    for prefix, label in (("subawardee_", "destination"), ("rcpt_", "origin")):
        grounded: set[str] = set()
        for column in RESOLVABLE_COLUMNS:
            if column.casefold().startswith(prefix):
                grounded.update(_grounded_values(resolved, column.casefold()))
        if grounded and not any(
            column.startswith(prefix) and _norm(literal) in grounded for column, literal in filters
        ):
            problems.append(
                f"net subaward flow does not constrain the named place on the {label} side"
            )
    return problems


def _flow_cross_table_join_problems(
    tree: exp.Expression,
    contract: AnalysisContract,
) -> list[str]:
    """Require the documented canonical keys for contract/flow derivations."""
    tables = set(contract.tables)
    if not (
        {"contract_county", "county_flow"} <= tables
        or {"contract_congress", "congress_flow"} <= tables
    ):
        return []
    referenced = {column.name.casefold() for column in tree.find_all(exp.Column)}
    alias_sources = _formula_alias_sources(tree)

    def joined(left_names: set[str], right_names: set[str]) -> bool:
        """Require the directional key in an actual equality join.

        Merely mentioning both keys somewhere in a query is insufficient: a
        destination join could include an unused origin column and previously
        pass validation. Simple CTE aliases are resolved through their source
        column so normal pre-aggregation remains supported.
        """
        for join in tree.find_all(exp.Join):
            condition = join.args.get("on")
            if condition is None:
                continue
            for equality in condition.find_all(exp.EQ):
                left = _expression_metric_names(equality.this, alias_sources)
                right = _expression_metric_names(equality.expression, alias_sources)
                if (
                    left_names & left
                    and right_names & right
                    or left_names & right
                    and right_names & left
                ):
                    return True
        return False

    if {"contract_county", "county_flow"} <= tables:
        valid_flow_keys = {"rcpt_cty", "subawardee_cty"}
        if contract.flow_direction == "outflow":
            valid_flow_keys = {"rcpt_cty"}
        elif contract.flow_direction == "inflow":
            valid_flow_keys = {"subawardee_cty"}
        if not joined({"county_fips"}, valid_flow_keys):
            direction_detail = (
                f"the {contract.flow_direction} side ({next(iter(valid_flow_keys))})"
                if contract.flow_direction in {"inflow", "outflow"}
                else "rcpt_cty and/or subawardee_cty"
            )
            return [
                "contract_county/county_flow derivations must join county_fips "
                f"to {direction_detail}, not county-name strings or the opposite "
                "flow direction"
            ]
    if {"contract_congress", "congress_flow"} <= tables:
        flow_ids = {"prime_awardee_stcd118", "subawardee_stcd118"}
        if contract.flow_direction == "outflow":
            flow_ids = {"prime_awardee_stcd118"}
        elif contract.flow_direction == "inflow":
            flow_ids = {"subawardee_stcd118"}
        if not (
            {"state_fips", "cd_118"} <= referenced and joined({"state_fips", "cd_118"}, flow_ids)
        ):
            direction_detail = (
                f"the {contract.flow_direction} side ({next(iter(flow_ids))})"
                if contract.flow_direction in {"inflow", "outflow"}
                else "prime_awardee_stcd118 and/or subawardee_stcd118"
            )
            return [
                "contract_congress/congress_flow derivations must canonicalize "
                "state_fips + cd_118 district number to numeric STCD118 and join "
                f"to {direction_detail}"
            ]
    # A contract/workforce row is the one-side of a geography-to-flow-event
    # join. Aggregating the contract denominator in the same SELECT that sums
    # flow events repeats that denominator once per event and can silently
    # shrink a percentage by orders of magnitude. This is a relational-grain
    # invariant: aggregate the flow side first, or group by and divide by the
    # single contract value.
    formula = contract.formula
    if formula.operator == "divide" and len(formula.operands) == 2:
        numerator, denominator = (operand.casefold() for operand in formula.operands)
        for select in tree.find_all(exp.Select):
            scoped_sums = [
                aggregate
                for aggregate in select.find_all(exp.Sum)
                if aggregate.find_ancestor(exp.Select) is select
            ]
            summed_metrics = [_expression_metric_names(aggregate, {}) for aggregate in scoped_sums]
            if any(numerator in names for names in summed_metrics) and any(
                denominator in names for names in summed_metrics
            ):
                return [
                    "contract/flow percentage denominator is duplicated by the "
                    "one-to-many event join; aggregate the flow side first or "
                    "divide by the single grouped contract value, never "
                    f"SUM({formula.operands[1]}) beside SUM({formula.operands[0]})"
                ]
    # Contract tables enumerate the full geography base; flow tables contain
    # only observed events. In a LEFT JOIN, no matching event means zero flow,
    # not an unknown value. Require COALESCE so zero-event counties/districts
    # produce 0 and 0% instead of misleading nulls.
    if not any(tree.find_all(exp.Coalesce)):
        return [
            "contract/flow derivations over all geographies must COALESCE "
            "missing flow-event aggregates to zero"
        ]
    if contract.operation == "correlation" and not any(tree.find_all(exp.Coalesce)):
        return [
            "contract/flow correlations across all geographies must LEFT JOIN "
            "from the contract geography base and COALESCE absent flow-event "
            "aggregates to zero before CORR"
        ]
    return []


def _geography_projection_problems(
    tree: exp.Expression,
    contract: AnalysisContract,
) -> list[str]:
    """Keep multi-geography results intelligible and mappable to users."""
    multirow_operation = contract.operation in {
        "ranking",
        "comparison",
        "breakdown",
        "distribution",
    }
    named_output = bool(contract.output_dimensions) and contract.operation in {
        "lookup",
        "aggregate",
    }
    if not multirow_operation and not named_output:
        return []
    legacy_required = {
        "state": "state",
        "county": "county",
        "congress": "cd_118",
    }.get(contract.geography_level)
    if not legacy_required:
        return []
    projections = list(getattr(tree, "expressions", []) or [])
    if any(projection.find(exp.Star) is not None for projection in projections):
        return []
    projected_columns = {
        column.name.casefold()
        for projection in projections
        for column in projection.find_all(exp.Column)
    }
    projected_columns.update(
        projection.alias.casefold()
        for projection in projections
        if getattr(projection, "alias", "")
    )
    planned_dimensions = {dimension.casefold() for dimension in contract.output_dimensions}
    if planned_dimensions:
        missing = sorted(planned_dimensions - projected_columns)
        if not missing:
            return []
        return [
            f"{contract.geography_level} result rows must SELECT the planned "
            f"readable dimension(s) {', '.join(missing)}, not only an id or measure"
        ]

    # Legacy/provider plans can omit output_dimensions. Derive readable labels
    # from the routed physical schemas instead of requiring a generic column
    # that flow tables do not contain. Both directional flow labels are valid
    # here: a focused inflow breakdown may display origins, while a national
    # inflow ranking displays destinations. Direction correctness is enforced
    # separately by the flow predicates and typed plan.
    readable_dimensions: set[str] = set()
    flow_labels = {
        "state_flow": {"rcpt_state_name", "subawardee_state_name"},
        "county_flow": {"rcpt_cty_name", "subawardee_cty_name"},
        "congress_flow": {"rcpt_cd_name", "subawardee_cd_name"},
    }
    for table in contract.tables:
        if table in flow_labels:
            readable_dimensions.update(flow_labels[table])
            continue
        dataset = get_dataset(table)
        if dataset is not None and dataset.label_column:
            readable_dimensions.add(dataset.label_column.casefold())
    if not readable_dimensions:
        readable_dimensions.add(legacy_required.casefold())
    if projected_columns.isdisjoint(readable_dimensions):
        expected = ", ".join(sorted(readable_dimensions))
        return [
            f"{contract.geography_level} result rows must SELECT a readable "
            f"geography dimension ({expected}), not only an id or measure"
        ]
    return []


def _focused_derived_output_problems(
    tree: exp.Expression,
    contract: AnalysisContract,
) -> list[str]:
    """Keep a pure derived-percentage request focused on its requested result.

    The provider may legitimately use numerator and denominator columns inside
    a CTE. Exposing those implementation fields in the final SELECT, however,
    changes tables and chart inputs between identical runs. This invariant is
    is controlled by the typed plan, so paraphrases receive identical behavior
    and requests that explicitly include components remain valid.
    """
    if contract.formula.operator != "divide" or contract.include_component_measures:
        return []

    dimensions = {value.casefold() for value in contract.output_dimensions}
    if not dimensions:
        dimensions.update({"state", "county", "cd_118", "agency", "agency_name"})
    projections = list(getattr(tree, "expressions", []) or [])
    if any(projection.find(exp.Star) is not None for projection in projections):
        return [
            "a pure derived percentage/ratio request must return only geography "
            "labels and the requested derived measure, not SELECT *"
        ]

    extra: list[str] = []
    for projection in projections:
        alias = projection.alias_or_name.casefold()
        normalized_alias = re.sub(r"[^a-z0-9]+", "_", alias).strip("_")
        if normalized_alias in dimensions:
            continue
        if projection.find(exp.Div) is not None or re.search(
            r"(?:^|_)(?:pct|percent|percentage|ratio)(?:_|$)", normalized_alias
        ):
            continue
        extra.append(alias or projection.sql(dialect="duckdb"))
    if not extra:
        return []
    return [
        "a pure derived percentage/ratio request must expose only geography "
        "labels and the final percentage/ratio; remove component output "
        "column(s): " + ", ".join(extra)
    ]


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
        ds.view_name.lower() for table in contract.tables if (ds := get_dataset(table)) is not None
    }
    used_views = _physical_tables(tree)
    unexpected = sorted(used_views - expected_views)
    if unexpected:
        problems.append(
            "query uses table(s) outside the routing contract: " + ", ".join(unexpected)
        )

    referenced_columns = {column.name.casefold() for column in tree.find_all(exp.Column)}
    if enforce_shape:
        missing_metrics = [
            metric
            for metric in contract.metric_columns
            if metric.casefold() not in referenced_columns
        ]
        if missing_metrics:
            problems.append(
                "query does not use required metric column(s): " + ", ".join(missing_metrics)
            )
    problems.extend(_acs_derivation_problems(tree, contract))
    problems.extend(_finra_count_derivation_problems(tree, contract))
    problems.extend(_predicate_unit_problems(contract))
    if enforce_shape:
        problems.extend(_formula_problems(tree, contract))
        problems.extend(_predicate_problems(tree, contract, resolved))
        problems.extend(_explicit_full_ranking_problems(tree, contract))
        problems.extend(_atomic_result_sql_problems(tree, contract))
    if enforce_shape:
        # These validate the final analytical result. Reasoning-mode inspection
        # queries may intentionally examine one non-flow table before composing
        # the final join and must not be rejected for lacking a flow-side field.
        problems.extend(_flow_direction_filter_problems(tree, contract, resolved))
        problems.extend(_net_flow_problems(tree, contract, resolved))
        problems.extend(_flow_cross_table_join_problems(tree, contract))
        problems.extend(_geography_projection_problems(tree, contract))
        problems.extend(_focused_derived_output_problems(tree, contract))

    # Positive filters on entity-like columns must trace to an entity grounded
    # from the user's text.  This blocks invented agencies and geographies.
    if enforce_shape:
        q_norm = _norm(question)
        for column, literal in _positive_dimension_filters(tree):
            literal_norm = _norm(literal)
            grounded = _grounded_values(resolved, column)
            if literal_norm not in grounded and not re.search(
                rf"\b{re.escape(literal_norm)}\b", q_norm
            ):
                problems.append(
                    f"filter {column}={literal!r} is not grounded in an entity named by the user"
                )
        problems.extend(_required_grounded_entity_problems(sql, resolved))

    # A single-period query must use the requested year, or the registry's
    # declared latest/default year. No silent multi-year sums. In reasoning
    # mode, one contract can span several tables while an intermediate query
    # intentionally touches only one of them; require the period only for a
    # table that this SQL statement actually reads.
    for table in contract.tables:
        dataset = get_dataset(table)
        if dataset is None or not dataset.year_column:
            continue
        if dataset.view_name.casefold() not in used_views:
            continue
        if table.startswith("gov_"):
            # The Year field is one descriptive VARCHAR value ('Fiscal Year
            # 2023'), not a time dimension. Availability is checked before SQL
            # generation; valid snapshot queries must not add a year filter.
            continue
        constrained = {_norm(x) for x in _column_literals(tree, dataset.year_column)}
        available = {_norm(x) for x in dataset.available_years}
        expected_values: list[str | int] = []
        planned = contract.period_by_table.get(table)
        if planned not in (None, "", "catalog snapshot", "all available periods"):
            expected_values.extend(planned if isinstance(planned, list) else [planned])
        elif contract.requested_period and _norm(contract.requested_period) in available:
            expected_values.append(contract.requested_period)
        elif len(set(contract.requested_years)) > 1:
            expected_values.extend(dict.fromkeys(contract.requested_years))
        elif contract.operation != "trend":
            expected = (
                contract.explicit_year
                if contract.explicit_year is not None
                else dataset.default_year
            )
            if expected is not None:
                expected_values.append(expected)
        missing_years = [value for value in expected_values if _norm(value) not in constrained]
        if missing_years:
            problems.append(
                f"{table} must constrain {dataset.year_column!r} to "
                f"{', '.join(str(x) for x in missing_years)}; "
                "otherwise the result changes across multiple years"
            )

    sql_lower = sql.lower()
    if enforce_shape and any(table.endswith("_flow") for table in contract.tables):
        if contract.flow_direction == "inflow" and "subawardee_" not in sql_lower:
            problems.append("inflow/receives must aggregate or filter on the subawardee side")
        if (
            contract.flow_direction == "outflow"
            and "rcpt_" not in sql_lower
            and "prime_awardee_" not in sql_lower
        ):
            problems.append(
                "outflow/sends must aggregate or filter on the prime-recipient "
                "rcpt/prime_awardee side"
            )

    if enforce_shape and contract.operation == "correlation":
        if not re.search(r"\bcorr\s*\(", sql_lower):
            problems.append(
                "a correlation question must compute CORR over the requested observations"
            )
        if not any(tree.find_all(exp.Count)):
            problems.append(
                "a correlation query must return the non-null paired observation "
                "count as sample_size"
            )
        problems.extend(_correlation_sample_problems(tree))
        if tree.args.get("limit") is not None:
            problems.append("correlation must not be computed from an arbitrary top-N slice")

    if enforce_shape and contract.top_k is not None and contract.operation != "correlation":
        limit = tree.args.get("limit")
        limit_values = (
            [int(x.this) for x in limit.find_all(exp.Literal) if str(x.this).isdigit()]
            if limit
            else []
        )
        if contract.top_k not in limit_values:
            problems.append(
                f"the user requested {contract.top_k} results, so LIMIT must equal {contract.top_k}"
            )

    if enforce_shape and contract.operation == "comparison":
        for column in RESOLVABLE_COLUMNS:
            named = _grounded_values(resolved, column)
            if len(named) < 2:
                continue
            sql_norm = _norm(sql)
            missing = sorted(
                value for value in named if not re.search(rf"\b{re.escape(value)}\b", sql_norm)
            )
            if missing:
                problems.append("comparison omits named entities: " + ", ".join(missing))

    if len(used_views) > 1:
        joins = list(tree.find_all(exp.Join))
        if any(join.args.get("on") is None and join.args.get("using") is None for join in joins):
            problems.append(
                "cross-dataset queries require an explicit join key; cartesian joins are not allowed"
            )
        problems.extend(_incompatible_year_joins(tree, contract))

    # Stable ordering prevents the same tied ranking from changing between runs.
    if enforce_shape and contract.operation == "ranking":
        order = tree.args.get("order")
        if order is None:
            problems.append("ranking queries require an explicit ORDER BY")
        else:
            ordered = list(order.expressions)
            formula_operator = contract.formula.operator
            measure_keys = (
                len(contract.sort_columns or contract.metric_columns[:1])
                if formula_operator in {"none", "identity"}
                else 1
            )
            # Multi-row rankings need a deterministic final key after every
            # planned ranking measure. A second measure alone is not a stable
            # label because rows can tie on both measures. Single-result
            # rankings keep their long-standing valid shape; their returned
            # number is unaffected by presentation order among exact ties.
            needs_stable_label = contract.result_scope in {"top_n", "full", "grouped"}
            if needs_stable_label and len(ordered) <= max(1, measure_keys):
                problems.append(
                    "rankings need a label tie-breaker after the measure for stable ordering"
                )
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
    problems = semantic_sql_problems(sql, question, contract, resolved, enforce_shape=enforce_shape)
    if problems:
        raise SqlValidationError("semantic contract: " + "; ".join(problems))


def normalize_generated_sql(sql: str, contract: AnalysisContract) -> str:
    """Normalize harmless model-chosen output aliases that leak API drift."""
    if contract.operation != "correlation":
        return sql
    tree = _parse(sql)
    corr_indexes = [
        index
        for index, projection in enumerate(tree.expressions)
        if projection.find(exp.Corr) is not None
    ]
    if len(corr_indexes) != 1:
        return sql
    index = corr_indexes[0]
    projections = list(tree.expressions)
    # Keep the full calculation plus sample_size and force one
    # provider-neutral correlation response field.
    projections[index] = projections[index].unalias().as_("correlation")
    tree.set("expressions", projections)
    return tree.sql(dialect="duckdb")
