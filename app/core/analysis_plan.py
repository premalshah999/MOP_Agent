"""Typed analysis plan shared by planning, query validation, and the UI.

The LLM still decides what the question means.  This module turns that decision
into a small, provider-neutral contract and reconciles only facts that are
explicit in the user's wording (years, top-N, sort and flow direction).  Those
facts then become invariants instead of suggestions buried in a prompt.
"""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.semantic.registry import get_dataset

Operation = Literal[
    "lookup",
    "ranking",
    "comparison",
    "trend",
    "correlation",
    "distribution",
    "aggregate",
    "breakdown",
]
FlowDirection = Literal["inflow", "outflow", "none"]
SortDirection = Literal["asc", "desc", "none"]
Statistic = Literal[
    "value",
    "sum",
    "average",
    "median",
    "count",
    "count_distinct",
    "correlation",
    "distribution",
    "derived",
    "unspecified",
]
FormulaOperator = Literal[
    "none",
    "identity",
    "add",
    "subtract",
    "multiply",
    "divide",
    "net_flow",
]
PredicateOperator = Literal["none", "gt", "gte", "lt", "lte", "eq", "neq"]
ResultScope = Literal["single", "top_n", "full", "grouped", "unspecified"]
ResultUnit = Literal[
    "usd",
    "persons",
    "percent",
    "ratio",
    "count",
    "correlation",
    "index",
    "value",
    "unspecified",
]

_OPERATIONS = {
    "lookup",
    "ranking",
    "comparison",
    "trend",
    "correlation",
    "distribution",
    "aggregate",
    "breakdown",
}
_YEAR_RE = re.compile(r"\b(?:fy\s*)?((?:19|20)\d{2})\b", re.IGNORECASE)
_PERIOD_RE = re.compile(
    r"\b((?:19|20)\d{2})\s*(?:-|–|—|to|through)\s*((?:19|20)\d{2})\b",
    re.IGNORECASE,
)
_TOP_RE = re.compile(
    r"\b(?:top|bottom|first|last|highest|lowest|largest|smallest|most|least)\s+(\d{1,3})\b",
    re.IGNORECASE,
)


class FormulaSpec(BaseModel):
    """Provider-neutral calculation selected by the language planner.

    Operands are exact runtime metric ids in semantic order. Downstream code
    validates/executes this structure; it must not infer a different formula
    from the user's prose.
    """

    operator: FormulaOperator = "none"
    operands: list[str] = Field(default_factory=list)
    scale: float = 1.0
    output_label: str = ""


class PredicateSpec(BaseModel):
    """Typed row-selection comparison chosen by the language planner."""

    operator: PredicateOperator = "none"
    operands: list[str] = Field(default_factory=list)
    # A comparison can be measure-to-measure or measure-to-literal. Keeping
    # the literal typed prevents the SQL writer from guessing that a valid
    # threshold such as "above 20%" is a malformed two-measure predicate.
    comparison_value: float | str | None = None


class SemanticPlan(BaseModel):
    """The language-understanding portion of an analysis request.

    Dataset routing and periods remain outside this model because they are
    reconciled with the physical registry. Everything here is meaning selected
    by the LLM and consumed as typed state by the rest of the pipeline.
    """

    operation: Operation = "lookup"
    statistic: Statistic = "unspecified"
    result_unit: ResultUnit = "unspecified"
    formula: FormulaSpec = Field(default_factory=FormulaSpec)
    predicate: PredicateSpec = Field(default_factory=PredicateSpec)
    observation_grain: str = "none"
    result_scope: ResultScope = "unspecified"
    sort_direction: SortDirection = "none"
    sort_columns: list[str] = Field(default_factory=list)
    top_k: int | None = None
    flow_direction: FlowDirection = "none"
    include_component_measures: bool = False
    output_dimensions: list[str] = Field(default_factory=list)


class AnalysisContract(BaseModel):
    intent: str = "ANALYTICAL"
    tables: list[str] = Field(default_factory=list)
    metric_columns: list[str] = Field(default_factory=list)
    geography_level: str = "none"
    operation: Operation = "lookup"
    flow_direction: FlowDirection = "none"
    explicit_year: int | None = None
    requested_years: list[int] = Field(default_factory=list)
    requested_period: str | None = None
    period_by_table: dict[str, Any] = Field(default_factory=dict)
    effective_period: Any = None
    year_strategy: str = ""
    sort_direction: SortDirection = "none"
    sort_columns: list[str] = Field(default_factory=list)
    top_k: int | None = None
    join_plan: str = ""
    assumptions: list[str] = Field(default_factory=list)
    statistic: Statistic = "unspecified"
    result_unit: ResultUnit = "unspecified"
    formula: FormulaSpec = Field(default_factory=FormulaSpec)
    predicate: PredicateSpec = Field(default_factory=PredicateSpec)
    observation_grain: str = "none"
    result_scope: ResultScope = "unspecified"
    include_component_measures: bool = False
    output_dimensions: list[str] = Field(default_factory=list)


def _with_required_bridge_tables(tables: list[str]) -> list[str]:
    """Add physical key bridges required by the runtime schema.

    Some congressional datasets expose only ``cd_118`` while congress_flow
    stores numeric STCD118. ``contract_congress`` is the loaded authoritative
    mapping from ``cd_118`` to ``state_fips``. Adding it is a physical join
    requirement, not a change to the user's requested measures.
    """

    output = list(dict.fromkeys(str(table) for table in tables))
    if "congress_flow" not in output or len(output) < 2:
        return output
    needs_bridge = any(
        table not in {"congress_flow", "contract_congress"}
        and (dataset := get_dataset(table)) is not None
        and "congress" in dataset.geography.casefold()
        and "state_fips" not in dataset.columns
        for table in output
    )
    if needs_bridge and "contract_congress" not in output:
        output.append("contract_congress")
    return output


def semantic_plan_from_routing(routing: dict[str, Any]) -> SemanticPlan:
    """Normalize a planner response without re-reading the user question.

    Older fixtures/providers may omit ``semantic_plan``. In that case this
    function derives only from already-decided routing fields, never from raw
    prose. That keeps one interpretation authoritative at runtime.
    """
    raw = routing.get("semantic_plan")
    payload = dict(raw) if isinstance(raw, dict) else {}

    operation = str(payload.get("operation") or routing.get("operation") or "lookup").casefold()
    if operation not in _OPERATIONS:
        operation = "lookup"
    if str(payload.get("statistic") or "").casefold() == "correlation":
        # A correlation statistic is never an aggregate lookup. Canonicalize
        # inconsistent provider JSON before execution so correlation shape,
        # sample-size validation, and final evidence selection all engage.
        operation = "correlation"
    payload["operation"] = operation

    sort_direction = str(
        payload.get("sort_direction") or routing.get("sort_direction") or "none"
    ).casefold()
    payload["sort_direction"] = (
        sort_direction if sort_direction in {"asc", "desc", "none"} else "none"
    )
    sort_columns = payload.get("sort_columns")
    payload["sort_columns"] = sort_columns if isinstance(sort_columns, list) else []
    flow_direction = str(
        payload.get("flow_direction") or routing.get("flow_direction") or "none"
    ).casefold()
    payload["flow_direction"] = (
        flow_direction if flow_direction in {"inflow", "outflow", "none"} else "none"
    )

    proposed_top = payload.get("top_k", routing.get("top_k"))
    try:
        top_k = int(proposed_top) if proposed_top is not None else None
    except (TypeError, ValueError):
        top_k = None
    payload["top_k"] = top_k if top_k is not None and 1 <= top_k <= 250 else None

    result_scope = str(payload.get("result_scope") or "unspecified").casefold()
    if result_scope not in {"single", "top_n", "full", "grouped", "unspecified"}:
        result_scope = "unspecified"
    # ``top 1`` and ``single best`` are the same result shape. Providers can
    # legitimately emit either label for an identical request, so collapse the
    # representation here instead of letting a cosmetic enum choice look like
    # semantic drift. This changes neither the requested limit nor the answer.
    if operation == "ranking" and payload["top_k"] == 1:
        result_scope = "single"
    elif result_scope == "unspecified" and payload["top_k"] is not None:
        result_scope = "top_n"
    payload["result_scope"] = result_scope
    # Internal plan consistency is a type-system concern, not language
    # interpretation. A lookup cannot represent a full/grouped geography
    # result, and a top-N result is necessarily a ranking.
    if operation == "lookup" and result_scope in {"full", "grouped"}:
        operation = "breakdown"
        payload["operation"] = operation
    elif operation == "lookup" and result_scope == "top_n":
        operation = "ranking"
        payload["operation"] = operation

    statistic = str(payload.get("statistic") or "unspecified").casefold()
    allowed_statistics = {
        "value",
        "sum",
        "average",
        "median",
        "count",
        "count_distinct",
        "correlation",
        "distribution",
        "derived",
        "unspecified",
    }
    if statistic not in allowed_statistics:
        statistic = "unspecified"
    if statistic == "unspecified" and operation == "correlation":
        statistic = "correlation"
    elif statistic == "unspecified" and operation == "distribution":
        statistic = "distribution"
    payload["statistic"] = statistic
    output_dimensions = payload.get("output_dimensions")
    if not isinstance(output_dimensions, list):
        output_dimensions = []
    payload["output_dimensions"] = output_dimensions
    # A grouped result over an explicitly filtered set of the same dimension
    # is a finite peer comparison, not an open-ended dimensional breakdown.
    # This uses only the planner's typed decisions: it does not re-interpret
    # user prose or add dataset-specific language rules. For example, filtering
    # state to two named values and returning state as the output dimension is
    # canonically a comparison; filtering state while outputting agency remains
    # a breakdown.
    filter_columns = routing.get("filter_columns")
    normalized_filters = {
        str(value).strip().casefold().replace("_", " ")
        for value in (filter_columns if isinstance(filter_columns, list) else [])
    }
    normalized_dimensions = {
        str(value).strip().casefold().replace("_", " ") for value in output_dimensions
    }
    if (
        operation == "breakdown"
        and result_scope == "grouped"
        and normalized_filters.intersection(normalized_dimensions)
    ):
        operation = "comparison"
        payload["operation"] = operation
    if statistic == "correlation" and result_scope == "grouped" and not output_dimensions:
        # A grouped statistic must name the dimension that defines its groups.
        # Several coefficient columns still form one atomic correlation result;
        # only an explicit output dimension (for example one coefficient per
        # state) makes that result grouped. This reconciles the typed fields
        # without reinterpreting the user's prose.
        result_scope = "single"
        payload["result_scope"] = result_scope
    result_unit = str(payload.get("result_unit") or "unspecified").casefold()
    allowed_units = {
        "usd",
        "persons",
        "percent",
        "ratio",
        "count",
        "correlation",
        "index",
        "value",
        "unspecified",
    }
    payload["result_unit"] = result_unit if result_unit in allowed_units else "unspecified"
    payload["observation_grain"] = str(
        payload.get("observation_grain") or routing.get("geography_level") or "none"
    )

    formula = payload.get("formula")
    if not isinstance(formula, dict):
        formula = {}
    payload["formula"] = formula
    predicate = payload.get("predicate")
    if not isinstance(predicate, dict):
        predicate = {}
    payload["predicate"] = predicate
    if (
        payload["statistic"] == "unspecified"
        and str(formula.get("operator") or "none").casefold() != "none"
    ):
        payload["statistic"] = "derived"
    try:
        return SemanticPlan.model_validate(payload)
    except Exception:
        # A malformed optional semantic block must not crash the service. The
        # already-normalized routing decisions remain a conservative fallback.
        return SemanticPlan(
            operation=operation,  # type: ignore[arg-type]
            statistic=statistic,  # type: ignore[arg-type]
            result_unit=payload["result_unit"],  # type: ignore[arg-type]
            observation_grain=str(payload["observation_grain"]),
            result_scope=result_scope,  # type: ignore[arg-type]
            sort_direction=payload["sort_direction"],  # type: ignore[arg-type]
            sort_columns=[str(value) for value in payload["sort_columns"]],
            top_k=payload["top_k"],
            flow_direction=payload["flow_direction"],  # type: ignore[arg-type]
        )


def _strategy_years_for_table(year_strategy: str, table: str) -> list[int]:
    """Read table-scoped years from the router's explicit period plan.

    Cross-source questions commonly contain more than one valid year, such as
    ACS 2023 joined to FINRA 2021.  The router already emits plans like
    ``acs_state: Year=2023; finra_state: Year=2021``.  Keep those assignments
    separate instead of treating every year as a requirement on every table.
    """
    if not year_strategy:
        return []
    match = re.search(
        rf"(?<![A-Za-z0-9_]){re.escape(table)}(?![A-Za-z0-9_])\s*[:=-]?\s*([^;\n]+)",
        year_strategy,
        re.IGNORECASE,
    )
    if not match:
        return []
    return [int(value) for value in _YEAR_RE.findall(match.group(1))]


def _strategy_clause_for_table(year_strategy: str, table: str) -> str:
    """Return only the table's clause from a multi-table period strategy."""
    if not year_strategy:
        return ""
    match = re.search(
        rf"(?<![A-Za-z0-9_]){re.escape(table)}(?![A-Za-z0-9_])\s*[:=-]?\s*([^;\n]+)",
        year_strategy,
        re.IGNORECASE,
    )
    return match.group(1).strip() if match else ""


def resolve_periods_by_table(
    tables: list[str],
    requested_years: list[int],
    requested_period: str | None,
    year_strategy: str,
    *,
    operation: Operation = "lookup",
) -> dict[str, Any]:
    """Resolve one period plan per physical table from catalog facts.

    The LLM supplies semantic association through ``year_strategy``; this
    function only validates those years against each table's available
    periods and fills unambiguous/default cases.  It never chooses a metric or
    changes the user's requested years.
    """
    output: dict[str, Any] = {}
    unique_years = list(dict.fromkeys(requested_years))
    for table in tables:
        dataset = get_dataset(str(table))
        if dataset is None:
            continue
        if str(table).startswith("gov_"):
            # Government-finance tables are fixed FY2023 snapshots whose Year
            # string is descriptive, not a query dimension. Keep FY2023 as the
            # catalog snapshot so SQL is not forced to compare that VARCHAR to
            # integer 2023. Preserve other requested years so the availability
            # guard can refuse them precisely.
            clause = _strategy_clause_for_table(year_strategy, str(table))
            planned = _strategy_years_for_table(year_strategy, str(table))
            explicitly_snapshot = bool(
                re.search(r"\b(?:catalog\s+)?snapshot\b|\bno\s+year\s+filter\b", clause, re.I)
            )
            if explicitly_snapshot or (len(tables) > 1 and not planned):
                output[str(table)] = "catalog snapshot"
            elif not unique_years or set(unique_years) == {2023}:
                output[str(table)] = "catalog snapshot"
            elif planned:
                output[str(table)] = planned[0] if len(planned) == 1 else planned
            elif len(unique_years) == 1:
                output[str(table)] = unique_years[0]
            else:
                output[str(table)] = unique_years
            continue
        available = {str(value).strip("'\"") for value in dataset.available_years}
        if requested_period and requested_period in available:
            output[str(table)] = requested_period
            continue
        planned = [
            year
            for year in _strategy_years_for_table(year_strategy, str(table))
            if str(year) in available
        ]
        if planned:
            output[str(table)] = planned[0] if len(planned) == 1 else planned
            continue
        compatible = [year for year in unique_years if str(year) in available]
        if len(compatible) == 1:
            output[str(table)] = compatible[0]
        elif len(unique_years) == 1:
            # Retain an unavailable explicit year so the availability guard can
            # produce a precise table-specific refusal.
            output[str(table)] = unique_years[0]
        elif operation == "trend":
            output[str(table)] = compatible or "all available periods"
        elif not unique_years and dataset.default_year is not None:
            output[str(table)] = dataset.default_year
        elif not unique_years:
            output[str(table)] = "catalog snapshot"
    return output


def _explicit_operation(question: str, proposed: Any) -> Operation:
    q = question.lower()
    rank_q = re.sub(r"\bat\s+(?:least|most)\b", "", q)
    if re.search(r"\b(correlat\w*|relationship|association)\b", q):
        return "correlation"
    if re.search(r"\b(trend|over time|year[- ]over[- ]year|by year)\b", q):
        return "trend"
    if re.search(r"\b(top|bottom|rank|highest|lowest|largest|smallest|most|least)\b", rank_q):
        return "ranking"
    if re.search(r"\b(compare|versus|vs\.?|difference between)\b", q):
        return "comparison"
    if re.search(r"\b(by agency|by category|breakdown|composition)\b", q):
        return "breakdown"
    proposed_norm = str(proposed or "").strip().lower()
    if proposed_norm in {"correlation", "distribution"}:
        # Metric labels can contain words such as "median" or "average".
        # Those words do not override an authoritative statistical plan unless
        # the user explicitly requested a different operation above.
        return proposed_norm  # type: ignore[return-value]
    if re.search(r"\b(total|average|mean|median|sum|how much|how many)\b", q):
        return "aggregate"
    if re.search(r"\bat\s+(?:least|most)\b", q):
        return "lookup"
    if proposed_norm in _OPERATIONS:
        return proposed_norm  # type: ignore[return-value]
    return "lookup"


def _flow_direction(question: str, proposed: Any) -> FlowDirection:
    q = question.lower()
    if re.search(
        r"(?:\bnet\b.*\b(?:subaward|subcontract|flow)|"
        r"\b(?:subaward|subcontract)\b.*\bnet\b)",
        q,
    ):
        return "none"
    # User wording wins over the model because these phrases are unambiguous.
    if re.search(
        r"(?:\b(receiv\w*|inflow|incoming|coming in|flowing to)\b|"
        r"\bflows?\s+(?:in(?:to)?|to)\b|"
        r"\b(?:subawards?|subcontracts?|sub[- ]contracts?)\s+in\b)",
        q,
    ):
        return "inflow"
    if re.search(
        r"(?:\b(send\w*|outflow|outgoing|going out|flowing from)\b|"
        r"\bflows?\s+out\s+(?:of|from)\b|"
        r"\b(?:subawards?|subcontracts?|sub[- ]contracts?)\s+out\b)",
        q,
    ):
        return "outflow"
    value = str(proposed or "").strip().lower()
    return value if value in {"inflow", "outflow"} else "none"  # type: ignore[return-value]


def _flow_direction_from_filter_columns(columns: list[str]) -> FlowDirection | None:
    """Derive direction relative to a focused geography from its bound side.

    A recipient/``rcpt_*`` geography is the origin, so fixing it means the
    result describes outflow. A ``subawardee_*`` geography is the destination,
    so fixing it means inflow. This contract fact is more reliable than words
    such as "receives" when that verb describes the ranked counterpart rather
    than the named focus location.
    """
    geography_columns = [
        str(column).casefold()
        for column in columns
        if re.search(r"(?:state|cty|county|cd|district)", str(column), re.I)
    ]
    fixes_origin = any(
        column.startswith(("rcpt_", "origin_", "source_")) for column in geography_columns
    )
    fixes_destination = any(
        column.startswith(("subawardee_", "destination_", "dest_")) for column in geography_columns
    )
    if fixes_origin and not fixes_destination:
        return "outflow"
    if fixes_destination and not fixes_origin:
        return "inflow"
    return None


def _sort_direction(
    question: str,
    *,
    operation: Operation,
    proposed: Any,
) -> SortDirection:
    q = re.sub(r"\bat\s+(?:least|most)\b", "", question.lower())
    # In an explicit "from X to Y" ordering, the endpoints establish the
    # sequence. "From most/poorest/highest to least/lowest" is descending in
    # the selected measure; the reverse wording is ascending. Evaluate this
    # before isolated words such as "least", which otherwise invert the list.
    if re.search(
        r"\bfrom\b.*\b(?:most|poorest|richest|highest|largest)\b.*"
        r"\bto\b.*\b(?:least|lowest|smallest)\b",
        q,
    ):
        return "desc"
    if re.search(
        r"\bfrom\b.*\b(?:least|lowest|smallest)\b.*"
        r"\bto\b.*\b(?:most|poorest|richest|highest|largest)\b",
        q,
    ):
        return "asc"
    if re.search(r"\b(bottom|lowest|smallest|least|fewest|ascending)\b", q):
        return "asc"
    if re.search(r"\b(top|descending)\b", q):
        return "desc"
    if re.search(r"\b(?:liabilities?|liability)\s*(?:minus|-)\s*assets?\b", q):
        if re.search(r"\b(highest|largest|most|greatest)\b", q):
            return "desc"
    if re.search(
        r"\b(?:poorest|largest deficit|highest deficit|highest negative|most negative)\b",
        q,
    ):
        return "asc"
    if re.search(r"\bat\s+(?:least|most)\b", question.lower()):
        return "none"
    # Sorting is semantically meaningful only for a ranking. Rich polarity
    # phrases such as "poorest by net position" or "largest deficit" cannot be
    # reduced to a global word list: the direction depends on the selected
    # measure's meaning. Preserve the audited route brain's direction for those
    # cases instead of forcing every bare ranking to descending.
    if operation == "ranking":
        proposed_norm = str(proposed or "").strip().lower()
        if proposed_norm in {"asc", "desc"}:
            return proposed_norm  # type: ignore[return-value]
        if re.search(r"\b(highest|largest|most|greatest)\b", q):
            return "desc"
        return "desc"
    return "none"


def build_analysis_contract(question: str, routing: dict[str, Any]) -> AnalysisContract:
    years = [int(value) for value in _YEAR_RE.findall(question)]
    period_match = _PERIOD_RE.search(question)
    requested_period = f"{period_match.group(1)}-{period_match.group(2)}" if period_match else None
    has_typed_semantics = isinstance(routing.get("semantic_plan"), dict)
    semantics = semantic_plan_from_routing(routing)
    metric_columns = [str(x) for x in (routing.get("columns") or [])]

    if has_typed_semantics:
        # This is the normal runtime path. Meaning has already been selected by
        # the planner and, for complex/uncertain requests, audited against the
        # schema. Keep that one typed interpretation authoritative. Re-reading
        # the prose here with regexes created a competing deterministic brain
        # that changed valid multi-part comparisons into rankings and reversed
        # model-selected ordering.
        operation = semantics.operation
        top_k = semantics.top_k
        sort_direction = semantics.sort_direction
        flow_direction = semantics.flow_direction
        formula = semantics.formula
        if formula.operator != "none" and formula.operands:
            # Formula operands preserve semantic order and may intentionally
            # repeat a measure (for example MAX(x) - MIN(x) across rows). The
            # physical metric contract itself is a set of required columns.
            formula_metrics = list(dict.fromkeys(formula.operands))
            if formula.operator == "identity" or semantics.include_component_measures:
                metric_columns = list(dict.fromkeys([*formula_metrics, *metric_columns]))
            else:
                metric_columns = formula_metrics
    else:
        # Compatibility path for old fixtures and third-party callers. New
        # runtime routes always carry semantic_plan. Keep the prior behavior
        # until those callers migrate, but never use it to override a typed plan.
        top = _TOP_RE.search(question)
        proposed_top = routing.get("top_k")
        try:
            proposed_top_int = int(proposed_top) if proposed_top is not None else None
        except (TypeError, ValueError):
            proposed_top_int = None
        operation = _explicit_operation(question, routing.get("operation"))
        top_k = int(top.group(1)) if top else proposed_top_int
        if top_k is not None and not 1 <= top_k <= 250:
            top_k = None

        q_norm = re.sub(r"[^a-z0-9]+", " ", question.casefold()).strip()
        standard_net_position = bool(
            re.search(
                r"\bdifference (?:between|in|of) (?:total )?assets (?:and|versus|vs) "
                r"(?:total )?liabilit(?:y|ies)\b",
                q_norm,
            )
        )
        gov_question = any(str(table).startswith("gov_") for table in (routing.get("tables") or []))
        liabilities_minus_assets = bool(
            re.search(
                r"\b(?:total )?liabilit(?:y|ies) (?:minus|less) (?:total )?assets\b",
                q_norm,
            )
        )
        assets_minus_liabilities = bool(
            re.search(
                r"\b(?:total )?assets (?:minus|less) (?:total )?liabilit(?:y|ies)\b",
                q_norm,
            )
        )
        if gov_question and liabilities_minus_assets:
            metric_columns = ["Total_Liabilities", "Total_Assets"]
            formula = FormulaSpec(
                operator="subtract",
                operands=list(metric_columns),
                output_label="liabilities_minus_assets",
            )
        elif gov_question and assets_minus_liabilities:
            metric_columns = ["Total_Assets", "Total_Liabilities"]
            formula = FormulaSpec(
                operator="subtract",
                operands=list(metric_columns),
                output_label="assets_minus_liabilities",
            )
        elif gov_question and standard_net_position:
            metric_columns = ["Net_Position"]
            formula = FormulaSpec(
                operator="identity",
                operands=["Net_Position"],
                output_label="net_position",
            )
        elif (
            re.search(
                r"\b(?:as (?:a )?(?:percent|percentage|ratio) of|percent of|ratio of)\b",
                question,
                re.I,
            )
            and len(metric_columns) >= 2
        ):
            formula = FormulaSpec(
                operator="divide",
                operands=list(metric_columns[:2]),
                scale=100.0 if re.search(r"\bpercent(?:age)?\b", question, re.I) else 1.0,
            )
        elif re.search(
            r"(?:\bnet\b.*\b(?:subaward|subcontract|flow)|"
            r"\b(?:subaward|subcontract)\b.*\bnet\b)",
            question,
            re.I,
        ):
            formula = FormulaSpec(operator="net_flow")
        else:
            formula = FormulaSpec()
        sort_direction = _sort_direction(
            question,
            operation=operation,
            proposed=routing.get("sort_direction"),
        )
        flow_direction = _flow_direction(question, routing.get("flow_direction"))
        semantics = SemanticPlan(
            operation=operation,
            formula=formula,
            observation_grain=str(routing.get("geography_level") or "none"),
            result_scope=(
                "single"
                if top_k == 1
                else "top_n"
                if top_k is not None
                else "full"
                if operation == "ranking" and re.match(r"^\s*rank\b", question, re.I)
                else "unspecified"
            ),
            sort_direction=sort_direction,
            top_k=top_k,
            flow_direction=flow_direction,
            include_component_measures=not bool(re.match(r"^\s*express\b", question, re.I)),
        )

    # Assumptions shown to users must come from the catalog, not an unverified
    # free-text model field (which previously mislabeled ACS percentages as counts).
    assumptions: list[str] = []
    analysis_tables = _with_required_bridge_tables(list(routing.get("tables") or []))
    effective_by_table = resolve_periods_by_table(
        analysis_tables,
        years,
        requested_period,
        str(routing.get("year_strategy") or ""),
        operation=operation,
    )
    if len(effective_by_table) == 1:
        effective_period: Any = next(iter(effective_by_table.values()))
    else:
        effective_period = effective_by_table or None
    if not years and operation != "trend":
        for table in routing.get("tables") or []:
            dataset = get_dataset(str(table))
            if dataset is not None and dataset.default_year is not None:
                assumptions.append(
                    f"{table} uses its catalog default period {dataset.default_year}."
                )
    if requested_period:
        assumptions.append(
            f"Uses the catalog's precomputed {requested_period} summary row where available; "
            "this is not assumed to be a sum of annual rows."
        )
    is_flow_query = any(str(table).endswith("_flow") for table in (routing.get("tables") or []))
    if is_flow_query:
        bound_direction = _flow_direction_from_filter_columns(
            [str(column) for column in (routing.get("filter_columns") or [])]
        )
        if bound_direction is not None:
            flow_direction = bound_direction
    sort_columns = list(semantics.sort_columns)
    if (
        operation == "ranking"
        and formula.operator in {"none", "identity"}
        and not sort_columns
        and metric_columns
    ):
        # Older typed fixtures did not distinguish rank keys from accompanying
        # display measures. Defaulting to the first measure retains their
        # intended primary ranking without re-reading the user's prose.
        sort_columns = [metric_columns[0]]
    return AnalysisContract(
        tables=analysis_tables,
        metric_columns=metric_columns,
        geography_level=str(routing.get("geography_level") or "none"),
        operation=operation,
        flow_direction=flow_direction if is_flow_query else "none",
        explicit_year=years[-1] if years else None,
        requested_years=years,
        requested_period=requested_period,
        period_by_table=effective_by_table,
        effective_period=effective_period,
        year_strategy=str(routing.get("year_strategy") or ""),
        sort_direction=sort_direction,
        sort_columns=sort_columns,
        top_k=top_k,
        join_plan=str(routing.get("join_plan") or ""),
        assumptions=assumptions,
        statistic=semantics.statistic,
        result_unit=semantics.result_unit,
        formula=formula,
        predicate=semantics.predicate,
        observation_grain=semantics.observation_grain,
        result_scope=semantics.result_scope,
        include_component_measures=semantics.include_component_measures,
        output_dimensions=list(semantics.output_dimensions),
    )
