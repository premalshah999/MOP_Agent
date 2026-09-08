"""Independent schema-grounded audit of analysis plans.

The fast fused router sees every table, but only a compact view of each metric.
It can therefore reject a valid paraphrase before detailed schema retrieval ever
runs.  This module gives a second LLM call a description-rich index of every
physical runtime measure and asks it to audit—not merely repeat—the first
decision.  It contains no query-specific answer rules and never executes SQL.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

from app.llm import client
from app.semantic.discovery import discover_metrics
from app.semantic.registry import load_registry, semantic_catalog_for_verification
from app.semantic.value_resolver import resolve_filter_values

_SYSTEM = """You are the independent route-verification gate for a public-policy
analytics assistant. Audit the INITIAL DECISION against the authoritative
RUNTIME SCHEMA INDEX below.

Your job is to prevent both false refusals and plausible-but-wrong routes.

Rules:
- Interpret ordinary language by meaning, not only exact column spelling. A
  user's phrase is supported when a runtime measure's label, aliases, or
  description clearly represents the same concept.
- Use only exact table ids and exact measure column names from the index.
- Select the geography-specific table that matches the request. If geography
  is omitted, use state level.
- A correlation between separately loaded numeric measures across rows is
  supported. It is NOT a request for a joint demographic subgroup. A subgroup
  intersection (for example, Hispanic people who have bachelor's degrees) is
  unsupported unless an exact joint field exists.
- A cross-sectional correlation across geographies is one statistic. It cannot
  identify a state/county/district with the "highest correlation" when each
  geography has only one selected-period observation. CLARIFY overall
  correlation versus a geography ranking/ratio in that case.
- For a one-year cross-sectional correlation WITHIN one named state, the state
  is the scope/filter and counties are the repeated observations. Use the
  compatible *_county table. A selected-year *_state table has one row for that
  state and can only return NULL; reserve it for correlations across states or
  an explicitly requested multi-year state time series.
- Preserve genuinely unsupported requests. Do not approximate missing sex,
  age, education, crime, employment, or other fields with nearby variables.
- If the user explicitly names an exact runtime dataset, do not switch to a
  sibling dataset to supply a field absent from that named dataset. A clear
  typo of a dataset id may be corrected. A documented derived measure may add
  a second same-geography table while retaining the named table; that is not a
  dataset substitution.
- Semantic equivalence must be exact enough to preserve the requested measure:
  Employees (people working) is not Federal Residents (people residing), prime
  Contracts are not subaward/subcontract flows, Revenue is not funding received,
  and a percentage is not a count. Never recover a question by changing one of
  those concepts.
- An explicit "by", "on", or "in terms of" measure governs nearby
  superlatives. Highest/richest and lowest/poorest geographies on one named
  measure is an extrema comparison of that measure, expressed as derived
  subtract [M,M]. It is ANALYTICAL; do not reopen income or another unrelated
  meaning for the adjectives.
- For an unqualified "where does X rank?", use the transparent convention that
  the highest measure value is rank 1 (sort_direction=desc). Do not reverse the
  order based on an unstated better/worse interpretation of the measure.
- For every ranking, `semantic_plan.sort_columns` contains only the exact
  measures that control row order, in priority order. Preserve multiple keys
  when the user ranks "by X and Y", but keep an accompanying "and their Y"
  measure out of sort_columns. Never silently sum, weight, or reorder keys.
- A county/district compared with its "state median" uses the median across
  represented counties/districts inside that state at the same grain and
  period. A state-table row is a statewide aggregate, not that median.
- Government-finance direction follows the expression: lowest Net_Position is
  the largest deficit, while the largest Liabilities - Assets is the same
  ordering expressed with the sign reversed. Accept either mathematically
  equivalent framing; do not reject a valid ascending ranking merely because
  the question also contains "largest" or "highest negative". An unqualified
  "difference between/in assets and liabilities" uses the documented
  Net_Position direction (Assets - Liabilities), never the reversed expression.
  When the user explicitly orders the operands (for example, Liabilities minus
  Assets), retain both exact operand columns in metric_columns; do not replace
  them with Net_Position because the downstream contract must preserve the
  requested output sign.
- A request for broad "federal spending", "federal money", or "direct federal
  spending" is CLARIFY unless the user names a specific channel. Never invent a
  composite by summing Contracts, Grants, Direct Payments, wages, or workforce
  fields that the user did not enumerate.
- State-level federal per-1,000-resident measures use spending_state. County or
  congressional true per-capita federal measures are unsupported because the
  stored denominator is unverified.
- Agency-level state federal spending uses spending_state_agency. County and
  congressional agency-spending tables are not loaded. Therefore a request for
  prime Contracts or Grants by agency at county/congressional grain is
  UNANSWERABLE. Do not relabel county/congress subaward flow as prime contracts.
- For raw state federal award/workforce totals without an agency breakdown, use
  contract_state. Use spending_state only for published per-1,000-resident
  measures.
- Flow direction matters: inflow is the subawardee/destination side; outflow is
  the prime-recipient/origin side.
- County-flow integer keys are county FIPS identifiers: join rcpt_cty for
  origin/outflow or subawardee_cty for destination/inflow directly to
  gov_county/acs_county/finra_county.fips or contract_county.county_fips.
  Never reject such a join merely because county_flow names the key rcpt_cty
  or subawardee_cty rather than fips.
- A ranking of geographies that "receive" inflow must output the subawardee
  geography; a ranking of geographies that "send" outflow must output the
  rcpt/origin geography. The opposite side is valid only when the user asks
  for sources/origins or destinations explicitly.
- Net subaward flow means destination inflow MINUS origin outflow for the same
  place. It requires both sides and a subtraction; it is not ordinary inflow.
- The source dictionary supports these cross-table subaward derivations at the
  same geography: Sub-contract Out = origin-side sum; Sub-Contract In =
  destination-side sum; Net Sub-Contract = inflow - outflow. Ratios using
  those quantities are supported when contract_* and *_flow have compatible
  geography and period.
- Federal Contracts (Indirect) is currently UNANSWERABLE. The newest dictionary
  calls it a separate `fed_act_obl_indirect` source field; an older dictionary
  gives a conflicting Contracts + Net Sub-Contract formula. The physical field
  is not loaded, so never infer or synthesize it.
- FINRA shares/indices cannot be turned into resident counts. No respondent
  count, population denominator, or survey weights are loaded; multiplying a
  FINRA share such as satisfied by ACS population is unsupported.
- ACS fields Age 18-65, White, Black, Asian, and Hispanic are percentages of
  Total population. A requested headcount for one of these fields is supported
  as `Total population * percentage * 0.01`; route both exact operands, set
  formula.operator=multiply, formula.scale=0.01, and result_unit=persons.
  Do not extend this derivation to education, poverty, owner/renter, or FINRA
  fields because their matching count denominator is not loaded.
  The word "population" alone does not request a headcount: ACS demographic
  fields are commonly described as population shares. Convert to persons only
  when the user explicitly requests a count, headcount, number of people, or
  "how many." Never multiply two demographic percentage fields together;
  they are separate marginals, not a joint subgroup.
- Cross-source questions may intentionally use different periods. ACS 2023 +
  FINRA 2021 is supported: assign periods per table, filter each separately,
  and join only on the geography key.
- state_flow has no period column. Never silently divide or combine its
  all-records snapshot with a default 2024 contract/workforce measure. A
  state-level ratio or per-employee calculation must CLARIFY that
  mixed-scope limitation unless the user explicitly accepts it.
- Do not claim a period is supported unless the selected runtime table lists
  it. Do not call the federal 2020-2024 summary a five-year sum.
- CLARIFY only when two materially different supported measures remain equally
  plausible. Prefer ANALYTICAL when one documented measure is the clear
  semantic match.
- An unqualified peer comparison uses all represented geographies at the same
  grain and selected period. When the entity and exact measure are clear, that
  default is ANALYTICAL and does not require a user-defined peer cohort.
- Do not turn a complete executable ANALYTICAL plan into CLARIFY merely because
  more than one calculation or ranking convention could be reasonable. When
  the selected tables and measures do not change, retain the initial plan and
  state its transparent method in the answer. CLARIFY only when the unresolved
  choice changes the measure, table, population, geography, or requested unit.
- Keep META and OUT_OF_SCOPE decisions when they are appropriate.
- Before returning, explicitly check: meaning, exact metric availability,
  geography, period, operation, and flow direction.
- Return a complete `semantic_plan`. It is the single authoritative meaning
  contract used downstream. Preserve formula operand order, observation grain,
  ranking scope, requested output dimensions, and whether component measures
  were requested. A ranking with top_k=1 has result_scope=single. If you
  correct the route, correct semantic_plan with it.
- Preserve explicit row predicates. "Geographies where A is higher than B"
  uses predicate gt [A,B] and operation=comparison; it is not a ranking unless
  the user also asks to rank, sort, top, or bottom.
- A selected-period value for one named geography from an already aggregated
  geography table is operation=lookup and statistic=value. Use aggregate/sum
  for event rows or when multiple physical rows really must be combined; the
  words "how much" or "how many dollars" alone do not require SUM over an
  already aggregated cell.
- A request for a dataset-wide total/actual number of a metric with no named
  geography and no requested breakdown combines geography rows using the
  metric's documented aggregation. It is aggregate + single, not a lookup
  table and not a physical row count.
- A correlation across one observation population has result_scope=single,
  even when several coefficient columns are requested. Use grouped only for
  a separate coefficient per explicit output dimension, and include that
  dimension in output_dimensions.

Examples that define the boundary:
- "benefit transfers to Maryland" -> ANALYTICAL contract_state, Direct Payments.
- "financial wellbeing by state" -> ANALYTICAL finra_state, satisfied.
- "employees in contract_county" -> UNANSWERABLE because Employees is absent;
  do not substitute Federal Residents.
- "top counties receiving Defense contracts" -> UNANSWERABLE because no loaded
  county table contains both prime Contracts and an agency dimension; county_flow
  contains subawards, which are different.
- "Sub-contract Out as a percent of Federal Contracts for Nevada counties" ->
  ANALYTICAL contract_county + county_flow, Contracts + subaward_amount.
- "Federal Contracts (Indirect) for counties in 2024" -> UNANSWERABLE.
- "how many satisfied residents are in Prince George County" -> UNANSWERABLE;
  offer the represented county's satisfied share instead.
- "2023 median household income versus 2021 financial literacy across states"
  -> ANALYTICAL acs_state + finra_state with separate period filters.
- "direct federal spending in Maryland" -> CLARIFY; ask which documented channel.

AUTHORITATIVE RUNTIME SCHEMA INDEX
==================================
{semantic_catalog}

Return ONLY JSON with this exact shape:
{{"intent":"ANALYTICAL|CLARIFY|UNANSWERABLE|META|OUT_OF_SCOPE",
  "requires_sql":<true only for ANALYTICAL>,
  "needs_clarification":<true only for CLARIFY>,
  "clarification_question":"",
  "reason":"short evidence-grounded reason",
  "tables":["exact_table_id"],
  "metric_columns":["Exact column"],
  "filter_columns":["exact dimension when known"],
  "geography_level":"state|county|congress|none",
  "operation":"lookup|ranking|comparison|trend|correlation|distribution|aggregate|breakdown",
  "flow_direction":"inflow|outflow|none",
  "sort_direction":"asc|desc|none",
  "top_k":null,
  "year_strategy":"supported period or no year filter",
  "join_plan":"",
  "semantic_plan":{{
    "operation":"lookup|ranking|comparison|trend|correlation|distribution|aggregate|breakdown",
    "statistic":"value|sum|average|median|count|count_distinct|correlation|distribution|derived|unspecified",
    "result_unit":"usd|persons|percent|ratio|count|correlation|index|value|unspecified",
    "formula":{{"operator":"none|identity|add|subtract|multiply|divide|net_flow","operands":["Exact column"],"scale":1.0,"output_label":""}},
    "predicate":{{"operator":"none|gt|gte|lt|lte|eq|neq","operands":["Exact column"],"comparison_value":null}},
    "observation_grain":"state|county|congress|year|row|none",
    "result_scope":"single|top_n|full|grouped|unspecified",
    "sort_direction":"asc|desc|none",
    "sort_columns":["Exact ranking metric in priority order"],
    "top_k":null,
    "flow_direction":"inflow|outflow|none",
    "include_component_measures":false,
    "output_dimensions":["exact dimension"]
  }},
  "confidence":"high|medium|low"}}"""


def verification_enabled() -> bool:
    return os.getenv("ROUTE_VERIFICATION_MODE", "always").strip().casefold() not in {
        "0",
        "false",
        "off",
        "disabled",
    }


def should_verify(initial: dict[str, Any]) -> bool:
    if not verification_enabled():
        return False
    intent = str(initial.get("intent") or "").strip().upper()
    if intent in {"CLARIFY", "UNANSWERABLE"}:
        return True
    if intent != "ANALYTICAL":
        return False
    # During migration, old providers/fixtures without typed semantics still
    # receive the full audit. Once a complete plan exists, a second LLM opinion
    # is reserved for genuinely complex or uncertain analyses instead of every
    # easy lookup.
    semantic_plan = initial.get("semantic_plan")
    if not isinstance(semantic_plan, dict):
        return True
    confidence = str(initial.get("confidence") or "medium").casefold()
    operation = str(semantic_plan.get("operation") or initial.get("operation") or "").casefold()
    formula = semantic_plan.get("formula")
    formula_operator = (
        str(formula.get("operator") or "none").casefold() if isinstance(formula, dict) else "none"
    )
    tables = [str(table) for table in (initial.get("tables") or [])]
    # Percentage/share datasets carry the highest unit and denominator risk.
    # Give them an independent schema-grounded semantic audit even when the
    # first model is confident. The verifier still reasons; it does not write
    # SQL or choose a numeric answer.
    high_risk_statistical_family = any(table.startswith(("acs_", "finra_")) for table in tables)
    return (
        confidence != "high"
        or high_risk_statistical_family
        or len(initial.get("tables") or []) > 1
        or operation in {"correlation", "trend"}
        or formula_operator not in {"", "none", "identity"}
    )


def _norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value).casefold()).strip()


def _strong_requested_metrics(question: str) -> set[str]:
    return {
        match.concept.variable
        for match in discover_metrics(question, limit=12)
        if match.score >= 0.94
    }


def _explicit_table(question: str) -> str | None:
    for table in load_registry().datasets:
        if re.search(rf"(?<![A-Za-z0-9_]){re.escape(table)}(?![A-Za-z0-9_])", question, re.I):
            return table
    return None


def _geography_matches(dataset_geography: str, requested: str) -> bool:
    left = _norm(dataset_geography)
    right = _norm(requested)
    if right == "congress":
        return "congress" in left or "district" in left
    return right in left


def _metric_unit_family(unit: str) -> str:
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


def _predicate_has_incompatible_units(
    plan: dict[str, Any],
    tables: list[str],
) -> bool:
    """Return true for ordered comparisons between unlike measure units."""

    predicate = plan.get("predicate")
    if not isinstance(predicate, dict) or str(predicate.get("operator") or "none") == "none":
        return False
    operands = [str(value) for value in (predicate.get("operands") or [])]
    if len(operands) != 2:
        return False
    registry = load_registry()
    operand_families: list[set[str]] = []
    for operand in operands:
        families = {
            _metric_unit_family(registry.datasets[table].metrics[operand].unit)
            for table in tables
            if table in registry.datasets and operand in registry.datasets[table].metrics
        }
        operand_families.append(families)
    return bool(
        operand_families[0]
        and operand_families[1]
        and operand_families[0].isdisjoint(operand_families[1])
    )


def _within_state_correlation_route(
    question: str,
    audited: dict[str, Any],
    *,
    explicit_table: str | None,
) -> dict[str, Any] | None:
    """Use county observations for a one-period correlation within a state.

    This is a schema capability invariant, not an interpretation shortcut: one
    selected state row cannot produce a correlation. The adjustment is made
    only when an exact named state is present and a same-family county table
    physically contains every requested metric.
    """
    if explicit_table or str(audited.get("intent") or "").upper() != "ANALYTICAL":
        return None
    semantic_plan = audited.get("semantic_plan")
    planned_operation = (
        str(semantic_plan.get("operation") or "").casefold()
        if isinstance(semantic_plan, dict)
        else str(audited.get("operation") or "").casefold()
    )
    if planned_operation != "correlation":
        return None
    if isinstance(semantic_plan, dict):
        if str(semantic_plan.get("observation_grain") or "").casefold() == "year":
            return None
    elif re.search(
        r"\b(?:over time|across years|by year|year[- ]over[- ]year|time series|trend)\b",
        question,
        re.I,
    ):
        return None
    tables = [str(table) for table in (audited.get("tables") or [])]
    if len(tables) != 1 or not tables[0].endswith("_state"):
        return None
    state_table = tables[0]
    county_table = state_table.removesuffix("_state") + "_county"
    registry = load_registry()
    state_dataset = registry.datasets.get(state_table)
    county_dataset = registry.datasets.get(county_table)
    if state_dataset is None or county_dataset is None:
        return None
    state_column = next(
        (
            column
            for column in ("state", "state_name")
            if column in state_dataset.columns and column in county_dataset.columns
        ),
        None,
    )
    if state_column is None:
        return None
    exact_states = [
        value
        for value, score in resolve_filter_values(state_table, state_column, question)
        if score >= 0.99
    ]
    if len(exact_states) != 1:
        return None
    metrics = {
        str(metric) for metric in (audited.get("metric_columns") or audited.get("columns") or [])
    }
    if not metrics or not metrics.issubset(set(county_dataset.metrics)):
        return None
    adjusted = dict(audited)
    adjusted["tables"] = [county_table]
    adjusted["geography_level"] = "county"
    filter_columns = [str(value) for value in (audited.get("filter_columns") or [])]
    if state_column not in filter_columns:
        filter_columns.append(state_column)
    adjusted["filter_columns"] = filter_columns
    semantic_plan = adjusted.get("semantic_plan")
    if isinstance(semantic_plan, dict):
        semantic_plan = dict(semantic_plan)
        semantic_plan["observation_grain"] = "county"
        adjusted["semantic_plan"] = semantic_plan
    adjusted["reason"] = (
        f"The correlation is scoped to {exact_states[0]}; {county_table} supplies "
        "the repeated county observations needed for a one-period coefficient."
    )
    return adjusted


def _schema_capable_cross_table_route(route: dict[str, Any]) -> bool:
    """Whether an existing analytical route has physical metrics and join keys.

    This does not infer intent, choose tables, or create a formula. It only
    checks a plan already produced by the primary language planner against the
    loaded registry so a second model cannot falsely claim a documented join
    is physically impossible.
    """
    if str(route.get("intent") or "").upper() != "ANALYTICAL":
        return False
    registry = load_registry()
    tables = [str(value) for value in (route.get("tables") or [])]
    if len(tables) < 2 or any(table not in registry.datasets for table in tables):
        return False
    geography = str(route.get("geography_level") or "none")
    if geography not in {"state", "county", "congress"}:
        return False
    if any(
        not _geography_matches(registry.datasets[table].geography, geography) for table in tables
    ):
        return False
    metrics = {str(value) for value in (route.get("metric_columns") or route.get("columns") or [])}
    available = {metric for table in tables for metric in registry.datasets[table].metrics}
    if not metrics or not metrics.issubset(available):
        return False

    if geography == "state":
        return all(
            {name.casefold() for name in registry.datasets[table].dimensions}
            & {"state", "state_name", "rcpt_state_name", "subawardee_state_name"}
            for table in tables
        )
    if geography == "county":
        county_keys = {
            "fips",
            "county_fips",
            "rcpt_cty",
            "subawardee_cty",
        }
        return all(
            {name.casefold() for name in registry.datasets[table].columns} & county_keys
            for table in tables
        )
    # Congressional flow joins require a bridge/canonicalization step and are
    # audited separately; direct district-code joins remain safely detectable.
    return all("cd_118" in registry.datasets[table].columns for table in tables)


def _reconcile_with_schema(
    question: str,
    initial: dict[str, Any],
    audited: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    """Block only schema-impossible recovery; never select an answer.

    This is a capability check over the registry. The LLM still interprets the
    language, but it cannot rescue a route by replacing an exact requested
    metric or by inventing a table that lacks a required dimension.
    """
    registry = load_registry()
    explicit = _explicit_table(question)
    discovery_question = (
        re.sub(re.escape(explicit), " ", question, flags=re.I) if explicit else question
    )
    strong_metrics = _strong_requested_metrics(discovery_question)

    if explicit and str(audited.get("intent") or "").upper() == "ANALYTICAL" and strong_metrics:
        audited_tables = [
            table for table in (audited.get("tables") or []) if table in registry.datasets
        ]
        # Retaining the explicitly named table while adding another table of
        # the same geography is a valid cross-dataset derivation. Replacing it
        # is still blocked. Metric capability is checked over the complete
        # audited route, so a missing concept cannot be silently substituted.
        if explicit not in audited_tables:
            available = set(registry.datasets[explicit].metrics)
        else:
            explicit_geo = registry.datasets[explicit].geography
            compatible_tables = [
                table
                for table in audited_tables
                if _geography_matches(registry.datasets[table].geography, explicit_geo)
                or _geography_matches(explicit_geo, registry.datasets[table].geography)
            ]
            available = {
                metric for table in compatible_tables for metric in registry.datasets[table].metrics
            }
        absent = sorted(strong_metrics - available)
        if explicit not in audited_tables or absent:
            blocked = dict(initial)
            blocked.update(
                intent="UNANSWERABLE",
                requires_sql=False,
                needs_clarification=False,
                clarification_question="",
                tables=[explicit],
                metric_columns=[],
                reason=(
                    f"The route anchored on {explicit} cannot supply the requested runtime measure(s): "
                    f"{', '.join(absent)}. A different field cannot be substituted."
                ),
            )
            return blocked, "blocked exact-dataset metric substitution"

    initial_intent = str(initial.get("intent") or "").upper()
    audited_intent = str(audited.get("intent") or "").upper()
    audited_metrics = set(audited.get("metric_columns") or audited.get("columns") or [])
    if initial_intent == "CLARIFY" and audited_intent == "ANALYTICAL" and len(audited_metrics) > 1:
        if not audited_metrics.issubset(strong_metrics):
            return initial, "blocked unrequested composite measure"

    # A named agency/department is a required dimension, not optional prose.
    # Validate that at least one runtime table can satisfy the requested
    # geography + strong metric(s) + agency dimension intersection.
    semantic_plan = audited.get("semantic_plan")
    if isinstance(semantic_plan, dict):
        requested_dimensions = {
            str(value).casefold()
            for value in (
                list(audited.get("filter_columns") or [])
                + list(semantic_plan.get("output_dimensions") or [])
            )
        }
        asks_agency = bool(requested_dimensions & {"agency", "agency_name", "agency_code"})
    else:
        asks_agency = bool(
            re.search(r"\b(?:agency|agencies|department|departments)\b", question, re.I)
        )
    geography = str(audited.get("geography_level") or initial.get("geography_level") or "none")
    if asks_agency and geography in {"state", "county", "congress"} and strong_metrics:

        def route_supports_agency_analysis(route: dict[str, Any]) -> bool:
            if str(route.get("intent") or "").upper() != "ANALYTICAL":
                return False
            route_tables = [
                str(table)
                for table in (route.get("tables") or [])
                if str(table) in registry.datasets
                and _geography_matches(registry.datasets[str(table)].geography, geography)
            ]
            if not route_tables:
                return False
            available = {
                metric for table in route_tables for metric in registry.datasets[table].metrics
            }
            agency_tables = [
                table
                for table in route_tables
                if {name.casefold() for name in registry.datasets[table].dimensions}
                & {"agency", "agency_name", "agency_code"}
            ]
            route_metrics = {
                str(metric)
                for metric in (route.get("metric_columns") or route.get("columns") or [])
            }
            required_metrics = route_metrics or strong_metrics
            # The agency table must supply at least one requested measure (the
            # agency-specific numerator); remaining measures may come from a
            # same-geography table joined on the documented geographic key.
            return required_metrics.issubset(available) and any(
                required_metrics & set(registry.datasets[table].metrics) for table in agency_tables
            )

        if route_supports_agency_analysis(initial) and not route_supports_agency_analysis(audited):
            return initial, "preserved schema-capable cross-table agency route"

        compatible_datasets = [
            dataset
            for dataset in registry.datasets.values()
            if _geography_matches(dataset.geography, geography)
        ]
        available = {metric for dataset in compatible_datasets for metric in dataset.metrics}
        agency_datasets = []
        for dataset in compatible_datasets:
            dimensions = {name.casefold() for name in dataset.dimensions}
            has_agency = "agency" in dimensions or "agency_name" in dimensions
            if has_agency:
                agency_datasets.append(dataset)
        agency_metric_available = any(
            strong_metrics & set(dataset.metrics) for dataset in agency_datasets
        )
        if not strong_metrics.issubset(available) or not agency_metric_available:
            blocked = dict(initial)
            blocked.update(
                intent="UNANSWERABLE",
                requires_sql=False,
                needs_clarification=False,
                clarification_question="",
                tables=[],
                metric_columns=[],
                reason=(
                    "No loaded runtime table contains the requested measure(s), "
                    f"{geography} geography, and an agency dimension together."
                ),
            )
            return blocked, "blocked impossible metric/geography/agency intersection"

    # Separate ACS percentages are marginal distributions. A verifier must
    # never turn a multi-measure comparison into a synthetic joint subgroup by
    # multiplying two shares. This is a schema/statistical invariant, so retain
    # the initial typed interpretation rather than forwarding an impossible
    # correction to SQL generation.
    audited_plan = audited.get("semantic_plan")
    audited_tables = [
        str(table) for table in (audited.get("tables") or []) if str(table) in registry.datasets
    ]
    initial_plan = initial.get("semantic_plan")
    if isinstance(audited_plan, dict) and _predicate_has_incompatible_units(
        audited_plan, audited_tables
    ):
        initial_predicate = (
            initial_plan.get("predicate") if isinstance(initial_plan, dict) else None
        )
        if (
            not isinstance(initial_predicate, dict)
            or str(initial_predicate.get("operator") or "none").casefold() == "none"
        ):
            return initial, "blocked row predicate across incompatible metric units"

    if isinstance(audited_plan, dict) and any(table.startswith("acs_") for table in audited_tables):
        formula = audited_plan.get("formula")
        if (
            isinstance(formula, dict)
            and str(formula.get("operator") or "none").casefold() == "multiply"
        ):
            percent_operands: set[str] = set()
            for operand in formula.get("operands") or []:
                for table in audited_tables:
                    metric = registry.datasets[table].metrics.get(str(operand))
                    if metric is not None and metric.unit.casefold() == "percent":
                        percent_operands.add(metric.id)
            if len(percent_operands) > 1:
                return initial, "blocked multiplication of marginal ACS percentages"

    # The first planner owns language interpretation. The verifier audits that
    # plan against richer schema descriptions, but it must not become a second
    # competing brain that silently changes an explicit output unit. It may
    # still repair tables, columns, formulas, and an unspecified unit.
    if isinstance(initial_plan, dict) and isinstance(audited_plan, dict):
        initial_unit = str(initial_plan.get("result_unit") or "unspecified").casefold()
        audited_unit = str(audited_plan.get("result_unit") or "unspecified").casefold()
        if (
            str(initial.get("intent") or "").upper() == "ANALYTICAL"
            and str(audited.get("intent") or "").upper() == "ANALYTICAL"
            and initial_unit != "unspecified"
            and audited_unit != initial_unit
        ):
            return initial, "preserved authoritative typed result unit"

        # A schema auditor cannot veto an otherwise complete executable plan
        # while affirming the exact same tables, measures, and typed semantics.
        # Such a response is internally inconsistent: it expresses no schema
        # or capability correction, only a second stylistic interpretation of
        # the user's words. Preserve the first planner as the sole language
        # authority. A genuine ambiguity remains free to clarify by returning
        # a different/incomplete plan or a different capability route.
        complete_fields = {
            "operation": {
                "lookup",
                "ranking",
                "comparison",
                "trend",
                "correlation",
                "distribution",
                "aggregate",
                "breakdown",
            },
            "statistic": {
                "value",
                "sum",
                "average",
                "median",
                "count",
                "count_distinct",
                "correlation",
                "distribution",
                "derived",
            },
            "result_unit": {
                "usd",
                "persons",
                "percent",
                "ratio",
                "count",
                "correlation",
                "index",
                "value",
            },
            "result_scope": {"single", "top_n", "full", "grouped"},
        }
        complete_initial_plan = (
            all(
                str(initial_plan.get(field) or "").casefold() in allowed
                for field, allowed in complete_fields.items()
            )
            and str(initial_plan.get("observation_grain") or "none").casefold() != "none"
        )
        same_tables = set(initial.get("tables") or []) == set(audited.get("tables") or [])
        same_metrics = set(initial.get("metric_columns") or initial.get("columns") or []) == set(
            audited.get("metric_columns") or audited.get("columns") or []
        )
        if (
            initial_intent == "ANALYTICAL"
            and audited_intent == "CLARIFY"
            and bool(initial.get("tables"))
            and bool(initial.get("metric_columns") or initial.get("columns"))
            and complete_initial_plan
            and initial_plan == audited_plan
            and same_tables
            and same_metrics
        ):
            return initial, "preserved complete analytical plan without a schema disagreement"
    adjusted = _within_state_correlation_route(
        question,
        audited,
        explicit_table=explicit,
    )
    if adjusted is not None:
        return adjusted, "used county observations for within-state correlation"
    if audited_intent in {"CLARIFY", "UNANSWERABLE"} and _schema_capable_cross_table_route(initial):
        return initial, "preserved primary route with documented cross-table keys"
    return audited, ""


def verify_route(
    question: str,
    initial: dict[str, Any],
    candidate_tables: set[str] | list[str] | tuple[str, ...] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return ``(decision, audit_metadata)``; fail open to the initial route.

    "Fail open" here means pipeline availability, not answer permissiveness: if
    the verifier provider is unavailable, the original router decision remains
    subject to the existing schema, SQL, and faithfulness gates.
    """
    if not should_verify(initial):
        return initial, {}
    verification_tables = set(candidate_tables or ()) | {
        str(table) for table in (initial.get("tables") or [])
    }
    system = _SYSTEM.format(
        semantic_catalog=semantic_catalog_for_verification(verification_tables or None)
    )
    user = (
        f"USER QUESTION\n{question}\n\n"
        f"INITIAL DECISION\n{json.dumps(initial, default=str)}\n\n"
        "Audit the decision and return the corrected final route."
    )
    try:
        audited = client.chat_json(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
            max_tokens=900,
            purpose="route_verification",
        )
    except client.LLMError as exc:
        return initial, {
            "performed": False,
            "changed": False,
            "available": False,
            "reason": str(exc)[:160],
        }
    if not isinstance(audited, dict):
        return initial, {
            "performed": False,
            "changed": False,
            "available": False,
            "reason": "verifier returned a non-object response",
        }

    merged = {**initial, **audited}
    merged, reconciliation = _reconcile_with_schema(question, initial, merged)
    before = {
        "intent": str(initial.get("intent") or "").upper(),
        "tables": list(initial.get("tables") or []),
        "metric_columns": list(initial.get("metric_columns") or initial.get("columns") or []),
        "semantic_plan": initial.get("semantic_plan"),
    }
    after = {
        "intent": str(merged.get("intent") or "").upper(),
        "tables": list(merged.get("tables") or []),
        "metric_columns": list(merged.get("metric_columns") or merged.get("columns") or []),
        "semantic_plan": merged.get("semantic_plan"),
    }
    return merged, {
        "performed": True,
        "changed": before != after,
        "available": True,
        "initial": before,
        "final": after,
        "reason": str(audited.get("reason") or "").strip()[:240],
        "reconciliation": reconciliation,
    }
