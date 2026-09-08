"""Production intent, dataset-routing, and semantic-planning entry point.

One structured model call keeps intent and routing decisions aligned while
avoiding an extra provider round trip. The production stage tests exercise this
same ``classify_and_route`` function. Non-analytical intents return empty
routing fields for the pipeline's clarification or catalog-aware responder.
"""

from __future__ import annotations

import re
from typing import Any

from app.core.analysis_plan import resolve_periods_by_table, semantic_plan_from_routing
from app.core.plan_verifier import verify_route
from app.llm import client
from app.semantic.discovery import discover_metrics
from app.semantic.registry import (
    catalog_for_prompt,
    domain_summary,
    get_dataset,
    load_registry,
)

INTENTS = {"ANALYTICAL", "CLARIFY", "UNANSWERABLE", "META", "OUT_OF_SCOPE"}
_VALID_TABLES = set(load_registry().datasets)


def _candidate_tables_for_question(question: str) -> set[str] | None:
    """Retrieve a compact, high-recall schema neighborhood for the planner.

    This function never selects the route. It only limits detailed prompt
    context to tables whose documented measures and dimensions are relevant;
    the language model remains responsible for interpretation and can still
    see every table id in the domain summary.
    """
    registry = load_registry()
    q = question.casefold()
    if "congress" in q or re.search(r"\bdistricts?\b|\b[A-Z]{2}-\d{1,2}\b", question):
        geography = "congress"
    elif re.search(r"\bcount(?:y|ies)\b", q):
        geography = "county"
    else:
        geography = "state"

    hinted_families: set[str] = set()
    if re.search(r"\b(?:acs|census|demograph\w*|poverty|household|population)\b", q):
        hinted_families.add("demographics")
    if re.search(r"\b(?:finra|financial literacy|risk aversion|financial satisfaction)\b", q):
        hinted_families.add("financial_health")
    if re.search(
        r"\b(?:government debt|government finance|liabilit\w*|assets?|revenue|expenses?|"
        r"net position|cash flow|pension|opeb)\b",
        q,
    ):
        hinted_families.add("government_finance")
    if re.search(
        r"\b(?:federal|contracts?|grants?|direct payments?|employees?|resident wages?)\b", q
    ):
        hinted_families.add("federal_funding")
    if re.search(r"\b(?:subawards?|subcontracts?|inflows?|outflows?)\b", q):
        hinted_families.add("subaward_flow")

    def geography_matches(value: str) -> bool:
        normalized = value.casefold()
        if geography == "congress":
            return "congress" in normalized or "district" in normalized
        return geography in normalized

    selected = {
        table
        for table in registry.datasets
        if re.search(rf"(?<![A-Za-z0-9_]){re.escape(table)}(?![A-Za-z0-9_])", question, re.I)
    }
    for match in discover_metrics(question, limit=16):
        if match.score < 0.76:
            continue
        if hinted_families and match.concept.family not in hinted_families:
            continue
        for dataset in registry.datasets.values():
            if match.concept.variable in dataset.metrics and geography_matches(dataset.geography):
                selected.add(dataset.id)

    if re.search(r"\b(?:agency|agencies|department|departments|dod|hhs)\b", q):
        selected.update(
            dataset.id
            for dataset in registry.datasets.values()
            if geography_matches(dataset.geography)
            and (not hinted_families or dataset.family in hinted_families)
            and {name.casefold() for name in dataset.dimensions}
            & {"agency", "agency_name", "agency_code"}
        )
    if re.search(r"\b(?:subawards?|subcontracts?|inflows?|outflows?)\b", q):
        selected.update(
            dataset.id
            for dataset in registry.datasets.values()
            if dataset.family == "subaward_flow" and geography_matches(dataset.geography)
        )

    # A very broad request is better served by the complete catalog. For a
    # focused question, this typically yields two to six tables and removes
    # most irrelevant sibling schemas from the prompt.
    return selected if 0 < len(selected) <= 10 else None


def _canonical_metric_names(proposed: list[Any], tables: list[str]) -> list[str]:
    """Resolve planner metric ids against routed physical schemas only."""
    canonical: list[str] = []
    for value in proposed:
        proposed_norm = str(value).strip().casefold().replace("_", " ")
        for table in tables:
            dataset = get_dataset(table)
            if dataset is None:
                continue
            for column in dataset.metrics:
                if column.casefold().replace("_", " ") == proposed_norm:
                    if column not in canonical:
                        canonical.append(column)
                    break
    return canonical


def _canonical_dimension_names(proposed: list[Any], tables: list[str]) -> list[str]:
    """Keep only exact dimensions physically available in the routed tables."""
    by_normalized: dict[str, str] = {}
    for table in tables:
        dataset = get_dataset(table)
        if dataset is None:
            continue
        for column in dataset.dimensions:
            by_normalized[column.casefold().replace("_", " ")] = column
    output: list[str] = []
    for value in proposed:
        found = by_normalized.get(str(value).strip().casefold().replace("_", " "))
        if found and found not in output:
            output.append(found)
    return output


_SYSTEM = """You are the intent + routing brain of a US public-policy data assistant.
ONE call decides BOTH (1) what kind of question this is and (2) which catalog
table(s) would answer it.

DOMAIN
======
{domain}

CATALOG (use exact ids when routing)
====================================
{catalog}

STEP 1 — classify the user's message:
- ANALYTICAL: answerable with SQL over the catalog (rankings, lookups,
  comparisons, trends, breakdowns, cross-dataset joins). The needed measure
  and scope are clear enough to write one query.
- CLARIFY: in-domain but the MEASURE itself is ambiguous (e.g. "federal money").
- UNANSWERABLE: a US data question but the metric is NOT in the catalog
  (crime, unemployment, GDP, weather statistics, forecasts).
- META: about the assistant, schema/catalog facts, available years, column
  availability, physical row counts, measure definitions/denominators, or term
  meaning. Asking for the VALUE or TOTAL of a measure is never META merely
  because a dataset name appears. Asking how many UNIQUE geographies are
  actually present is ANALYTICAL because it requires COUNT(DISTINCT ...) over
  the runtime data; asking how many physical rows are stored is META.
- OUT_OF_SCOPE: not about this data (chitchat, jokes, weather, general knowledge).

Rules:
- If the question NAMES a specific catalog measure (grants, contracts, direct
  payments, employees, subaward/subcontract, financial literacy, poverty,
  assets, liabilities, debt ratio, income, population, ...), it is ANALYTICAL
  even if phrased "how much / how many / how much did X get in <measure>".
  Reserve CLARIFY for when the MEASURE itself is ambiguous: "federal money"
  (which channel?), "best states" (by what?), "doing well" (which metric?).
- Descriptive superlatives that map to ONE clear catalog measure are ANALYTICAL,
  not CLARIFY ("poorest"->poverty rate, "how wealthy"->median income,
  "most stressed"->financial constraint, "most educated"->bachelor's).
- An explicit "by", "on", or "in terms of" measure controls any nearby
  superlative. "Richest/highest and poorest/lowest geographies on M" means the
  maximum and minimum observations of M, not a switch to income or another
  measure. Route it ANALYTICAL with derived subtract [M,M], preserve both
  extreme labels and values, and do not ask which unrelated definition of the
  adjectives the user intended.
- Government-finance polarity follows the requested expression: "poorest by
  net position" and "largest deficit" mean the lowest Assets - Liabilities;
  "largest Liabilities - Assets" means the highest value of that reversed
  expression. An unqualified "difference between/in assets and liabilities"
  means the catalog-defined Net_Position direction: Assets - Liabilities. Do
  not force all of these to descending Net_Position or reverse their operands.
- "trend ... by year" / rankings / comparisons / lookups / breakdowns are ANALYTICAL.
- "free cash flow", "cash flow" -> gov_* (ANALYTICAL), NOT the *_flow tables.
- A complete request is ANALYTICAL even if it omits a year (defaults apply).
- Do not ask for clarification merely because a qualitative analytical word
  permits several reasonable calculation conventions. When the measure,
  grouping, and comparison target are clear (for example, "uneven",
  "concentrated", "spread out", or "volatile"), choose a standard,
  scale-appropriate method, make that method explicit in the answer, and
  proceed. For unevenness across groups of different sizes, a relative spread
  measure such as coefficient of variation is preferable to a raw dollar gap;
  for concentration within one distribution, a top-share or HHI-style measure
  is appropriate. CLARIFY only when the unresolved choice changes the actual
  catalog measure, population, geography, or unit—not just the summary method.
- "Compare X to its peers" is complete when X, the measure, and geography are
  clear. Unless the user names a narrower cohort, peers means every other
  represented geography at the same grain and selected period. Route it as an
  ANALYTICAL comparison/ranking; do not ask the user to define peers.
- `year_strategy` must agree with the selected table's catalog year note. When
  the user omits a year, state the catalog default period (for example,
  contract_state -> 2024); use "no year filter" only for tables whose catalog
  explicitly says they have no year column or are a fixed snapshot.
- Misspellings/abbreviations are fine when intent is clear.
- Treat close dataset-id typos as the closest catalog id when the match is
  unambiguous (for example, contract_static_state -> contract_state).
- Distinguish the number of rows from the value of a count metric. "How many
  rows/records" is META; "how many employees/people/awards" is ANALYTICAL and
  must calculate that measure. Never substitute a dataset's row count for a
  requested metric total.
- If the user explicitly names a dataset and asks for a measure that is absent
  from that dataset's runtime schema, normally return UNANSWERABLE with the
  missing-field reason. One documented exception is a derived federal-contract
  measure below: keep the named contract_* table as the grain anchor and add
  the matching *_flow table. That is a cross-table calculation defined by the
  source variable dictionary, not substitution with a sibling dataset.
- Do not substitute a semantically different field to rescue a request:
  Employees is not Federal Residents, prime Contracts are not subaward flows,
  Revenue is not funding received, and a share is not a count.
- Never substitute the closest available demographic column for a requested
  category that is not in the routed table. A request for sex/gender, age 85+,
  associate's degree, or less than 9th grade is UNANSWERABLE when those exact
  fields are absent, even if Total population or a broader education threshold
  is available.
- Separate ACS percentage columns are marginal estimates, not joint
  cross-tabulations. A question asking for an intersection across two
  dimensions (race/ethnicity AND education, sex AND education, age AND
  education, etc.) is UNANSWERABLE unless one exact joint measure exists in the
  catalog. Never assume independence or multiply marginal percentages.
- A CORRELATION between two separately loaded numeric measures across rows is
  answerable and is not a demographic intersection. For example, correlating
  Hispanic share with Income >$50K across states or counties is ANALYTICAL;
  asking how many Hispanic people earn over $50K is an unsupported joint count.
- A cross-sectional correlation across geographies produces one coefficient,
  not one correlation per state/county/district. If the user asks which single
  geography has the "highest correlation" but the tables contain only one
  observation per geography for the selected period, CLARIFY whether they want
  the overall correlation or a ranking/ratio of the two measures.
- In a one-year correlation scoped WITHIN one named state, the state is a
  filter, not the observation grain. Use the matching county table so the
  state's counties supply repeated observations. Do not use the state table's
  single selected-year row and return NULL. Use a state table only when the
  requested observations are multiple states or an explicit multi-year state
  time series.
- Questions asking WHY an ACS education field refers to adults 25+, what its
  denominator means, or which age groups are loaded are META definition/schema
  questions. Answer from the column descriptions; do not rerun the prior value.
- FINRA geographic fields are survey shares/indices, not resident counts. The
  runtime FINRA files have no respondent count, population denominator, or
  survey weights. "How many satisfied residents" is UNANSWERABLE as a count;
  do not multiply `satisfied` by ACS Total population. The share itself remains
  answerable where the geography is represented.

STEP 2 (only when intent=ANALYTICAL) — pick the SMALLEST set of tables:
- Geography suffix matches the asked grain (_state / _county / _congress).
  If NO grain is named, default to _state. Never return multiple grains of the
  same family — pick exactly one.
- Exception for within-state cross-sectional correlations: a phrase such as
  "correlation for the state of X" defines the geographic scope. The repeated
  observation grain is county, so choose the compatible _county table and
  filter its state column to X.
- Federal awards by AGENCY -> spending_state_agency. Without an agency split -> contract_*.
- County/congressional prime Contracts or Grants by agency are UNANSWERABLE:
  the corresponding agency-grain tables are not loaded. Never substitute
  county_flow/congress_flow because those contain downstream subawards, not
  prime contract or grant totals.
- contract_state is the PRIMARY state-level table for RAW award/workforce totals.
  For a state-level published "Per 1000 residents" measure, use spending_state:
  the contract_* normalized fields do not have a traceable resident denominator.
  At county/congress grain, a request for a true per-capita or per-1,000-resident
  value is UNANSWERABLE unless the user explicitly accepts the stored, undefined
  published-normalization field. Never relabel that field as per capita.
- "free cash flow" / fiscal health / assets / debt -> gov_*.
- "subaward" / "subcontract" / money flowing between places -> *_flow.
- demographics / population / race / education / income / poverty -> acs_*.
- financial literacy / stress / risk aversion -> finra_*.
- Cross-dataset ("X and their Y" from different families) -> return BOTH tables.
- Multiple source periods are valid in one cross-sectional join. For example,
  ACS 2023 joined to FINRA 2021 is ANALYTICAL: assign each year to its own
  table in `year_strategy`, filter the tables separately, and join geography
  only. Never require every selected table to contain every year in the question.
- The supplied Federal Contracts variable dictionary defines these DERIVED
  subaward measures across the prime-award and flow tables at the same geography:
  "Sub-contract Out" = SUM(subaward amount) on the rcpt/origin side;
  "Sub-Contract In" = SUM on the subawardee/destination side;
  "Net Sub-Contract" = inflow minus outflow.
  Ratios or percentages involving those three subaward terms are valid
  cross-table calculations. Route contract_state + state_flow, contract_county +
  county_flow, or contract_congress + congress_flow as appropriate. These are
  formulas, not physical column names, so `metric_columns` must list the exact
  underlying runtime fields (Contracts and/or subaward amount).
- `Federal Contracts (Indirect)` is not safely derivable in the current
  catalog. The newest source dictionary calls it a separate
  `fed_act_obl_indirect` field, while an older dictionary describes a different
  Contracts + Net Sub-Contract formula. That physical field is not loaded.
  Until the data owner resolves the conflict, return UNANSWERABLE rather than
  manufacturing an indirect-contract value from flow data.
- Period coverage still governs derived measures: state_flow has no year
  column, so it cannot supply a 2024-only derived state value. county_flow and
  congress_flow do support FY2024. A missing period is a real limitation, not
  a reason to reject the same calculation at a supported grain.
- Do not silently combine state_flow's all-records snapshot with a default
  2024 contract/workforce value for a ratio, per-employee measure, or other
  derived comparison. Without an explicit user
  instruction accepting those mixed scopes, CLARIFY the limitation. A net
  state subaward total by itself may still use state_flow with no year filter.
- Per-capita / share variants normally stay in the SAME table, except the
  audited federal-spending state rule above.
- The federal-spending period '2020-2024' is a precomputed summary with an
  undocumented aggregation method. It may be selected when explicitly asked
  for, but never call it a five-year total or sum it with the 2024 row.
- For flow questions with a named place: inflow filters the subawardee/destination
  side; outflow filters the prime-recipient/origin rcpt side. Agency breakdowns
  group by agency_name and industry breakdowns group by naics_2digit_title.
- For "which geography receives the most inflow" or "which geography sends the
  most outflow", the ranked output geography is on that same named side:
  subawardee_* for receives/inflow and rcpt_* for sends/outflow. Use the
  opposite side only when the user explicitly asks for sources/origins sending
  into a destination or destinations receiving from an origin.
- A net subaward/subcontract flow is inflow minus outflow for the same place and
  must use both subawardee and rcpt sides with a subtraction; never answer it as
  ordinary inflow.
- ACS share/percent/rate questions use the percentage metric directly; do NOT
  add `Total population`. Add `Total population` only when the user asks for a
  count/number of people derived from a demographic percentage whose described
  denominator is explicitly Total population. Education percentages use adults
  age 25+, and that denominator is absent, so education counts are unavailable.
- In ACS wording, "Black population", "Asian population", "Hispanic
  population", and similar demographic names refer to the loaded percentage
  measure unless the user explicitly says count, headcount, number of people,
  "how many", or another unmistakable count unit. The word "population" by
  itself is not a request to derive persons.
- For "how many unique counties/states/districts are in <dataset>", route the
  table and use aggregate + COUNT(DISTINCT geography key), normally filtered to
  the catalog's latest/default year. `metric_columns` is correctly empty for
  this query shape and `needs_clarification` MUST be false because the requested
  geography dimension is the thing being counted. Do not answer from the
  physical row count.
- If two different tables are equally plausible AND the choice changes the
  answer, set needs_clarification=true and ask which one.
- For rankings, set `top_k` from the user's requested N. If no N is stated,
  reason about grammatical scope: a singular target asking for the single
  maximum gets 1; a plural target such as "which agencies/counties/states"
  gets 10 so the answer contains a useful ranked list. But an explicit command
  "rank [the/all] <geographies> by ..." asks for the full named scope and must
  set top_k=null; do not silently turn that command into a top-10 result.
- For every ranking, put only the measures that CONTROL row order in
  `semantic_plan.sort_columns`, in priority order. "Rank counties by Black and
  Asian population" uses [Black, Asian]. "States with the highest financial
  literacy and their debt ratio" uses [financial_literacy] because debt ratio
  is accompanying output, not another ranking key. Never silently sum,
  average, weight, or reorder the selected sort measures.
- For an unqualified "where does X rank?", use highest measure value as rank 1
  (sort_direction=desc) and state that convention in the answer. Use ascending
  only when the user explicitly asks for lowest/least/bottom/ascending or the
  requested expression itself defines that polarity.
- When a county or district is compared with its "state median", the benchmark
  is the median across represented counties or districts inside that state at
  the same grain and period. Do not substitute a single state-table aggregate;
  that is a statewide value, not a median.

SEMANTIC PLAN — make meaning explicit once:
- Return a complete `semantic_plan`. Downstream stages treat it as the
  authoritative interpretation and will not re-read the user's wording.
- `statistic` is value, sum, average, median, count, count_distinct,
  correlation, distribution, or derived.
- `result_unit` is the unit the USER requested after any formula: usd, persons,
  percent, ratio, count, correlation, index, or value. It is not necessarily
  the source column's unit. A demographic headcount derived from a percentage
  has result_unit=persons.
- `formula.operator` is none, identity, add, subtract, multiply, divide, or
  net_flow. `formula.operands` are exact runtime metric columns in semantic
  order. For A minus B use [A,B]; for A as a percent of B use [A,B] with
  scale=100. A repeated subtract operand [M,M] represents the difference
  between two requested observations of M, such as its maximum minus minimum.
  Do not replace explicitly ordered operands with an equivalent field whose
  displayed sign differs.
- `predicate` captures an explicit row condition. For measure-to-measure
  "counties where renter share is higher than owner share", use gt with
  ["Renter occupied","Owner occupied"]. This is a comparison/selection, not a
  ranking, unless the user separately asks to rank, sort, top, or bottom.
- A selected-period value for one named geography from an already aggregated
  geography table is operation=lookup and statistic=value. Use aggregate/sum
  for event rows or when multiple physical rows really must be combined; the
  words "how much" or "how many dollars" alone do not require SUM over an
  already aggregated cell.
- When a user asks for the total/actual number of a measure in a dataset but
  names no individual geography and requests no breakdown, combine the
  dataset's geography rows using that measure's documented default aggregation:
  operation=aggregate, result_scope=single, no output_dimensions. This is
  different from a named-geography lookup and from a physical row count.
- `observation_grain` names the repeated observations used by the statistic,
  which can differ from a filter's geography (Colorado county correlation ->
  county observations).
- `result_scope` is single, top_n, full, grouped, or unspecified. An explicit
  "rank the counties" request is full; "top 10" is top_n; a single maximum is
  single. A ranking with top_k=1 is always single, never top_n. A correlation
  across one observation population is also single,
  even when the answer contains several coefficient columns. Use grouped only
  when the user explicitly requests a separate coefficient for each named
  output dimension, and include that dimension in `output_dimensions`.
- `sort_columns` contains exact runtime metric columns that control a ranking,
  in primary-to-secondary order. It is empty for non-rankings and for a
  ranking on one derived formula result.
- `include_component_measures` is true only when the user asks to see formula
  inputs as well as the derived result.
- `output_dimensions` lists exact requested dimension columns such as state,
  county, county_fips, cd_118, agency_name, or year. Do not add technical ids
  that the user did not request.

Return ONLY JSON:
{{"intent": "<one of ANALYTICAL|CLARIFY|UNANSWERABLE|META|OUT_OF_SCOPE>",
 "requires_sql": <bool>,
 "needs_clarification": <bool>,
 "clarification_question": "<question to ask or empty>",
 "reason": "<short>",
 "tables": ["<exact ids; for META include explicitly referenced datasets>"],
 "metric_columns": ["<exact referenced measure columns; no dimensions>"],
 "filter_columns": ["<exact dimension/filter columns required>"],
 "geography_level": "state|county|congress|none",
 "operation": "lookup|ranking|comparison|trend|correlation|distribution|aggregate|breakdown",
 "flow_direction": "inflow|outflow|none",
 "sort_direction": "asc|desc|none",
 "top_k": <integer or null>,
 "year_strategy": "<period to use, or 'no year filter'>",
 "join_plan": "<how to join if >1 table, else empty>",
 "semantic_plan": {{
   "operation": "lookup|ranking|comparison|trend|correlation|distribution|aggregate|breakdown",
   "statistic": "value|sum|average|median|count|count_distinct|correlation|distribution|derived|unspecified",
   "result_unit": "usd|persons|percent|ratio|count|correlation|index|value|unspecified",
   "formula": {{"operator": "none|identity|add|subtract|multiply|divide|net_flow", "operands": ["<exact metric>"], "scale": 1.0, "output_label": "<short label>"}},
   "predicate": {{"operator": "none|gt|gte|lt|lte|eq|neq", "operands": ["<exact metric>"], "comparison_value": <number|string|null>}},
   "observation_grain": "state|county|congress|year|row|none",
   "result_scope": "single|top_n|full|grouped|unspecified",
   "sort_direction": "asc|desc|none",
   "sort_columns": ["<exact ranking metric, in priority order>"],
   "top_k": <integer or null>,
   "flow_direction": "inflow|outflow|none",
   "include_component_measures": <bool>,
   "output_dimensions": ["<exact dimension>"]
 }},
 "assumptions": ["<only assumptions required by catalog defaults>"],
 "confidence": "high|medium|low"}}"""


_FEWSHOT = """Examples:
  "top 10 counties in maryland by grants" -> ANALYTICAL, ["contract_county"]
  "which agencies give the most grants to Maryland" -> ANALYTICAL, ["spending_state_agency"], ranking, top_k=10
  "Maryland congressional districts by free cash flow" -> ANALYTICAL, ["gov_congress"]
  "subcontract inflow to Maryland" -> ANALYTICAL, ["state_flow"]
  "top 10 states by debt ratio" -> ANALYTICAL, ["gov_state"]
  "states with highest financial literacy and their government debt ratio" -> ANALYTICAL, ["finra_state","gov_state"]
  "2023 state income versus 2021 financial literacy" -> ANALYTICAL, ["acs_state","finra_state"], correlation, separate table years
  "Sub-contract Out as a percent of Federal Contracts for Nevada counties" -> ANALYTICAL, ["contract_county","county_flow"], ["Contracts","subaward_amount"]
    semantic_plan: derived divide ["subaward_amount","Contracts"], scale=100, observation_grain=county, result_scope=full, output_dimensions=["state","county"]
  "Federal Contracts (Indirect) for counties in 2024" -> UNANSWERABLE, []
  "where is the maximum asian population by count" -> ANALYTICAL, ["acs_state"]
    semantic_plan: derived multiply ["Total population","Asian"], scale=0.01, result_unit=persons, result_scope=single
  "how many unique counties are in the ACS county dataset" -> ANALYTICAL, ["acs_county"], [], aggregate
    semantic_plan: count_distinct, observation_grain=county, result_scope=single
  "Which counties have a higher renter population than owner population?" -> ANALYTICAL, ["acs_county"], ["Renter occupied","Owner occupied"], comparison
    semantic_plan: predicate gt ["Renter occupied","Owner occupied"], result_unit=percent, result_scope=full, output_dimensions=["county","state"]
  "gap between the highest and lowest counties on Grants" -> ANALYTICAL, ["contract_county"], ["Grants"], comparison
    semantic_plan: derived subtract ["Grants","Grants"], result_unit=usd, result_scope=single, output_dimensions=["county","state"]
  "how does Texas Grants compare to its peers" -> ANALYTICAL, ["contract_state"], ["Grants"], comparison
    semantic_plan: value, observation_grain=state, result_scope=grouped, output_dimensions=["state"]
  "why does bachelor's attainment refer to adults 25+" -> META, ["acs_state"], ["Education >= Bachelor's"]
  "how many Hispanic people have bachelor's degrees" -> UNANSWERABLE, []
  "correlation between Hispanic share and Income >$50K across states" -> ANALYTICAL, ["acs_state"], ["Hispanic","Income >$50K"], correlation
  "how many women have associate's degrees" -> UNANSWERABLE, []
  "how many people age 85+ are in the ACS data" -> UNANSWERABLE, []
  "how many satisfied residents are in Prince George County" -> UNANSWERABLE, []
  "how many grant dollars did Maryland receive" -> ANALYTICAL, ["contract_state"]
  "how much did California get in direct payments" -> ANALYTICAL, ["contract_state"]
  "how many employees does contract_static_state dataset have" -> ANALYTICAL, ["contract_state"], ["Employees"], aggregate
    semantic_plan: sum, result_unit=persons, observation_grain=state, result_scope=single, output_dimensions=[]
  "what is the actual number of employees in the contract_state dataset" -> ANALYTICAL, ["contract_state"], ["Employees"], aggregate
    semantic_plan: sum, result_unit=persons, observation_grain=state, result_scope=single, output_dimensions=[]
  "how many employees are in contract_county" -> UNANSWERABLE, []
  "Maryland financial literacy in 2024" -> UNANSWERABLE, []
  "does contract_state contain an Employees column" -> META, ["contract_state"], ["Employees"]
  "how many rows are in contract_state" -> META, ["contract_state"]
  "How much federal money goes to Maryland?" -> CLARIFY, []
  "How much direct federal spending goes to Maryland?" -> CLARIFY, []
  "top counties receiving Defense contracts" -> UNANSWERABLE, []
  "rank the states" -> CLARIFY, []
  "top counties with the maximum crime rate" -> UNANSWERABLE, []
  "what is FINRA?" -> META, []
  "who are you?" -> META, []
  "tell me a joke" -> OUT_OF_SCOPE, []
"""


def _history_snippet(history: list[dict[str, Any]] | None) -> str:
    if not history:
        return ""
    # Prior assistant prose can contain an earlier model mistake.  Feed the
    # router user intent plus structured contract memory, never generated
    # analytical prose that can recursively contaminate the next answer.
    recent = [h for h in history if h.get("role") == "user"][-4:]
    return "\n".join(f"{h['role']}: {h.get('content', '')[:200]}" for h in recent)


def _safe_default() -> dict[str, Any]:
    return {
        "intent": "CLARIFY",
        "requires_sql": False,
        "needs_clarification": True,
        "clarification_question": "Could you rephrase or add detail to your question?",
        "reason": "fused intent+route model unavailable; failing safe",
        "tables": [],
        "columns": [],
        "filter_columns": [],
        "geography_level": "none",
        "operation": "lookup",
        "flow_direction": "none",
        "sort_direction": "none",
        "top_k": None,
        "year_strategy": "",
        "join_plan": "",
        "assumptions": [],
        "confidence": "low",
        "service_unavailable": True,
    }


def _unavailable_period_reason(
    question: str,
    tables: list[str],
    year_strategy: str = "",
) -> str:
    """Return a catalog-grounded reason when an explicit period cannot exist.

    This is a structural availability check, not an answer heuristic: it stops
    the LLM from generating SQL for years the routed physical table does not
    contain and lets the normal unsupported-guidance path suggest a valid year.
    """
    raw_years = [int(value) for value in re.findall(r"\b(?:19|20)\d{2}\b", question)]
    if not raw_years:
        return ""
    range_matches = re.findall(
        r"\b((?:19|20)\d{2})\s*[-\u2013]\s*((?:19|20)\d{2})\b",
        question,
    )
    range_labels = {f"{left}-{right}" for left, right in range_matches}
    requested_period = next(iter(range_labels), None) if len(range_labels) == 1 else None
    period_by_table = resolve_periods_by_table(
        tables,
        raw_years,
        requested_period,
        year_strategy,
    )

    for table in tables:
        dataset = get_dataset(table)
        if dataset is None:
            continue
        available = {str(value).strip("'\"") for value in dataset.available_years}
        matched_ranges = range_labels & available
        years = list(raw_years)
        for label in matched_ranges:
            left, right = (int(value) for value in label.split("-", 1))
            # Those endpoints identify one stored summary label; they are not
            # requests for missing annual rows.
            for endpoint in (left, right):
                if endpoint in years:
                    years.remove(endpoint)

        if table.startswith("gov_"):
            assigned = period_by_table.get(table)
            assigned_years = assigned if isinstance(assigned, list) else [assigned]
            missing = sorted(
                {int(year) for year in assigned_years if isinstance(year, int) and year != 2023}
            )
            if missing:
                return f"{table} is a Fiscal Year 2023 snapshot; {', '.join(map(str, missing))} is not loaded."
            continue
        if dataset.year_column is None:
            # A table with no period field cannot contribute a year-specific
            # component to a multi-table calculation either.
            if raw_years or range_labels:
                return f"{table} has no year column, so a year-specific answer is unavailable."
            continue
        assigned = period_by_table.get(table)
        assigned_values = assigned if isinstance(assigned, list) else [assigned]
        assigned_years = {
            int(value)
            for value in assigned_values
            if isinstance(value, int) or (isinstance(value, str) and value.isdigit())
        }
        numeric_available = {
            int(value) for value in available if re.fullmatch(r"(?:19|20)\d{2}", value)
        }
        if numeric_available and assigned_years:
            missing = sorted(assigned_years - numeric_available)
        elif dataset.default_year is not None:
            missing = sorted(
                {year for year in assigned_years if str(year) != str(dataset.default_year)}
            )
        else:
            missing = []
        if missing:
            valid = ", ".join(str(value) for value in dataset.available_years) or str(
                dataset.default_year
            )
            return (
                f"{table} does not contain {', '.join(map(str, missing))}. "
                f"Available runtime period(s): {valid}."
            )
    return ""


def classify_and_route(
    question: str, history: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    candidate_tables = _candidate_tables_for_question(question)
    system = _SYSTEM.format(domain=domain_summary(), catalog=catalog_for_prompt(candidate_tables))
    convo = _history_snippet(history)
    user = (
        (f"Recent conversation:\n{convo}\n\n" if convo else "")
        + _FEWSHOT
        + f"\nClassify and route this message:\n{question}"
    )
    try:
        raw = client.chat_json(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
            # The typed plan is substantially richer than the legacy route
            # object. A 500-token cap intermittently truncated valid DeepSeek
            # JSON on multi-table formulas, which surfaced as a false service
            # outage even though the provider had answered.
            max_tokens=900,
            purpose="stage12_intent_route",
        )
    except client.LLMError:
        return _safe_default()
    if not isinstance(raw, dict):
        return _safe_default()

    # The compact first-pass router can miss terminology that appears only in
    # a variable description. Audit every analytical/data-availability
    # decision against a description-rich index before the pipeline is allowed
    # to route or refuse the question. This remains LLM reasoning over the
    # runtime schema; it is not a query-specific answer rule.
    raw, route_verification = verify_route(question, raw, candidate_tables)
    planner_supplied_semantics = isinstance(raw.get("semantic_plan"), dict)
    semantics = semantic_plan_from_routing(raw)

    intent = str(raw.get("intent", "")).strip().upper()
    if intent not in INTENTS:
        intent = "CLARIFY"
    referenced_tables = [t for t in (raw.get("tables") or []) if t in _VALID_TABLES]
    unavailable_period = (
        _unavailable_period_reason(
            question,
            referenced_tables,
            str(raw.get("year_strategy") or ""),
        )
        if intent == "ANALYTICAL" and referenced_tables
        else ""
    )
    if unavailable_period:
        intent = "UNANSWERABLE"
        raw["reason"] = unavailable_period
        raw["needs_clarification"] = False
    tables = referenced_tables if intent == "ANALYTICAL" else []
    if (
        intent == "ANALYTICAL"
        and any(table.endswith("_flow") for table in tables)
        and semantics.formula.operator == "net_flow"
    ):
        # Net is a two-sided calculation, not an inflow/outflow side filter.
        semantics = semantics.model_copy(update={"flow_direction": "none"})
    raw_metrics = raw.get("metric_columns") or raw.get("columns") or []
    metric_columns = _canonical_metric_names(list(raw_metrics), referenced_tables)
    requested_metric_columns = list(metric_columns)
    formula = semantics.formula
    if formula.operator != "none" and formula.operands:
        canonical_operands = _canonical_metric_names(list(formula.operands), referenced_tables)
        if len(canonical_operands) == len(formula.operands):
            formula = formula.model_copy(update={"operands": canonical_operands})
            semantics = semantics.model_copy(update={"formula": formula})
            # Real arithmetic determines the required inputs. Identity merely
            # labels one selected measure and must not erase companion measures
            # used by a correlation, comparison, or side-by-side result.
            if formula.operator != "identity":
                metric_columns = list(canonical_operands)
            else:
                for column in canonical_operands:
                    if column not in metric_columns:
                        metric_columns.append(column)
            if semantics.include_component_measures and formula.operator != "identity":
                for column in requested_metric_columns:
                    if column not in metric_columns:
                        metric_columns.append(column)
    predicate = semantics.predicate
    if predicate.operator != "none" and predicate.operands:
        canonical_predicate_operands = _canonical_metric_names(
            list(predicate.operands), referenced_tables
        )
        if len(canonical_predicate_operands) == len(predicate.operands):
            predicate = predicate.model_copy(update={"operands": canonical_predicate_operands})
            semantics = semantics.model_copy(update={"predicate": predicate})
            for operand in canonical_predicate_operands:
                if operand not in metric_columns:
                    metric_columns.append(operand)
    # Government Net_Position is the catalog's exact stored identity for
    # Total_Assets - Total_Liabilities. Canonicalize mathematically equivalent
    # formulas so follow-up memory and repeatability signatures do not alternate
    # between a stored metric and an expanded expression. Preserve a reversed
    # subtraction only when the user explicitly ordered "liabilities minus
    # assets"; otherwise convert -Net_Position to Net_Position and reverse the
    # selected ordering so the ranked geography is mathematically unchanged.
    if (
        any(table.startswith("gov_") for table in referenced_tables)
        and formula.operator == "subtract"
        and formula.operands
        in (
            ["Total_Assets", "Total_Liabilities"],
            ["Total_Liabilities", "Total_Assets"],
        )
        and not (
            formula.operands == ["Total_Liabilities", "Total_Assets"]
            and re.search(
                r"\bliabilit(?:y|ies)\s*(?:minus|-|less)\s*(?:total\s+)?assets?\b",
                question,
                re.I,
            )
        )
    ):
        reversed_formula = formula.operands == ["Total_Liabilities", "Total_Assets"]
        formula = formula.model_copy(
            update={
                "operator": "identity",
                "operands": ["Net_Position"],
                "scale": 1.0,
                "output_label": formula.output_label or "Net Position",
            }
        )
        canonical_sort = semantics.sort_direction
        if reversed_formula:
            canonical_sort = {"asc": "desc", "desc": "asc"}.get(
                semantics.sort_direction,
                semantics.sort_direction,
            )
        semantics = semantics.model_copy(
            update={
                "formula": formula,
                "statistic": "value",
                "sort_direction": canonical_sort,
            }
        )
        metric_columns = ["Net_Position"]
    sort_columns = _canonical_metric_names(list(semantics.sort_columns), referenced_tables)
    if semantics.operation == "ranking" and formula.operator in {"none", "identity"}:
        sort_columns = [column for column in sort_columns if column in metric_columns]
        if not sort_columns and metric_columns:
            # Backward-compatible provider default. This is derived from the
            # already-selected metric order, never from a question phrase.
            sort_columns = [metric_columns[0]]
    else:
        sort_columns = []
    semantics = semantics.model_copy(update={"sort_columns": sort_columns})
    output_dimensions = _canonical_dimension_names(
        list(semantics.output_dimensions), referenced_tables
    )
    # A county name is not a globally unique result identity. Whenever county
    # rows are requested and the table has a state dimension, carry the state
    # label as a stable companion for answers, maps, and follow-up references.
    available_dimensions = {
        dimension
        for table in referenced_tables
        if (dataset := get_dataset(table)) is not None
        for dimension in dataset.dimensions
    }
    if (
        "county" in output_dimensions
        and "state" in available_dimensions
        and "state" not in output_dimensions
    ):
        output_dimensions.append("state")
    semantics = semantics.model_copy(update={"output_dimensions": output_dimensions})
    filter_columns = _canonical_dimension_names(
        list(raw.get("filter_columns") or []), referenced_tables
    )
    # A result dimension describes the rows to return; it is not also an input
    # filter. This distinction is essential for directional data. For example,
    # "destinations receiving from MD-08" filters the origin and groups the
    # destination instead of constraining both sides to Maryland.
    if any(table.endswith("_flow") for table in referenced_tables):
        filter_columns = [
            column for column in filter_columns if column not in set(output_dimensions)
        ]
    # A national flow ranking ranks the side named by the direction: inflow
    # ranks destinations/subawardees and outflow ranks origins/prime recipients.
    # The other side remains valid for a focused breakdown (for example,
    # origins sending into one named destination), which has filter dimensions.
    flow_geography_dimensions = {
        "state_flow": {
            "inflow": "subawardee_state_name",
            "outflow": "rcpt_state_name",
            "all": {"subawardee_state_name", "rcpt_state_name"},
        },
        "county_flow": {
            "inflow": "subawardee_cty_name",
            "outflow": "rcpt_cty_name",
            "all": {"subawardee_cty_name", "rcpt_cty_name"},
        },
        "congress_flow": {
            "inflow": "subawardee_cd_name",
            "outflow": "rcpt_cd_name",
            "all": {"subawardee_cd_name", "rcpt_cd_name"},
        },
    }
    if (
        len(tables) == 1
        and tables[0] in flow_geography_dimensions
        and semantics.operation == "ranking"
        and semantics.flow_direction in {"inflow", "outflow"}
        and not filter_columns
    ):
        flow_dimensions = flow_geography_dimensions[tables[0]]
        all_flow_dimensions = set(flow_dimensions["all"])
        if not output_dimensions or all_flow_dimensions.intersection(output_dimensions):
            output_dimensions = [
                dimension for dimension in output_dimensions if dimension not in all_flow_dimensions
            ]
            output_dimensions.append(str(flow_dimensions[semantics.flow_direction]))
            semantics = semantics.model_copy(update={"output_dimensions": output_dimensions})
    if not planner_supplied_semantics and any(table.startswith("acs_") for table in tables):
        q_lower = question.lower()
        asks_share = bool(re.search(r"\b(percent|percentage|share|rate)\b", q_lower))
        asks_count = bool(
            re.search(r"\b(count|number of people|how many people|people, not percent)\b", q_lower)
        )
        if asks_share and not asks_count and len(metric_columns) > 1:
            metric_columns = [
                column
                for column in metric_columns
                if column not in {"Total population", "# of household"}
            ]
    dimension_count = semantics.statistic == "count_distinct"
    if not planner_supplied_semantics:
        # Temporary compatibility for providers/fixtures that predate the
        # semantic plan. This path is observable and can be removed after the
        # migration window; it never overrides a supplied typed interpretation.
        dimension_count = bool(
            re.search(
                r"\b(?:how many(?:\s+unique)?|(?:total\s+)?number of(?:\s+unique)?|"
                r"count of(?:\s+unique)?)\s+"
                r"(?:states|counties|districts|agencies|rows|records)\b",
                question,
                re.IGNORECASE,
            )
        )
    missing_measure = (
        intent == "ANALYTICAL" and bool(tables) and not metric_columns and not dimension_count
    )
    needs_clar = (
        bool(raw.get("needs_clarification"))
        or intent == "CLARIFY"
        or (intent == "ANALYTICAL" and not tables)
        or missing_measure
    )
    return {
        "intent": intent,
        "requires_sql": intent == "ANALYTICAL",
        "needs_clarification": needs_clar,
        "clarification_question": (
            "Which exact measure should I calculate?"
            if missing_measure
            else str(raw.get("clarification_question") or "").strip()
        ),
        "reason": str(raw.get("reason") or "").strip(),
        "tables": tables,
        "catalog_tables": referenced_tables if intent == "META" else [],
        "catalog_columns": metric_columns if intent == "META" else [],
        # Backward-compatible key; now guaranteed to contain canonical metric
        # columns only, rather than a mixture of measures and dimensions.
        "columns": metric_columns,
        # These dimensions are selected by the same language plan as the
        # measure/formula. Grounding must not independently reinterpret every
        # geography-looking phrase as another filter.
        "filter_columns": filter_columns,
        "geography_level": str(raw.get("geography_level") or "none"),
        "operation": semantics.operation,
        "flow_direction": semantics.flow_direction,
        "sort_direction": semantics.sort_direction,
        "top_k": semantics.top_k,
        "year_strategy": str(raw.get("year_strategy") or ""),
        "join_plan": str(raw.get("join_plan") or ""),
        "semantic_plan": semantics.model_dump(),
        # User-visible assumptions are built from registry facts later; model
        # prose here is intentionally discarded.
        "assumptions": [],
        "confidence": str(raw.get("confidence") or "medium"),
        "clarification": (
            "Which exact measure should I calculate?"
            if missing_measure
            else str(raw.get("clarification_question") or "").strip()
        ),
        "service_unavailable": False,
        "route_verification": route_verification,
    }
