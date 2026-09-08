"""Catalog backed by data/schema/metadata.json (the single source of truth).

This module is intentionally thin: it adapts the curated metadata.json + manifest.json
into the DatasetDefinition models the rest of the app consumes, and exposes
prompt-ready catalog helpers used by the LLM-grounded pipeline (intent / routing /
retrieval / generation).

Public API preserved for existing consumers:
  quote_identifier, mart_view_name, load_registry, get_dataset, all_allowed_views
New helpers for the LLM pipeline:
  metadata_doc, catalog_for_prompt, table_schema_block, critical_warnings_for,
  join_hints_for, geographic_keys, common_question_patterns,
  semantic_catalog_for_verification
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Any

from app.paths import MANIFEST_PATH, METADATA_PATH
from app.semantic.models import (
    DatasetDefinition,
    DimensionDefinition,
    MetricDefinition,
    SemanticRegistrySnapshot,
)

REGISTRY_VERSION = "metadata-catalog-v3"
_SPECIAL_IDENTIFIER = re.compile(r"[^A-Za-z0-9_]")


# ---------------------------------------------------------------------------
# Identifier helpers (unchanged public contract)
# ---------------------------------------------------------------------------
def quote_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    if _SPECIAL_IDENTIFIER.search(name) or not name or name[0].isdigit():
        return f'"{escaped}"'
    return escaped


def mart_view_name(table_name: str) -> str:
    return f"mart_{table_name}"


# ---------------------------------------------------------------------------
# Raw metadata access
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _raw() -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = json.loads(MANIFEST_PATH.read_text())
    metadata = json.loads(METADATA_PATH.read_text())
    return manifest, metadata


def metadata_doc() -> dict[str, Any]:
    """The full curated metadata.json (critical warnings, joins, patterns, tables)."""
    return _raw()[1]


def table_metadata(table_name: str) -> dict[str, Any]:
    """Return a defensive copy of the curated, user-facing table metadata."""
    raw = metadata_doc().get("tables", {}).get(table_name, {})
    return dict(raw) if isinstance(raw, dict) else {}


def column_metadata(table_name: str) -> dict[str, dict[str, Any]]:
    """Effective column documentation, including inherited sibling docs.

    This is the public adapter used by the dataset library. Keeping inheritance
    here means the frontend, prompt catalog, and SQL layer all describe a
    variable from the same source of truth.
    """
    return {name: dict(value) for name, value in _effective_meta_cols(table_name).items()}


def geographic_keys() -> dict[str, Any]:
    return metadata_doc().get("geographic_keys", {})


def common_question_patterns() -> dict[str, Any]:
    return metadata_doc().get("common_question_patterns", {})


# ---------------------------------------------------------------------------
# Column classification
# ---------------------------------------------------------------------------
_YEAR_COLUMNS = {"year", "Year", "act_dt_fis_yr"}

# Columns that are identifiers / dimensions / geometry, never aggregatable measures.
_KEY_COLUMNS = {
    "state",
    "county",
    "cd_118",
    "fips",
    "state_fips",
    "county_fips",
    "agency",
    "agency_name",
    "agency_code",
    "naics",
    "naics_2digit_code",
    "naics_2digit_title",
    "naics_2digit",
    "rcpt_st_cd",
    "rcpt_state_name",
    "subawardee_st_cd",
    "subawardee_state_name",
    "rcpt_cty",
    "subawardee_cty",
    "rcpt_cty_name",
    "subawardee_cty_name",
    "rcpt_state",
    "subawardee_state",
    "rcpt_full_name",
    "subawardee_full_name",
    "rcpt_cd_name",
    "subawardee_cd_name",
    "prime_awardee_stcd118",
    "subawardee_stcd118",
    "origin_lat",
    "origin_lon",
    "dest_lat",
    "dest_lon",
    "Unnamed: 0",
}

_AVG_HINTS = (
    "per_capita",
    "per capita",
    "per 1000",
    "per1000",
    "ratio",
    "rate",
    "median",
    "percent",
    "%",
    "index",
    "share",
)
_DIMENSION_COLUMNS = tuple(sorted(_KEY_COLUMNS - {"Unnamed: 0"}))

_CURATED_METRIC_ALIASES = {
    "total population": ["population count", "resident population"],
    "age 18-65": ["working-age share", "working-age population percentage"],
    "# of household": ["household count", "number of households"],
    "income >$50k": ["households earning over $50,000", "household income above 50k"],
    "income >$100k": ["households earning over $100,000", "household income above 100k"],
    "income >$200k": ["households earning over $200,000", "household income above 200k"],
    "below poverty": ["poverty rate", "percent below poverty", "below poverty level"],
    "education >= high school": ["high school graduate or higher", "high school attainment"],
    "education >= bachelor's": [
        "bachelor's degree attainment",
        "college degree attainment",
        "bachelor degree or higher",
    ],
    "education >= graduate": ["graduate degree attainment", "graduate or professional degree"],
    "median household income": ["household income", "median income"],
    "owner occupied": ["homeownership rate", "owner-occupied housing share"],
    "renter occupied": ["renter occupancy rate", "renter-occupied housing share"],
    "total_assets": ["government resources", "resources owned", "government assets"],
    "current_assets": ["short-term assets", "liquid assets", "resources available within one year"],
    "total_liabilities": ["government debt and obligations", "total obligations"],
    "current_liabilities": ["short-term obligations", "bills due within one year"],
    "non-current_liabilities": ["long-term obligations", "long-term liabilities"],
    "net_position": ["government net worth", "government equity"],
    "net_pension_liability": ["unfunded pension obligations", "pension shortfall"],
    "net_opeb_liability": ["retiree healthcare obligations", "unfunded retiree benefits"],
    "compensated_absences": ["unused leave liability", "vacation and sick leave obligations"],
    "revenue": ["government income", "taxes fees and transfers"],
    "expenses": ["cost of public services", "government expenditures"],
    "current_ratio": ["short-term solvency", "liquidity ratio"],
    "financial_constraint": ["financial stress", "financial constraint"],
    "financial_literacy": ["financial literacy", "financial knowledge"],
    "alternative_financing": [
        "alternative financial services",
        "payday loans and high-cost credit",
    ],
    "satisfied": ["financial wellbeing", "financial well-being", "financially satisfied"],
    "risk_averse": ["risk aversion", "unwillingness to take financial risk"],
    "free_cash_flow": ["free cash flow", "cash flow", "fiscal cushion"],
    "debt_ratio": ["debt ratio", "liabilities to assets ratio"],
    "bonds,_loans_&_notes": [
        "bonds loans and notes",
        "bonded debt",
        "amount borrowed",
        "government borrowing",
        "borrow",
        "borrowed",
        "borrowing",
    ],
    "subaward_amount": [
        "subaward dollars",
        "subcontract dollars",
        "sub-contract out",
        "sub-contract in",
        "net sub-contract",
        "net subcontract",
    ],
    "subaward_amount_year": [
        "subaward dollars",
        "subcontract dollars",
        "sub-contract out",
        "sub-contract in",
        "net sub-contract",
        "net subcontract",
    ],
    "contracts": [
        "federal contracts",
        "federal contract obligations",
        "prime federal contracts",
        "direct federal contracts",
    ],
    "direct payments": [
        "benefit transfers",
        "transfer payments",
        "federal benefits",
        "welfare payments",
    ],
    "resident wage": ["federal wages paid to residents", "resident federal wages"],
    "employees wage": ["federal civilian payroll", "federal employee wages"],
    # Terminology used by the supplied source dictionary differs from the
    # runtime column labels. Preserve both vocabularies in LLM grounding.
    "employees": ["employee count", "federal employment", "federal employees"],
    "federal residents": ["resident federal employees", "federal employees residing"],
}

_CURATED_METRIC_LABELS = {
    "below poverty": "Poverty rate",
    "education >= bachelor's": "Bachelor's degree attainment",
    "education >= graduate": "Graduate degree attainment",
    "financial_literacy": "Financial literacy index",
    "financial_constraint": "Financial constraint index",
    "alternative_financing": "Alternative financing index",
    "satisfied": "Financial satisfaction share",
    "satisfaction": "Financial satisfaction index",
    "risk_averse": "Risk aversion index",
    "contracts": "Federal contracts",
    "grants": "Federal grants",
    "subaward_amount": "Subaward amount",
    "subaward_amount_year": "Subaward amount",
}


def _metric_label(column: str) -> str:
    curated = _CURATED_METRIC_LABELS.get(column.casefold())
    if curated:
        return curated
    text = " ".join(column.replace("_", " ").replace(",", " ").replace("&", "and").split())
    return text[:1].upper() + text[1:]


def _is_measure(column: str) -> bool:
    if column in _KEY_COLUMNS or column in _YEAR_COLUMNS:
        return False
    return True


def _aggregation_for(column: str, meta_col: dict[str, Any]) -> str:
    haystack = f"{column} {meta_col.get('unit', '')} {meta_col.get('range', '')}".lower()
    if meta_col.get("range") == "0–1" or "0-1" in haystack or "0–1" in haystack:
        return "avg"
    if any(hint in haystack for hint in _AVG_HINTS):
        return "avg"
    return "sum"


def _family_for(table_name: str) -> str:
    prefix = table_name.split("_", 1)[0]
    return {
        "acs": "demographics",
        "gov": "government_finance",
        "contract": "federal_funding",
        "spending": "federal_funding",
        "finra": "financial_health",
        "state": "subaward_flow",
        "county": "subaward_flow",
        "congress": "subaward_flow",
    }.get(prefix, "general")


def _label_column(geography: str, columns: list[str]) -> str:
    for candidate in (
        ("cd_118" if geography in {"congress", "congressional_district"} else None),
        ("county" if geography == "county" else None),
        "state",
        "rcpt_state_name",
        "rcpt_cd_name",
    ):
        if candidate and candidate in columns:
            return candidate
    return columns[0] if columns else "state"


def _year_column(meta: dict[str, Any], columns: list[str]) -> str | None:
    declared = meta.get("year_column")
    if declared and declared in columns:
        return declared
    for candidate in ("year", "Year", "act_dt_fis_yr"):
        if candidate in columns:
            return candidate
    return None


def _available_years(meta: dict[str, Any]) -> list[str | int]:
    raw = meta.get("year_values")
    if isinstance(raw, list) and raw:
        values: list[str | int] = [str(v).strip("'\"") for v in raw]
        return values
    rng = meta.get("year_range")
    if isinstance(rng, str) and rng.strip():
        found = [int(tok) for tok in re.findall(r"(?:19|20)\d{2}", rng)]
        is_range = ("–" in rng or "-" in rng) and "," not in rng and len(found) == 2
        if is_range:
            lo, hi = sorted(found)
            expanded: list[str | int] = list(range(lo, hi + 1))
            return expanded
        parsed: list[str | int] = list(found) if found else [rng]
        return parsed
    return []


def _default_year(
    table_name: str, meta: dict[str, Any], years: list[str | int]
) -> str | int | None:
    if table_name.startswith("gov_"):
        return None  # gov tables are single-snapshot, never year-filtered
    if table_name.startswith("contract_") or table_name.startswith("spending_"):
        return "2024"
    if table_name.startswith("acs_"):
        return 2023
    if table_name.startswith("finra_"):
        return 2021
    if years:
        return years[-1]
    return None


# ---------------------------------------------------------------------------
# Critical warnings → per-table caveats
# ---------------------------------------------------------------------------
def _warnings_index() -> dict[str, list[str]]:
    """Map table_name -> list of human-readable critical-warning strings."""
    warnings = metadata_doc().get("_critical_warnings", {})
    index: dict[str, list[str]] = {}

    def add(table: str, message: str) -> None:
        index.setdefault(table, [])
        if message not in index[table]:
            index[table].append(message)

    casing = warnings.get("state_name_casing", {})
    fix = casing.get("sql_fix", "Wrap state in LOWER() before joining.")
    for t in casing.get("lowercase_tables", []):
        add(t, f"`state` is lowercase. {fix}")
    for t in casing.get("uppercase_tables", []):
        add(t, f"`state` is UPPERCASE. {fix}")
    for t in casing.get("titlecase_tables", []):
        add(t, f"`state` is Title Case. {fix}")

    spec = warnings.get("special_character_columns", {})
    cols = ", ".join(spec.get("columns", []))
    for t in spec.get("tables_affected", []):
        add(t, f"These columns must be double-quoted in SQL: {cols}.")

    spaces = warnings.get("columns_with_spaces", {})
    ex = ", ".join(spaces.get("examples", []))
    for t in spaces.get("tables_affected", []):
        add(t, f"Columns with spaces must be double-quoted, e.g. {ex}.")

    ystr = warnings.get("year_as_string_in_contract_tables", {})
    for t in ystr.get("tables_affected", []):
        add(
            t,
            "`year` is a STRING with exactly two values per geography: '2024' "
            "(single-year snapshot) and '2020-2024' (precomputed multi-year "
            "summary; its exact aggregation method is not documented here). You MUST filter "
            "to exactly one period — default to year = '2024' unless the user "
            "explicitly asks for the multi-year summary. NEVER call that row a "
            "five-year sum, omit the period filter, or SUM across both rows.",
        )

    gov = warnings.get("gov_year_label", {})
    for t in ("gov_state", "gov_county", "gov_congress"):
        add(
            t,
            gov.get("note", "Single year only.")
            + " "
            + gov.get("sql_fix", "Do not filter by year."),
        )

    # Missing-data placeholder rows must not win bottom rankings.
    add(
        "gov_state",
        "Rhode Island and Vermont report 0 for EVERY financial column "
        "(Total_Liabilities, Debt_Ratio, Current_Ratio, Revenue, Expenses, "
        "and all per-capita fields) because their source filings are missing "
        "— these are NOT real zeros. For ANY lowest/bottom/ascending ranking "
        "on ANY financial column you MUST add `AND Total_Liabilities > 0` to "
        "keep these missing-data rows out of the result.",
    )
    add(
        "gov_county",
        "Coverage is field-specific. Exclude NULL for the requested financial "
        "metric and exclude rows whose core financial fields are all missing/zero. "
        "Do NOT use a blanket `Total_Liabilities > 0` filter for unrelated metrics: "
        "some counties have partial filings with valid assets, revenue, or expenses.",
    )
    add(
        "gov_congress",
        "Twenty district rows contain zero in every financial field because mapped "
        "source filings are unavailable. Exclude all-zero placeholder rows from "
        "financial rankings and summaries.",
    )

    for t in ("finra_state",):
        add(t, "Survey waves only: 2009, 2012, 2015, 2018, 2021. No other years exist.")
    for t in ("finra_county", "finra_congress"):
        add(t, "Only year 2021 exists.")

    sfd = warnings.get("state_flow_duplicate_columns", {})
    add(
        "state_flow",
        sfd.get("fix", "Use rcpt_state_name / subawardee_state_name.")
        + " state_flow has NO year column (all available records). "
        "subaward_amount_year can be negative (clawbacks). Totals include "
        "intra-state flows (for example, Maryland to Maryland) unless the SQL "
        "explicitly excludes them; never describe an unfiltered total as only "
        "funding to or from other states.",
    )

    cfi = warnings.get("congress_flow_integer_district_id", {})
    add(
        "congress_flow",
        cfi.get("fix", "Use rcpt_cd_name; never join the integer district id to cd_118."),
    )

    cff = warnings.get("county_flow_fips_as_integer", {})
    add("county_flow", cff.get("note", "rcpt_cty / subawardee_cty are integer FIPS codes."))

    # Junk pandas index column present in the flow exports.
    manifest, _ = _raw()
    for table, info in manifest.items():
        if "Unnamed: 0" in info.get("columns", []):
            add(
                table,
                'Never SELECT or aggregate the "Unnamed: 0" column — it is a junk row index, not data.',
            )

    # Table-level analyst notes are part of the same semantic contract as the
    # column dictionary. Feed them to SQL generation, answer writing, and the
    # faithfulness judge so the public dataset page and the agent cannot drift
    # into two different descriptions of runtime coverage.
    for table, meta in metadata_doc().get("tables", {}).items():
        if not isinstance(meta, dict):
            continue
        for note in meta.get("critical_notes") or []:
            if str(note).strip():
                add(table, str(note).strip())

    return index


def critical_warnings_for(tables: list[str]) -> list[str]:
    index = _warnings_index()
    seen: list[str] = []
    for table in tables:
        for message in index.get(table, []):
            tagged = f"[{table}] {message}"
            if tagged not in seen:
                seen.append(tagged)
    return seen


# ---------------------------------------------------------------------------
# Column-doc inheritance
#
# Several sibling-grain tables (acs_congress, acs_county, gov_congress,
# gov_county, spending_state, contract_congress, ...) ship in metadata.json with
# the note "same variables as <state table>" but EMPTY per-column descriptions.
# That strips the %-vs-count / unit / quoting signal the canonical state-level
# table documents. We transparently inherit those docs by exact column name so
# the LLM is grounded the same way at every grain.
# ---------------------------------------------------------------------------
_CANONICAL_BY_FAMILY = {
    "demographics": "acs_state",
    "government_finance": "gov_state",
    "federal_funding": "contract_state",
    "financial_health": "finra_state",
}


@lru_cache(maxsize=32)
def _effective_meta_cols(table_name: str) -> dict[str, Any]:
    tables = metadata_doc().get("tables", {})
    own = tables.get(table_name, {}).get("columns", {})
    own = own if isinstance(own, dict) else {}
    canonical_id = _CANONICAL_BY_FAMILY.get(_family_for(table_name))
    canon = tables.get(canonical_id, {}).get("columns", {})
    canon = canon if canonical_id and isinstance(canon, dict) else {}
    merged: dict[str, Any] = {}
    for col, meta in own.items():
        meta = dict(meta) if isinstance(meta, dict) else {}
        ref = (
            canon.get(col)
            if canonical_id != table_name and isinstance(canon.get(col), dict)
            else {}
        )
        for field in ("description", "unit", "sql_name", "range", "sample_values"):
            if not meta.get(field) and ref.get(field):
                meta[field] = ref[field]
        if not meta.get("description") and col.endswith("_per_capita"):
            base_column = col[: -len("_per_capita")]
            base = own.get(base_column)
            if not isinstance(base, dict) or not base.get("description"):
                base = canon.get(base_column)
            base_description = (
                str(base.get("description") or "").rstrip(".") if isinstance(base, dict) else ""
            )
            if base_description:
                meta["description"] = (
                    f"{base_description}, divided by the represented population (USD per person)."
                )
        merged[col] = meta
    return merged


# ---------------------------------------------------------------------------
# Dataset construction
# ---------------------------------------------------------------------------
def _dimensions(columns: list[str], meta_cols: dict[str, Any]) -> dict[str, DimensionDefinition]:
    dims: dict[str, DimensionDefinition] = {}
    for col in columns:
        if col in _DIMENSION_COLUMNS:
            dims[col] = DimensionDefinition(
                id=col,
                column=col,
                label=col.replace("_", " ").title(),
                description=str(meta_cols.get(col, {}).get("description", "")),
            )
    return dims


def _metric_semantics(column: str, meta_col: dict[str, Any]) -> tuple[str, str, list[str]]:
    lower = column.casefold()
    concept = lower
    variant = "value"
    for suffix, variant_name in (
        (" per 1000", "per_1000_residents"),
        ("_per_capita", "per_capita"),
    ):
        if concept.endswith(suffix):
            concept = concept[: -len(suffix)]
            variant = variant_name
            break
    unit = str(meta_col.get("unit") or "").casefold()
    if variant == "value" and unit in {"percent", "percentage", "%"}:
        variant = "percentage"
    elif variant == "value" and unit in {"usd", "dollars", "$"}:
        variant = "total_usd"
    concept = re.sub(r"[^a-z0-9]+", "_", concept).strip("_")
    human = re.sub(r"[_]+", " ", column).replace(",", " ").replace("&", "and")
    aliases = [human.casefold().strip()]
    aliases.extend(_CURATED_METRIC_ALIASES.get(lower, []))
    return concept or lower, variant, list(dict.fromkeys(alias for alias in aliases if alias))


def _metric_unit(column: str, meta_col: dict[str, Any]) -> str:
    """Return a documented or description-derived display unit.

    Source dictionaries occasionally omit a unit while explicitly describing
    a ratio, index, or 0-1 share. Keeping that semantic signal avoids exposing
    the unhelpful generic unit ``value`` to the planner and dataset library.
    """

    declared = str(meta_col.get("unit") or "").strip()
    if declared:
        return declared
    lower = column.casefold()
    description = str(meta_col.get("description") or "").casefold()
    range_text = str(meta_col.get("range") or "").casefold()
    if "ratio" in lower or "ratio" in description:
        return "ratio"
    if range_text in {"0-1", "0–1"}:
        return "proportion (0-1)" if "share" in description else "index (0-1)"
    return "value"


def _metrics(columns: list[str], meta_cols: dict[str, Any]) -> dict[str, MetricDefinition]:
    metrics: dict[str, MetricDefinition] = {}
    for col in columns:
        if not _is_measure(col):
            continue
        meta_col = meta_cols.get(col, {}) if isinstance(meta_cols.get(col), dict) else {}
        sql_name = meta_col.get("sql_name") or quote_identifier(col)
        concept, variant, aliases = _metric_semantics(col, meta_col)
        metrics[col] = MetricDefinition(
            id=col,
            label=_metric_label(col),
            description=str(meta_col.get("description", "")) or col,
            sql=sql_name,
            unit=_metric_unit(col, meta_col),
            aggregation=_aggregation_for(col, meta_col),
            synonyms=aliases,
            semantic_concept=concept,
            semantic_variant=variant,
        )
    groups: dict[str, dict[str, str]] = {}
    for metric in metrics.values():
        groups.setdefault(str(metric.semantic_concept), {})[str(metric.semantic_variant)] = (
            metric.id
        )
    for metric in metrics.values():
        variants = groups.get(str(metric.semantic_concept), {})
        metric.related_variants = {
            variant: metric_id for variant, metric_id in variants.items() if metric_id != metric.id
        }
    return metrics


def _build_dataset(
    table_name: str, manifest: dict[str, Any], metadata: dict[str, Any]
) -> DatasetDefinition:
    info = manifest[table_name]
    meta = metadata.get("tables", {}).get(table_name, {})
    meta_cols = _effective_meta_cols(table_name)
    columns = list(info.get("columns", []))
    geography = str(meta.get("geography") or "").strip() or "state"
    years = _available_years(meta)
    return DatasetDefinition(
        id=table_name,
        display_name=table_name.replace("_", " ").title(),
        description=str(meta.get("description") or f"Curated analytical dataset `{table_name}`."),
        table_name=table_name,
        view_name=mart_view_name(table_name),
        grain=str(meta.get("grain") or f"One row per {geography}."),
        geography=geography,
        family=_family_for(table_name),
        year_column=_year_column(meta, columns),
        default_year=_default_year(table_name, meta, years),
        available_years=years,
        label_column=_label_column(geography, columns),
        dimensions=_dimensions(columns, meta_cols),
        metrics=_metrics(columns, meta_cols),
        columns=columns,
        caveats=critical_warnings_for([table_name]),
    )


@lru_cache(maxsize=1)
def load_registry() -> SemanticRegistrySnapshot:
    manifest, metadata = _raw()
    datasets = {
        table_name: _build_dataset(table_name, manifest, metadata) for table_name in manifest
    }
    return SemanticRegistrySnapshot(version=REGISTRY_VERSION, datasets=datasets)


def get_dataset(dataset_id: str) -> DatasetDefinition | None:
    return load_registry().datasets.get(dataset_id)


def all_allowed_views() -> set[str]:
    return {dataset.view_name for dataset in load_registry().datasets.values()}


# ---------------------------------------------------------------------------
# Prompt-ready catalog helpers (used by the LLM pipeline)
# ---------------------------------------------------------------------------
def _measure_columns(dataset: DatasetDefinition) -> list[str]:
    return [c for c in dataset.columns if _is_measure(c)]


def _key_columns(dataset: DatasetDefinition) -> list[str]:
    return [c for c in dataset.columns if c in _KEY_COLUMNS or c in _YEAR_COLUMNS]


def _year_note(ds: DatasetDefinition) -> str:
    meta = metadata_doc().get("tables", {}).get(ds.id, {})
    if not ds.year_column:
        return "no year column — never add a year filter"
    if ds.default_year is None:
        return (
            f"column `{ds.year_column}` exists but data is a SINGLE snapshot "
            f"({meta.get('year_range') or 'fixed value'}) — never add a year filter"
        )
    return (
        f"`{ds.year_column}` ({meta.get('year_type', 'see column desc')}; "
        f"values: {', '.join(str(y) for y in ds.available_years) or 'n/a'}; "
        f"default: {ds.default_year})"
    )


def catalog_for_prompt(table_ids: set[str] | list[str] | tuple[str, ...] | None = None) -> str:
    """Compact catalog of loaded tables for an LLM prompt.

    One block per table: name, family, geography, grain, year handling, key
    columns, and measure columns. The router receives the full catalog; agents
    that already have an audited route can request only those tables, reducing
    cost and preventing irrelevant sibling schemas from distracting analysis.
    """
    reg = load_registry()
    selected = set(table_ids) if table_ids is not None else None
    lines: list[str] = []
    for ds in reg.datasets.values():
        if selected is not None and ds.id not in selected:
            continue
        year_note = _year_note(ds)
        table_notes = metadata_doc().get("tables", {}).get(ds.id, {}).get("critical_notes") or []
        runtime_note = (
            "\n- runtime limitations: " + " | ".join(str(note) for note in table_notes)
            if table_notes
            else ""
        )
        measures: list[str] = []
        for metric_id in _measure_columns(ds):
            metric = ds.metrics.get(metric_id)
            if metric is None:
                measures.append(metric_id)
                continue
            canonical = re.sub(r"[^a-z0-9]+", " ", metric_id.casefold()).strip()
            aliases = [
                alias
                for alias in metric.synonyms
                if re.sub(r"[^a-z0-9]+", " ", alias.casefold()).strip() != canonical
            ]
            alias_note = f" (also: {', '.join(aliases[:3])})" if aliases else ""
            measures.append(metric_id + alias_note)
        lines.append(
            f"### {ds.id}  [{ds.family} · {ds.geography}]\n"
            f"- {ds.description}\n"
            f"- grain: {ds.grain}\n"
            f"- year: {year_note}\n"
            f"- key columns: {', '.join(_key_columns(ds)) or '(none)'}\n"
            f"- measures: {', '.join(measures) if measures else '(none)'}"
            f"{runtime_note}"
        )
    return "\n\n".join(lines)


@lru_cache(maxsize=32)
def _semantic_catalog_for_verification_cached(selected_ids: tuple[str, ...]) -> str:
    """A description-rich, deduplicated measure index for route auditing.

    The fast router receives a compact table catalog.  A refusal or a wrong
    high-confidence route can still happen when the user's wording occurs only
    in a variable description (for example, "benefit transfers" for Direct
    Payments).  This index keeps all 67 distinct runtime measures, their rich
    descriptions, aliases, units, and the exact tables that physically contain
    them.  It is used by a separate LLM audit; it never chooses an answer or
    manufactures a field.
    """
    registry = load_registry()
    selected = set(selected_ids) if selected_ids else None
    by_metric: dict[str, dict[str, Any]] = {}
    for dataset in registry.datasets.values():
        if selected is not None and dataset.id not in selected:
            continue
        for metric in dataset.metrics.values():
            item = by_metric.setdefault(
                metric.id,
                {
                    "tables": [],
                    "labels": [],
                    "descriptions": [],
                    "aliases": [],
                    "units": [],
                },
            )
            item["tables"].append(dataset.id)
            item["labels"].append(metric.label)
            item["descriptions"].append(metric.description)
            item["aliases"].extend(metric.synonyms)
            item["units"].append(metric.unit)

    table_lines = [
        f"- {dataset.id}: geography={dataset.geography}; year={_year_note(dataset)}; "
        f"dimensions={', '.join(dataset.dimensions) or '(none)'}"
        for dataset in registry.datasets.values()
        if selected is None or dataset.id in selected
    ]
    metric_lines: list[str] = []
    for metric_id, item in by_metric.items():
        description = max(
            (str(value) for value in item["descriptions"] if str(value).strip()),
            key=len,
            default=metric_id,
        )
        aliases = list(dict.fromkeys(str(value) for value in item["aliases"] if str(value).strip()))
        labels = list(dict.fromkeys(str(value) for value in item["labels"] if str(value).strip()))
        units = list(dict.fromkeys(str(value) for value in item["units"] if str(value).strip()))
        metric_lines.append(
            f"- exact column `{metric_id}` | tables={','.join(item['tables'])} | "
            f"label={labels[0] if labels else metric_id} | unit={','.join(units) or 'value'} | "
            f"meaning={description} | aliases={', '.join(aliases)}"
        )
    flow_join = str(
        (metadata_doc().get("cross_dataset_joins") or {}).get("flow_to_other_tables") or ""
    ).strip()
    return (
        "RUNTIME TABLE INDEX\n"
        + "\n".join(table_lines)
        + "\n\nRUNTIME MEASURE INDEX\n"
        + "\n".join(metric_lines)
        + (f"\n\nAUTHORITATIVE CROSS-TABLE KEY\n- {flow_join}" if flow_join else "")
    )


def semantic_catalog_for_verification(
    table_ids: set[str] | list[str] | tuple[str, ...] | None = None,
) -> str:
    """Description-rich measure index, optionally scoped by schema retrieval."""
    selected = tuple(sorted({str(value) for value in table_ids})) if table_ids else ()
    return _semantic_catalog_for_verification_cached(selected)


_FAMILY_BLURB = {
    "demographics": "Census ACS demographics: population, age, race/ethnicity shares, "
    "education attainment, income brackets, median household income, poverty, housing.",
    "government_finance": "State/local government finance (single FY2023 snapshot): "
    "assets, liabilities, pension/OPEB, revenue, expenses, free cash flow, debt & current ratios.",
    "federal_funding": "Federal awards from USAspending: contracts, grants, direct payments, "
    "resident wages, federal employees; also broken out by federal agency.",
    "financial_health": "FINRA NFCS financial-health indices (0–1): financial literacy, "
    "financial constraint/stress, alternative financing, satisfaction, risk aversion.",
    "subaward_flow": "Federal subaward dollar flows between geographies (prime → sub), "
    "by agency and industry.",
}


def domain_summary() -> str:
    """A short description of what the catalog can and cannot answer (Stage 1)."""
    reg = load_registry()
    by_family: dict[str, list[str]] = {}
    for ds in reg.datasets.values():
        by_family.setdefault(ds.family, []).append(ds.id)
    lines: list[str] = []
    for family, tables in by_family.items():
        blurb = _FAMILY_BLURB.get(family, family)
        lines.append(f"- {family}: {blurb}\n  tables: {', '.join(sorted(tables))}")
    return "Geographies: US states, counties, and 118th congressional districts.\n" + "\n".join(
        lines
    )


def table_schema_block(table_name: str) -> str:
    """Detailed per-table grounding block: every column with type / quoting /
    description / sample values, plus year handling and the critical warnings."""
    ds = get_dataset(table_name)
    if ds is None:
        return f"(unknown table: {table_name})"
    meta_cols = _effective_meta_cols(table_name)
    out: list[str] = [
        f"TABLE {table_name}  (view: {ds.view_name})",
        f"  description: {ds.description}",
        f"  grain: {ds.grain}   geography: {ds.geography}",
    ]
    out.append(f"  year: {_year_note(ds)}")
    out.append("  columns:")
    for col in ds.columns:
        mc = meta_cols.get(col, {}) if isinstance(meta_cols.get(col), dict) else {}
        sql_name = mc.get("sql_name")
        ref = f" use-in-sql:{sql_name}" if sql_name else ""
        ctype = mc.get("type", "")
        desc = mc.get("description", "")
        samples = mc.get("sample_values")
        sample_txt = f" e.g. {samples}" if samples else ""
        metric = ds.metrics.get(col)
        semantic = (
            f" measure; unit:{metric.unit}; default-row-aggregation:{metric.aggregation}"
            if metric is not None
            else " dimension"
        )
        out.append(f"    - {col} [{ctype};{semantic.strip()}]{ref}: {desc}{sample_txt}".rstrip())
    warns = critical_warnings_for([table_name])
    if warns:
        out.append("  CRITICAL:")
        out.extend(f"    * {w}" for w in warns)
    return "\n".join(out)


def join_hints_for(tables: list[str]) -> list[str]:
    """Relevant cross-table join patterns when more than one table is involved."""
    if len(tables) < 2:
        return []
    joins = metadata_doc().get("cross_table_joins", {})
    hints: list[str] = [joins.get("warning", "")] if joins.get("warning") else []
    for key, pattern in joins.get("patterns", {}).items():
        hints.append(f"{key}: {pattern}")
    example = joins.get("example_multi_table_query")
    if example:
        hints.append(f"example: {example}")
    return hints
