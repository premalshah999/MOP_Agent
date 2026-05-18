"""Catalog backed by data/schema/metadata.json (the single source of truth).

This module is intentionally thin: it adapts the curated metadata.json + manifest.json
into the DatasetDefinition models the rest of the app consumes, and exposes
prompt-ready catalog helpers used by the LLM-grounded pipeline (intent / routing /
retrieval / generation).

Public API preserved for existing consumers:
  quote_identifier, mart_view_name, load_registry, get_dataset, all_allowed_views
New helpers for the LLM pipeline:
  metadata_doc, catalog_for_prompt, table_schema_block, critical_warnings_for,
  join_hints_for, geographic_keys, common_question_patterns
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
    "state", "county", "cd_118", "fips", "state_fips", "county_fips",
    "agency", "agency_name", "agency_code", "naics", "naics_2digit_code",
    "naics_2digit_title", "naics_2digit",
    "rcpt_st_cd", "rcpt_state_name", "subawardee_st_cd", "subawardee_state_name",
    "rcpt_cty", "subawardee_cty", "rcpt_cty_name", "subawardee_cty_name",
    "rcpt_state", "subawardee_state", "rcpt_full_name", "subawardee_full_name",
    "rcpt_cd_name", "subawardee_cd_name", "prime_awardee_stcd118", "subawardee_stcd118",
    "origin_lat", "origin_lon", "dest_lat", "dest_lon", "Unnamed: 0",
}

_AVG_HINTS = (
    "per_capita", "per capita", "per 1000", "per1000", "ratio", "rate",
    "median", "percent", "%", "index", "share",
)
_DIMENSION_COLUMNS = (
    "state", "county", "cd_118", "fips", "state_fips", "county_fips",
    "agency", "agency_name", "rcpt_state_name", "subawardee_state_name",
    "rcpt_cd_name", "subawardee_cd_name", "naics_2digit_title",
)


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
        return [str(v).strip("'\"") for v in raw]
    rng = meta.get("year_range")
    if isinstance(rng, str) and rng.strip():
        found = [int(tok) for tok in re.findall(r"(?:19|20)\d{2}", rng)]
        is_range = ("–" in rng or "-" in rng) and "," not in rng and len(found) == 2
        if is_range:
            lo, hi = sorted(found)
            return list(range(lo, hi + 1))
        return found or [rng]
    return []


def _default_year(table_name: str, meta: dict[str, Any], years: list[str | int]) -> str | int | None:
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
            "(single year) and '2020-2024' (5-year aggregate). You MUST filter "
            "to exactly one period — default to year = '2024' unless the user "
            "explicitly asks for the multi-year/5-year total. NEVER omit the "
            "year filter and NEVER SUM across both rows (that double counts).",
        )

    gov = warnings.get("gov_year_label", {})
    for t in ("gov_state", "gov_county", "gov_congress"):
        add(t, gov.get("note", "Single year only.") + " " + gov.get("sql_fix", "Do not filter by year."))

    for t in ("finra_state",):
        add(t, "Survey waves only: 2009, 2012, 2015, 2018, 2021. No other years exist.")
    for t in ("finra_county", "finra_congress"):
        add(t, "Only year 2021 exists.")

    sfd = warnings.get("state_flow_duplicate_columns", {})
    add("state_flow", sfd.get("fix", "Use rcpt_state_name / subawardee_state_name.") +
        " state_flow has NO year column (lifetime aggregate). subaward_amount can be negative (clawbacks).")

    cfi = warnings.get("congress_flow_integer_district_id", {})
    add("congress_flow", cfi.get("fix", "Use rcpt_cd_name; never join the integer district id to cd_118."))

    cff = warnings.get("county_flow_fips_as_integer", {})
    add("county_flow", cff.get("note", "rcpt_cty / subawardee_cty are integer FIPS codes."))

    # Junk pandas index column present in the flow exports.
    manifest, _ = _raw()
    for table, info in manifest.items():
        if "Unnamed: 0" in info.get("columns", []):
            add(table, 'Never SELECT or aggregate the "Unnamed: 0" column — it is a junk row index, not data.')

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
    if not canonical_id or canonical_id == table_name:
        return {k: dict(v) for k, v in own.items() if isinstance(v, dict)}
    canon = tables.get(canonical_id, {}).get("columns", {})
    canon = canon if isinstance(canon, dict) else {}
    merged: dict[str, Any] = {}
    for col, meta in own.items():
        meta = dict(meta) if isinstance(meta, dict) else {}
        ref = canon.get(col) if isinstance(canon.get(col), dict) else {}
        for field in ("description", "unit", "sql_name", "range", "sample_values"):
            if not meta.get(field) and ref.get(field):
                meta[field] = ref[field]
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


def _metrics(columns: list[str], meta_cols: dict[str, Any]) -> dict[str, MetricDefinition]:
    metrics: dict[str, MetricDefinition] = {}
    for col in columns:
        if not _is_measure(col):
            continue
        meta_col = meta_cols.get(col, {}) if isinstance(meta_cols.get(col), dict) else {}
        sql_name = meta_col.get("sql_name") or quote_identifier(col)
        metrics[col] = MetricDefinition(
            id=col,
            label=col.replace("_", " ").strip(),
            description=str(meta_col.get("description", "")) or col,
            sql=sql_name,
            unit=str(meta_col.get("unit", "value")),
            aggregation=_aggregation_for(col, meta_col),
        )
    return metrics


def _build_dataset(table_name: str, manifest: dict[str, Any], metadata: dict[str, Any]) -> DatasetDefinition:
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
        table_name: _build_dataset(table_name, manifest, metadata)
        for table_name in manifest
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


def catalog_for_prompt() -> str:
    """Compact catalog of every loaded table for the routing LLM.

    One block per table: name, family, geography, grain, year handling, key
    columns, and measure columns. Token-efficient but complete enough to route.
    """
    reg = load_registry()
    lines: list[str] = []
    for ds in reg.datasets.values():
        year_note = _year_note(ds)
        measures = _measure_columns(ds)
        lines.append(
            f"### {ds.id}  [{ds.family} · {ds.geography}]\n"
            f"- {ds.description}\n"
            f"- grain: {ds.grain}\n"
            f"- year: {year_note}\n"
            f"- key columns: {', '.join(_key_columns(ds)) or '(none)'}\n"
            f"- measures: {', '.join(measures) if measures else '(none)'}"
        )
    return "\n\n".join(lines)


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
    return (
        "Geographies: US states, counties, and 118th congressional districts.\n"
        + "\n".join(lines)
    )


def table_schema_block(table_name: str) -> str:
    """Detailed per-table grounding block: every column with type / quoting /
    description / sample values, plus year handling and the critical warnings."""
    ds = get_dataset(table_name)
    if ds is None:
        return f"(unknown table: {table_name})"
    meta = metadata_doc().get("tables", {}).get(table_name, {})
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
        out.append(f"    - {col} [{ctype}]{ref}: {desc}{sample_txt}".rstrip())
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
