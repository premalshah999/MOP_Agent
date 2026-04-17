"""LLM-assisted table routing + geographic utilities for the MOP agent."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Optional

from app.llm import llm_available, llm_complete, llm_model
from app.metadata_utils import (
    agency_spending_total_expression,
    available_tables,
    count_columns,
    default_year_filter,
    default_spending_total_expression,
    load_metadata,
    monetary_columns,
    relevant_warnings,
    table_columns,
    table_metadata,
    table_year_column,
)
from app.query_frame import (
    STATE_TO_POSTAL,
    US_STATE_NAMES,
    detect_geo_level as detect_geo_level_from_frame,
    extract_period_label,
    extract_state_name as extract_state_name_from_frame,
    infer_query_frame,
    state_postal_code as state_postal_code_from_frame,
)

_log = logging.getLogger(__name__)

METADATA_PATH = Path("data/schema/metadata.json")
MANIFEST_PATH = Path("data/schema/manifest.json")

METADATA = load_metadata()

# ---------------------------------------------------------------------------
# Geography helpers
# ---------------------------------------------------------------------------
def detect_geo_level(question: str) -> Optional[str]:
    return detect_geo_level_from_frame(question)


def extract_state_name(question: str) -> Optional[str]:
    return extract_state_name_from_frame(question)


def state_postal_code(question: str) -> Optional[str]:
    return state_postal_code_from_frame(question)


def extract_year(question: str) -> Optional[str]:
    period = extract_period_label(question)
    if period and period.isdigit():
        return period
    return None


# ---------------------------------------------------------------------------
# Available tables from manifest
# ---------------------------------------------------------------------------
def _available_tables() -> set[str]:
    return available_tables()


# ---------------------------------------------------------------------------
# Compact table catalog for the LLM routing call
# ---------------------------------------------------------------------------
TABLE_CATALOG = """\
Tables available (pick 1-4 needed to answer the question):
- acs_state: ACS demographics by state (population, race, income, poverty, education). Year: int 2010-2023
- acs_county: same as acs_state but by county (fips key). Year: int 2010-2023
- acs_congress: same by congressional district (cd_118 key). Year: int 2010-2023
- gov_state: government finance by state (revenue, expenses, assets, liabilities, debt ratio, pension, OPEB). Single year FY2023
- gov_county: same by county (fips key). Single year FY2023
- gov_congress: same by congressional district (cd_118 key). Single year FY2023
- contract_state: federal contracts/grants/spending by state. Year: string '2024'
- contract_county: same by county (fips key). Year: string '2024'
- contract_congress: same by congressional district (cd_118 key). Year: string '2024'
- finra_state: FINRA financial literacy/constraint survey by state. Year: int (2009,2012,2015,2018,2021)
- finra_county: same by county (fips key). Year: int 2021 only
- finra_congress: same by congressional district. Year: int 2021 only
- spending_state: federal spending breakdown by state. Year: string '2024'
- spending_state_agency: federal spending by state + agency (21 agencies). Year: string '2024'
- state_flow: inter-state subaward fund flows (no year column)
- county_flow: inter-county subaward flows with act_dt_fis_yr (fiscal year)
- congress_flow: inter-district subaward flows with act_dt_fis_yr"""


# ---------------------------------------------------------------------------
# Keyword-based pre-filter (fast fallback, no LLM)
# ---------------------------------------------------------------------------
def _keyword_route(question: str) -> list[str]:
    """Fast keyword-based table routing as fallback."""
    frame = infer_query_frame(question)
    geo = frame.geo_level
    family = frame.family

    tables: set[str] = set()

    def add_geo(prefix: str) -> None:
        if geo == "county":
            tables.add(f"{prefix}_county")
        elif geo == "congress":
            tables.add(f"{prefix}_congress")
        elif geo == "state":
            tables.add(f"{prefix}_state")
        else:
            tables.add(f"{prefix}_state")

    if family in ("acs", "gov", "finra", "contract"):
        add_geo(family)
    elif family == "agency":
        if geo == "county":
            tables.add("contract_county_agency")
        elif geo == "congress":
            tables.add("contract_cd_agency")
        else:
            tables.add("spending_state_agency")
    elif family == "breakdown":
        if frame.wants_agency_dimension or frame.wants_jobs_metric:
            tables.add("spending_state_agency")
        else:
            tables.add("spending_state")
    elif family == "flow":
        if frame.intent == "trend":
            tables.add("county_flow" if geo == "county" else "congress_flow" if geo == "congress" else "county_flow")
        else:
            tables.add("state_flow" if geo not in {"county", "congress"} else f"{'county' if geo == 'county' else 'congress'}_flow")
    else:
        tables.update({"gov_state", "acs_state"})

    available = _available_tables()
    return sorted(t for t in tables if t in available)


# ---------------------------------------------------------------------------
# LLM-assisted table routing
# ---------------------------------------------------------------------------
def route_tables(question: str) -> list[str]:
    """Select the right tables for a question. Uses LLM when available, keyword fallback otherwise."""
    deterministic = _keyword_route(question)
    if deterministic:
        return deterministic

    if not llm_available():
        return deterministic

    try:
        raw = llm_complete(
            [
                {
                    "role": "system",
                    "content": (
                        "You select database tables needed to answer a user question. "
                        "Return ONLY a JSON array of table names. No explanation.\n\n"
                        "Prefer the smallest set of tables possible. "
                        "Never invent tables that are not listed.\n\n"
                        + TABLE_CATALOG
                    ),
                },
                {"role": "user", "content": question},
            ],
            model=llm_model(),
            temperature=0,
            max_tokens=100,
        )
        # Parse JSON array from response
        match = re.search(r"\[.*\]", raw, re.DOTALL)
        if match:
            tables = json.loads(match.group())
            available = _available_tables()
            result = [t for t in tables if isinstance(t, str) and t in available]
            if result:
                return result
    except Exception:
        pass

    return _keyword_route(question)


# ---------------------------------------------------------------------------
# Sample rows cache (populated lazily on first use)
# ---------------------------------------------------------------------------
_sample_rows_cache: dict[str, str] = {}


def _get_sample_rows(table_name: str, limit: int = 3) -> str:
    """Fetch and cache sample rows for a table. Returns formatted string or empty on error."""
    if table_name in _sample_rows_cache:
        return _sample_rows_cache[table_name]
    try:
        from app.db import execute_query
        df = execute_query(f"SELECT * FROM {table_name} LIMIT {limit}")
        result = df.to_string(index=False, max_colwidth=30)
        _sample_rows_cache[table_name] = result
        return result
    except Exception as exc:
        _log.debug("Failed to fetch sample rows for %s: %s", table_name, exc)
        _sample_rows_cache[table_name] = ""
        return ""


# ---------------------------------------------------------------------------
# Schema context builder
# ---------------------------------------------------------------------------
# Join path hints per table pair — helps LLM pick the right key
_JOIN_HINTS: dict[tuple[str, str], str] = {}

def _build_join_hints() -> None:
    """Populate join hints for common cross-table pairs."""
    if _JOIN_HINTS:
        return
    state_tables = ["acs_state", "gov_state", "contract_state", "spending_state", "spending_state_agency", "finra_state"]
    county_tables = ["acs_county", "gov_county", "contract_county", "finra_county"]
    congress_tables = ["acs_congress", "gov_congress", "contract_congress", "finra_congress"]

    for i, a in enumerate(state_tables):
        for b in state_tables[i+1:]:
            _JOIN_HINTS[(a, b)] = "LOWER(a.state) = LOWER(b.state)"
    for i, a in enumerate(county_tables):
        for b in county_tables[i+1:]:
            _JOIN_HINTS[(a, b)] = "a.fips = b.fips"
    for i, a in enumerate(congress_tables):
        for b in congress_tables[i+1:]:
            _JOIN_HINTS[(a, b)] = "a.cd_118 = b.cd_118"


def build_schema_context(table_names: list[str]) -> str:
    """Build column-level schema context for the selected tables, including sample rows and join hints."""
    _build_join_hints()
    available = _available_tables()
    tables = METADATA.get("tables", {})
    selected = {k: v for k, v in tables.items() if k in table_names and k in available}

    if not selected:
        selected = {k: v for k, v in tables.items() if k in available}

    lines = ["SCHEMA FOR SELECTED TABLES:\n"]
    lines.extend(
        [
            "CANONICAL CHATBOT RULES:",
            "- Treat generic federal spending questions as channel-based unless a dashboard composite is explicitly defined.",
            "- For default spending composition in contract/spending tables, use Contracts + Grants + \"Resident Wage\".",
            "- Do not silently fold in \"Direct Payments\", \"Federal Residents\", Employees, or \"Employees Wage\" unless the user explicitly asks for them.",
            "- Counts are not dollars, and per-1000/per-capita columns are already normalized metrics.",
            "- Use stored \"Per 1000\" and `_per_capita` fields directly when the user asks for them; do not silently recompute them from totals.",
            "- Be honest about provenance: separate runtime behavior verified in code from semantic descriptions documented in metadata or docs.",
            "- Treat '2020-2024' as a separate aggregate period label, not the same thing as single-year 2024.",
            "",
        ]
    )

    warnings = relevant_warnings(list(selected.keys()))
    if warnings:
        lines.append("CRITICAL WARNINGS:")
        for warning in warnings:
            lines.append(f"- {warning}")
        lines.append("")

    for name, info in selected.items():
        lines.append(f"TABLE: {name}")
        lines.append(f"  Description: {info.get('description', 'n/a')}")
        lines.append(f"  Grain: {info.get('grain', 'n/a')}")
        lines.append(f"  Rows: {info.get('row_count', '?')}")

        # Separate columns into key columns and data columns for clarity
        key_cols = []
        data_cols = []
        for col, meta in info.get("columns", {}).items():
            col_type = meta.get("type", "?")
            entry = f"{col} ({col_type})"
            if col.lower() in ("state", "county", "fips", "cd_118", "year", "agency",
                               "rcpt_state_name", "subawardee_state_name", "rcpt_cd_name",
                               "subawardee_cd_name", "rcpt_state", "subawardee_state",
                               "act_dt_fis_yr", "agency_name", "county_fips"):
                key_cols.append(entry)
            else:
                data_cols.append(entry)
        if key_cols:
            lines.append(f"  Key columns: {', '.join(key_cols)}")
        lines.append(f"  Data columns: {', '.join(data_cols)}")

        year_col = table_year_column(name)
        if year_col:
            lines.append(f"  Year column: {year_col}")
        if info.get("year_type"):
            lines.append(f"  Year handling: {info['year_type']}")
        year_filter = default_year_filter(name)
        if year_filter:
            lines.append(f"  Default year filter when unspecified: {year_filter}")
        if info.get("state_name_casing"):
            lines.append(f"  State casing: {info['state_name_casing']}")

        money_cols = monetary_columns(name)
        if money_cols:
            lines.append(f"  Monetary columns: {', '.join(money_cols)}")
        count_cols = count_columns(name)
        if count_cols:
            lines.append(f"  Count columns (do not add into dollar totals): {', '.join(count_cols)}")
        total_formula = agency_spending_total_expression(name, alias="spending_total") if name == "spending_state_agency" else default_spending_total_expression(name, alias="spending_total")
        if total_formula:
            if name == "spending_state_agency":
                lines.append(
                    "  Agency spending default: when the user asks for top agencies by spending, use "
                    f"{total_formula} and exclude Direct Payments, Federal Residents, Employees, and Employees Wage unless explicitly requested."
                )
            else:
                lines.append(
                    "  Broad spending rule: if the user asks for federal spending without naming a component, "
                    f"use {total_formula}; do not add Direct Payments, Federal Residents, Employees, Employees Wage, count columns, or per-1000 columns unless explicitly requested."
                )

        # Sample rows — helps LLM understand actual data format, casing, types
        sample = _get_sample_rows(name)
        if sample:
            lines.append(f"  Sample rows:")
            for row_line in sample.split("\n")[:4]:  # header + 3 rows
                lines.append(f"    {row_line}")
        lines.append("")

    # Add join hints if multiple tables selected
    if len(selected) > 1:
        names = list(selected.keys())
        hints = []
        for i, a in enumerate(names):
            for b in names[i+1:]:
                key = (a, b) if (a, b) in _JOIN_HINTS else (b, a)
                if key in _JOIN_HINTS:
                    hints.append(f"  {a} ↔ {b}: ON {_JOIN_HINTS[key]}")
        if hints:
            lines.append("JOIN PATHS:")
            lines.extend(hints)
            lines.append("")

    return "\n".join(lines)
