"""LLM-assisted table routing + geographic utilities for the MOP agent."""

from __future__ import annotations

import json
import logging
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from openai import OpenAI

_log = logging.getLogger(__name__)

METADATA_PATH = Path("data/schema/metadata.json")
MANIFEST_PATH = Path("data/schema/manifest.json")

with METADATA_PATH.open() as _f:
    METADATA = json.load(_f)

# ---------------------------------------------------------------------------
# State / geography constants
# ---------------------------------------------------------------------------
US_STATE_NAMES = [
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "district of columbia", "florida", "georgia",
    "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky",
    "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada", "new hampshire",
    "new jersey", "new mexico", "new york", "north carolina", "north dakota",
    "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
    "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "west virginia", "wisconsin", "wyoming",
]

STATE_TO_POSTAL = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "district of columbia": "DC", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH",
    "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}


# ---------------------------------------------------------------------------
# Geography helpers
# ---------------------------------------------------------------------------
def detect_geo_level(question: str) -> Optional[str]:
    q = question.lower()
    if "county" in q or "counties" in q:
        return "county"
    if "district" in q or "congress" in q:
        return "congress"
    if "state" in q or "states" in q:
        return "state"
    return None


def extract_state_name(question: str) -> Optional[str]:
    q = question.lower()
    for state in sorted(US_STATE_NAMES, key=len, reverse=True):
        if re.search(rf"\b{re.escape(state)}\b", q):
            return state
    return None


def state_postal_code(question: str) -> Optional[str]:
    state = extract_state_name(question)
    return STATE_TO_POSTAL.get(state) if state else None


def extract_year(question: str) -> Optional[str]:
    match = re.search(r"\b(19\d{2}|20\d{2})\b", question)
    return match.group(1) if match else None


# ---------------------------------------------------------------------------
# Available tables from manifest
# ---------------------------------------------------------------------------
def _available_tables() -> set[str]:
    if not MANIFEST_PATH.exists():
        return set(METADATA.get("tables", {}).keys())
    with MANIFEST_PATH.open() as f:
        manifest = json.load(f)
    return set(manifest.keys())


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
    q = question.lower()
    geo = detect_geo_level(question)
    datasets: set[str] = set()

    if any(k in q for k in ["census", "acs", "demographic", "population", "poverty", "education", "income", "race", "hispanic", "household"]):
        datasets.add("acs")
    if any(k in q for k in ["government finance", "liabilities", "assets", "net position", "pension", "opeb", "debt", "current ratio", "free cash flow", "revenue", "expenses"]):
        datasets.add("gov")
    if any(k in q for k in ["finra", "financial literacy", "financial constraint", "alternative financing", "risk averse"]):
        datasets.add("finra")
    if any(k in q for k in ["federal spending", "contracts", "grants", "direct payments", "resident wage", "federal residents", "employment", "employees"]):
        datasets.add("contract")
    if any(k in q for k in ["agency", "department of", "defense", "treasury"]):
        datasets.add("spending_state_agency")
    if any(k in q for k in ["fund flow", "subaward", "subawardee", "flow", "awardee"]):
        datasets.add("flow")

    # Default: gov + acs (most common)
    if not datasets:
        datasets.update({"gov", "acs"})

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

    for ds in datasets:
        if ds in ("acs", "gov", "finra", "contract"):
            add_geo(ds)
        elif ds == "spending_state_agency":
            tables.add("spending_state_agency")
        elif ds == "flow":
            has_time = any(k in q for k in ["fiscal year", "by year", "over time", "trend"])
            if has_time:
                tables.add("county_flow" if geo == "county" else "congress_flow" if geo == "congress" else "county_flow")
            else:
                tables.add("state_flow" if geo != "county" and geo != "congress" else f"{'county' if geo == 'county' else 'congress'}_flow")

    available = _available_tables()
    return sorted(t for t in tables if t in available)


# ---------------------------------------------------------------------------
# LLM-assisted table routing
# ---------------------------------------------------------------------------
def route_tables(question: str) -> list[str]:
    """Select the right tables for a question. Uses LLM when available, keyword fallback otherwise."""
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        return _keyword_route(question)

    try:
        client = OpenAI(
            api_key=api_key,
            base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
            timeout=float(os.getenv("DEEPSEEK_TIMEOUT", "15")),
        )
        response = client.chat.completions.create(
            model=os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
            temperature=0,
            max_tokens=100,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You select database tables needed to answer a user question. "
                        "Return ONLY a JSON array of table names. No explanation.\n\n"
                        + TABLE_CATALOG
                    ),
                },
                {"role": "user", "content": question},
            ],
        )
        raw = (response.choices[0].message.content or "").strip()
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
    state_tables = ["acs_state", "gov_state", "contract_state", "spending_state",
                    "spending_state_agency", "finra_state", "contract_state_agency"]
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

        if info.get("year_type"):
            lines.append(f"  Year handling: {info['year_type']}")
        if info.get("state_name_casing"):
            lines.append(f"  State casing: {info['state_name_casing']}")

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
