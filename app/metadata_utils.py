from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent
METADATA_PATH = ROOT_DIR / "data" / "schema" / "metadata.json"
MANIFEST_PATH = ROOT_DIR / "data" / "schema" / "manifest.json"

_SPECIAL_CHARS = re.compile(r"[\s,#&><\-]")
_ID_COLUMNS = {
    "state",
    "state_fips",
    "county",
    "county_fips",
    "fips",
    "cd_118",
    "year",
    "Year",
    "agency",
    "agency_code",
    "rcpt_state_name",
    "subawardee_state_name",
    "rcpt_state",
    "subawardee_state",
    "rcpt_cty",
    "subawardee_cty",
    "rcpt_cty_name",
    "subawardee_cty_name",
    "rcpt_cd_name",
    "subawardee_cd_name",
    "rcpt_full_name",
    "subawardee_full_name",
    "origin_lat",
    "origin_lon",
    "dest_lat",
    "dest_lon",
    "naics_2digit_code",
    "naics_2digit_title",
    "Unnamed: 0",
    "prime_awardee_stcd118",
    "subawardee_stcd118",
    "rcpt_st_cd",
    "subawardee_st_cd",
    "act_dt_fis_yr",
}
_MONEY_COLUMNS = {
    "Contracts",
    "Grants",
    "Resident Wage",
    "Direct Payments",
    "Employees Wage",
    "subaward_amount",
    "subaward_amount_year",
    "Total_Liabilities",
    "Current_Assets",
    "Compensated_Absences",
    "Net_Position",
    "Total_Assets",
    "Non-Current_Liabilities",
    "Bonds,_Loans_&_Notes",
    "Current_Liabilities",
    "Net_Pension_Liability",
    "Free_Cash_Flow",
    "Expenses",
    "Net_OPEB_Liability",
    "Revenue",
}
_COUNT_COLUMNS = {"Federal Residents", "Employees"}
_AGENCY_SPENDING_COMPONENTS = ("Contracts", "Grants", "Resident Wage")
_DEFAULT_SPENDING_COMPONENTS = ("Contracts", "Grants", "Resident Wage")


@lru_cache(maxsize=1)
def load_metadata() -> dict[str, Any]:
    with METADATA_PATH.open() as f:
        return json.load(f)


@lru_cache(maxsize=1)
def load_manifest() -> dict[str, Any]:
    with MANIFEST_PATH.open() as f:
        return json.load(f)


def available_tables() -> set[str]:
    return set(load_manifest().keys())


def table_metadata(table_name: str) -> dict[str, Any]:
    return load_metadata().get("tables", {}).get(table_name, {})


def table_columns(table_name: str) -> list[str]:
    manifest = load_manifest()
    info = manifest.get(table_name)
    if info and "columns" in info:
        return list(info["columns"])
    return list(table_metadata(table_name).get("columns", {}).keys())


def column_metadata(table_name: str, column_name: str) -> dict[str, Any]:
    return table_metadata(table_name).get("columns", {}).get(column_name, {})


def quote_identifier(name: str) -> str:
    sql_name = None
    for table in load_metadata().get("tables", {}).values():
        col_meta = table.get("columns", {}).get(name)
        if col_meta and col_meta.get("sql_name"):
            sql_name = col_meta["sql_name"]
            break
    if sql_name:
        return sql_name
    if _SPECIAL_CHARS.search(name):
        return f'"{name}"'
    return name


def column_requires_quotes(name: str) -> bool:
    return quote_identifier(name) != name


def table_year_column(table_name: str) -> str | None:
    info = table_metadata(table_name)
    if info.get("year_column"):
        return info["year_column"]
    cols = table_columns(table_name)
    if "year" in cols:
        return "year"
    if "Year" in cols:
        return "Year"
    if "act_dt_fis_yr" in cols:
        return "act_dt_fis_yr"
    return None


def default_year_value(table_name: str) -> str | int | None:
    if table_name.startswith("acs_"):
        return 2023
    if table_name == "finra_state":
        return 2021
    if table_name in {"finra_county", "finra_congress"}:
        return 2021
    if table_name.startswith("contract_"):
        return "2024"
    if table_name == "spending_state":
        return "2024"
    if table_name == "spending_state_agency":
        return "2024"
    if table_name in {"county_flow", "congress_flow"}:
        return 2024
    return None


def default_year_filter(table_name: str) -> str | None:
    year_col = table_year_column(table_name)
    default_value = default_year_value(table_name)
    if not year_col or default_value is None:
        return None
    value = f"'{default_value}'" if isinstance(default_value, str) else str(default_value)
    return f"{quote_identifier(year_col)} = {value}"


def state_casing(table_name: str) -> str | None:
    return table_metadata(table_name).get("state_name_casing")


def geography_key(table_name: str) -> str | None:
    cols = table_columns(table_name)
    for candidate in ("state", "county", "fips", "cd_118", "agency", "rcpt_state_name", "rcpt_state"):
        if candidate in cols:
            return candidate
    return None


def is_identifier_column(column_name: str) -> bool:
    return column_name in _ID_COLUMNS or column_name.endswith("Per 1000")


def monetary_columns(table_name: str) -> list[str]:
    cols = table_columns(table_name)
    return [col for col in cols if col in _MONEY_COLUMNS]


def count_columns(table_name: str) -> list[str]:
    cols = table_columns(table_name)
    return [col for col in cols if col in _COUNT_COLUMNS]


def default_spending_component_columns(table_name: str) -> list[str]:
    cols = set(table_columns(table_name))
    return [col for col in _DEFAULT_SPENDING_COMPONENTS if col in cols]


def default_spending_total_expression(table_name: str, alias: str = "spending_total") -> str | None:
    cols = default_spending_component_columns(table_name)
    if len(cols) != len(_DEFAULT_SPENDING_COMPONENTS):
        return None
    expr = " + ".join(f"COALESCE({quote_identifier(col)}, 0)" for col in cols)
    return f"({expr}) AS {alias}"


def agency_spending_total_expression(table_name: str, alias: str = "spending_total") -> str | None:
    cols = [col for col in _AGENCY_SPENDING_COMPONENTS if col in table_columns(table_name)]
    if len(cols) != len(_AGENCY_SPENDING_COMPONENTS):
        return None
    expr = " + ".join(f"COALESCE({quote_identifier(col)}, 0)" for col in cols)
    return f"({expr}) AS {alias}"


def relevant_warnings(table_names: list[str]) -> list[str]:
    warnings = load_metadata().get("_critical_warnings", {})
    selected = set(table_names)
    messages: list[str] = []
    for key, info in warnings.items():
        affected = set(info.get("tables_affected", []))
        if affected and not (selected & affected):
            continue
        text = info.get("problem") or info.get("note")
        fix = info.get("fix") or info.get("sql_fix")
        if text:
            msg = text
            if fix:
                msg += f" Fix: {fix}"
            messages.append(msg)
    return messages


def normalize_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def tokenize(text: str) -> set[str]:
    return {token for token in normalize_text(text).split() if token}


def column_match_score(table_name: str, column_name: str, question: str) -> float:
    q_tokens = tokenize(question)
    if not q_tokens:
        return 0.0

    col_meta = column_metadata(table_name, column_name)
    name_text = normalize_text(column_name)
    desc_text = normalize_text(col_meta.get("description", ""))
    tokens = tokenize(f"{name_text} {desc_text}")
    score = 0.0

    if name_text and name_text in normalize_text(question):
        score += 6.0
    if col_meta.get("description") and desc_text and desc_text in normalize_text(question):
        score += 2.0

    score += len(q_tokens & tokens)

    if "per capita" in normalize_text(question) and "per capita" in name_text:
        score += 4.0
    if "per 1000" in normalize_text(question) and "per 1000" in name_text:
        score += 4.0
    if "poverty" in q_tokens and "below poverty" in name_text:
        score += 3.0
    if "income" in q_tokens and "median household income" in name_text:
        score += 3.0
    if "debt" in q_tokens and column_name == "Debt_Ratio":
        score += 4.0
    if "pension" in q_tokens and "Net_Pension_Liability" in column_name:
        score += 4.0
    if "liabilities" in q_tokens and "Total_Liabilities" in column_name:
        score += 3.0
    if "contracts" in q_tokens and column_name == "Contracts":
        score += 4.0
    if "grants" in q_tokens and column_name == "Grants":
        score += 4.0
    if "payments" in q_tokens and column_name == "Direct Payments":
        score += 4.0

    return score
