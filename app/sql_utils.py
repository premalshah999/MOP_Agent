"""SQL postprocessing utilities — extract, quote, fix, prepare."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

from app.metadata_utils import available_tables, column_requires_quotes, table_columns

METADATA_PATH = Path("data/schema/metadata.json")

with METADATA_PATH.open() as _f:
    _METADATA = json.load(_f)

_CRITICAL_WARNINGS = _METADATA.get("_critical_warnings", {})


# ---------------------------------------------------------------------------
# Extract SQL from LLM response (strips markdown fences, preamble)
# ---------------------------------------------------------------------------
def extract_sql(text: str) -> str:
    if not text:
        return ""
    # Check for fenced code block
    fenced = re.search(r"```(?:sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    # Find the first WITH or SELECT
    upper = text.upper()
    with_idx = upper.find("WITH")
    select_idx = upper.find("SELECT")
    if with_idx != -1 and (select_idx == -1 or with_idx < select_idx):
        return text[with_idx:].strip()
    if select_idx != -1:
        return text[select_idx:].strip()
    return text.strip()


# ---------------------------------------------------------------------------
# Auto-quote columns with spaces or special characters
# ---------------------------------------------------------------------------
def auto_quote_columns(sql: str) -> str:
    columns: list[str] = []
    for table_name in available_tables():
        for col in table_columns(table_name):
            if column_requires_quotes(col):
                columns.append(col)

    if not columns:
        return sql

    for col in sorted(set(columns), key=len, reverse=True):
        quoted = f'"{col}"'
        pattern = r'(?<!")' + re.escape(col) + r'(?!")'
        sql = re.sub(pattern, quoted, sql)
    return sql


# ---------------------------------------------------------------------------
# Fix year = 2024 -> year = '2024' for contract/spending tables
# ---------------------------------------------------------------------------
def auto_fix_year_string(sql: str) -> str:
    tables = _CRITICAL_WARNINGS.get("year_as_string_in_contract_tables", {}).get("tables_affected", [])
    if not tables:
        return sql
    if not any(re.search(rf"\b{re.escape(t)}\b", sql, flags=re.IGNORECASE) for t in tables):
        return sql
    sql = re.sub(r"(?<!\")\byear\s*=\s*(\d{4})\b", r"year = '\1'", sql)
    return sql


def auto_fix_known_schema_mismatches(sql: str) -> str:
    spending_state_only = re.search(r"\bspending_state\b", sql, flags=re.IGNORECASE) and not re.search(
        r"\bspending_state_agency\b", sql, flags=re.IGNORECASE
    )
    if spending_state_only:
        sql = re.sub(r"\byear\b", "Year", sql, flags=re.IGNORECASE)

    contract_or_agency_tables = any(
        re.search(rf"\b{table}\b", sql, flags=re.IGNORECASE)
        for table in ("contract_state", "contract_county", "contract_congress", "spending_state_agency")
    )
    if contract_or_agency_tables:
        sql = re.sub(r"\bYear\b", "year", sql)

    return sql


# ---------------------------------------------------------------------------
# Detect if the question asks for a ranking (for LIMIT injection)
# ---------------------------------------------------------------------------
def is_ranking_question(question: str) -> bool:
    q = question.lower()
    keywords = ["highest", "lowest", "most", "least", "largest", "smallest", "top", "bottom", "rank", "leading"]
    return any(k in q for k in keywords)


# ---------------------------------------------------------------------------
# Detect explicit top-K from question
# ---------------------------------------------------------------------------
def detect_explicit_k(question: str) -> Optional[int]:
    q = question.lower()
    # "top 10 states", "which 5 counties"
    match = re.search(r"\b(?:top|bottom)\s+(\d{1,3})\b", q)
    if match:
        return int(match.group(1))
    match = re.search(r"\b(?:which|show|list|compare)\s+(\d{1,3})\s+(?:states|counties|districts|agencies|rows)\b", q)
    if match:
        return int(match.group(1))
    match = re.search(r"\b(\d{1,3})\s+(?:highest|lowest|largest|smallest|most|least)\b", q)
    if match:
        return int(match.group(1))
    # Word numbers
    words = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7,
             "eight": 8, "nine": 9, "ten": 10, "fifteen": 15, "twenty": 20}
    match = re.search(r"\b(?:top|bottom)\s+(" + "|".join(words) + r")\b", q)
    if match:
        return words[match.group(1)]
    return None


def ranking_top_k(question: str, default_k: int = 15, max_k: int = 50) -> int:
    explicit = detect_explicit_k(question)
    k = explicit if explicit is not None else default_k
    return min(max(k, 1), max_k)


# ---------------------------------------------------------------------------
# Apply LIMIT to SQL if it's a ranking question
# ---------------------------------------------------------------------------
def apply_limit(sql: str, top_k: int) -> str:
    if not sql:
        return sql
    match = re.search(r"\bLIMIT\s+(\d+)\b", sql, flags=re.IGNORECASE)
    if match:
        existing = int(match.group(1))
        if existing == top_k:
            return sql
        return re.sub(r"\bLIMIT\s+\d+\b", f"LIMIT {top_k}", sql, flags=re.IGNORECASE)
    if re.search(r"\bORDER\s+BY\b", sql, flags=re.IGNORECASE):
        if sql.strip().endswith(";"):
            return sql.strip()[:-1] + f" LIMIT {top_k};"
        return sql.strip() + f" LIMIT {top_k}"
    return sql


# ---------------------------------------------------------------------------
# Full prepare pipeline
# ---------------------------------------------------------------------------
def prepare_sql(sql: str, question: str) -> str:
    """Extract, quote, fix year types, and apply LIMIT as needed."""
    sql = extract_sql(sql).strip()
    if not sql:
        return ""
    if is_ranking_question(question):
        sql = apply_limit(sql, ranking_top_k(question))
    sql = auto_quote_columns(sql)
    sql = auto_fix_known_schema_mismatches(sql)
    sql = auto_fix_year_string(sql)
    return sql
