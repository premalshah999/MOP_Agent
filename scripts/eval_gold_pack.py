from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import statistics
import subprocess
import sys
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import duckdb
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

AVAILABLE_TABLES = set()
ORACLE_CONN: duckdb.DuckDBPyConnection | None = None


@dataclass
class GoldCase:
    id: int
    section: str
    difficulty: str
    focus: str
    question: str
    expected_tables: list[str] = field(default_factory=list)
    expected_table: str | None = None
    expected_geo: str | None = None
    expected_year: str | None = None
    expected_metric: str | None = None
    expected_metric_kind: str | None = None
    expected_limit: int | None = None
    oracle_sql: str | None = None
    comparison_mode: str | None = None
    caution_expected: str | None = None
    answer_must_include: list[str] = field(default_factory=list)
    answer_should_include_any: list[str] = field(default_factory=list)
    answer_must_not_include: list[str] = field(default_factory=list)
    expected_support: str = "supported"
    notes: str = ""


def qcol(name: str) -> str:
    return f'"{name}"'


def lower_eq(column: str, value: str) -> str:
    return f"LOWER({column}) = {sql_string(value.lower())}"


def lower_in(column: str, values: list[str]) -> str:
    inner = ", ".join(sql_string(v.lower()) for v in values)
    return f"LOWER({column}) IN ({inner})"


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def composite_spending_expr() -> str:
    return 'COALESCE(Contracts, 0) + COALESCE(Grants, 0) + COALESCE("Resident Wage", 0)'


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [
        re.sub(r"[^a-z0-9]+", "_", str(col).strip().lower()).strip("_")
        for col in out.columns
    ]
    return out


def detect_label_column(df: pd.DataFrame) -> str | None:
    for candidate in (
        "state",
        "county",
        "cd_118",
        "agency",
        "year",
        "label",
        "rcpt_state_name",
        "subawardee_state_name",
        "rcpt_cty_name",
        "subawardee_cty_name",
        "rcpt_cd_name",
        "subawardee_cd_name",
        "naics_2digit_title",
        "agency_name",
    ):
        if candidate in df.columns:
            return candidate
    object_cols = [c for c in df.columns if df[c].dtype == "object"]
    return object_cols[0] if object_cols else None


def detect_numeric_columns(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            cols.append(col)
    return cols


def approx_equal(a: float, b: float, *, rel_tol: float = 1e-4, abs_tol: float = 1e-6) -> bool:
    if math.isnan(a) and math.isnan(b):
        return True
    return math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)


def _registered(name: str) -> bool:
    return name in AVAILABLE_TABLES


def _manifest_path() -> Path:
    return ROOT_DIR / "data" / "schema" / "manifest.json"


def init_oracle_conn() -> None:
    global ORACLE_CONN
    if ORACLE_CONN is not None:
        return
    manifest = json.loads(_manifest_path().read_text(encoding="utf-8"))
    conn = duckdb.connect(":memory:")
    conn.execute("PRAGMA threads=4")
    for name, info in manifest.items():
        parquet_path = str(Path(info["path"]).resolve()).replace("'", "''")
        conn.execute(
            f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{parquet_path}')"
        )
        AVAILABLE_TABLES.add(name)
    ORACLE_CONN = conn


def execute_oracle_query(sql: str) -> pd.DataFrame:
    init_oracle_conn()
    assert ORACLE_CONN is not None
    return ORACLE_CONN.execute(sql).df()


def _state_like_filter(table: str, state: str) -> str:
    if table in {"contract_state", "contract_county", "spending_state", "spending_state_agency"}:
        return lower_eq("state", state)
    return lower_eq("state", state)


def _md_district_filter() -> str:
    return "UPPER(cd_118) LIKE 'MD-%'"


def _year_filter(table: str, year: str | None) -> str | None:
    if not year:
        return None
    if table.startswith("gov_"):
        return None
    column = "Year" if table.startswith(("acs_", "finra_")) else "year"
    if column == "Year":
        return f"{column} = {int(year)}" if year.isdigit() else f"CAST({column} AS VARCHAR) = {sql_string(year)}"
    return f"{column} = {sql_string(year)}"


def ranking_sql(
    *,
    table: str,
    label_col: str,
    value_expr: str,
    where: list[str] | None = None,
    order: str = "DESC",
    limit: int = 1,
    group_by: list[str] | None = None,
) -> str:
    where_clause = ""
    if where:
        where_clause = " WHERE " + " AND ".join(where)
    group_clause = ""
    if group_by:
        group_clause = " GROUP BY " + ", ".join(group_by)
    order_expr = "value DESC" if order.upper() == "DESC" else "value ASC"
    return (
        f"SELECT {label_col}, {value_expr} AS value FROM {table}"
        f"{where_clause}{group_clause} ORDER BY {order_expr} LIMIT {limit}"
    )


def comparison_sql(
    *,
    table: str,
    label_col: str,
    value_expr: str,
    where: list[str] | None = None,
    order: str = "DESC",
) -> str:
    where_clause = ""
    if where:
        where_clause = " WHERE " + " AND ".join(where)
    order_expr = "value DESC" if order.upper() == "DESC" else "value ASC"
    return f"SELECT {label_col}, {value_expr} AS value FROM {table}{where_clause} ORDER BY {order_expr}"


def multi_metric_compare_sql(
    *,
    table: str,
    label_col: str,
    metrics: list[str],
    where: list[str] | None = None,
    order_by: str | None = None,
) -> str:
    where_clause = ""
    if where:
        where_clause = " WHERE " + " AND ".join(where)
    cols = ", ".join([label_col] + [qcol(metric) if " " in metric or "#" in metric or ">" in metric else metric for metric in metrics])
    order_clause = f" ORDER BY {order_by}" if order_by else ""
    return f"SELECT {cols} FROM {table}{where_clause}{order_clause}"


def trend_sql(
    *,
    table: str,
    label_col: str,
    value_expr: str,
    where: list[str] | None = None,
) -> str:
    where_clause = ""
    if where:
        where_clause = " WHERE " + " AND ".join(where)
    return f"SELECT {label_col}, {value_expr} AS value FROM {table}{where_clause} ORDER BY {label_col}"


def scalar_sql(expr: str) -> str:
    return f"SELECT {expr} AS value"


def add_case(cases: list[GoldCase], **kwargs: Any) -> None:
    cases.append(GoldCase(id=len(cases) + 1, **kwargs))


def build_section_1(cases: list[GoldCase]) -> None:
    conceptual = [
        ("Easy", "routing", "Which dataset should you use to answer questions about demographic characteristics across states, counties, and congressional districts?", ["acs", "census"], [], "The ACS / Census dataset is the right routing choice."),
        ("Easy", "routing", "Which dataset should you use to answer questions about local government liabilities, revenues, and fiscal health?", ["government finances", "gov_spending", "government finance"], [], "Government finances should be selected."),
        ("Easy", "routing", "Which dataset should you use to answer questions about household financial literacy and financial constraints?", ["finra"], [], "FINRA should be selected."),
        ("Easy", "routing", "Which dataset should you use to answer questions about federal contracts, grants, direct payments, and resident wages by geography?", ["federal spending", "contract_static", "contract"], [], "Federal spending geography totals should be selected."),
        ("Easy", "routing", "Which dataset should you use if the user asks about spending by specific agencies?", ["agency", "contract_agency", "spending_state_agency"], [], "Agency-level spending should be selected."),
        ("Easy", "routing", "Which dataset should you use if the user asks where subcontract dollars are flowing into or out of Maryland?", ["flow", "fund flow", "subcontract"], [], "Flow tables should be selected."),
        ("Easy", "availability", "What years are available in Government Finances?", ["fiscal year 2023", "2023"], ["2021"], "Government finances availability is FY2023 only."),
        ("Easy", "availability", "What years are available in FINRA state data?", ["2009", "2012", "2015", "2018", "2021"], [], "FINRA state years should be listed."),
        ("Easy", "availability", "What years are available in FINRA county data?", ["2021"], ["2018"], "FINRA county availability is 2021 only."),
        ("Easy", "availability", "What periods are available in Federal Spending and Federal Spending by Agency?", ["2020-2024", "2024"], [], "Federal spending periods should be listed."),
        ("Easy", "availability", "Is Federal Spending Breakdown available below the state level?", ["state-only", "state only", "no"], [], "Breakdown is state-only."),
        ("Easy", "schema", "What does `cd_118` mean in these tables?", ["congressional district", "md-05", "text"], [], "cd_118 should be explained as a text district identifier."),
        ("Moderate", "schema", "Are all year fields numeric in the project?", ["no", "2020-2024", "fiscal year 2023"], [], "Year fields are mixed text/numeric."),
        ("Moderate", "schema", "Are state names formatted consistently across every dataset?", ["no", "casing", "normalize"], [], "State casing is inconsistent."),
        ("Moderate", "schema", "Should the chatbot automatically recompute `Per 1000` and `_per_capita` fields?", ["no", "stored", "normalized"], [], "Stored normalized fields should be used by default."),
        ("Moderate", "routing", "If the user asks for “top agencies by spending in Maryland,” which data source and spending definition should be used?", ["agency", "contracts", "grants", "resident wage"], ["direct payments"], "Agency state data with dashboard composite should be used."),
        ("Moderate", "routing", "If the user asks for “top agencies by direct payments in Maryland,” what should the chatbot do?", ["direct payments", "agency"], ["resident wage"], "Should rank by Direct Payments only."),
        ("Moderate", "safety", "If the user asks for “Maryland liabilities in 2021,” how should the chatbot respond?", ["not available", "fiscal year 2023", "2021"], [], "Should explain government-finance 2021 is unavailable."),
        ("Moderate", "safety", "If the user asks for county-level FINRA values in 2018, how should the chatbot respond?", ["not available", "2021"], [], "Should explain FINRA county is 2021 only."),
        ("Moderate", "safety", "If the user asks for state-level fund flow in 2018 specifically, is that directly aligned with current dashboard behavior?", ["cautious", "state flow", "year"], [], "Should warn about state-flow year handling."),
        ("Hard", "routing", "What is the difference between `contract_static`, `contract_agency`, and `spending_breakdown`?", ["geography totals", "agency", "state"], [], "Should distinguish static totals, by-agency rows, and breakdown state data."),
        ("Hard", "schema", "What is the difference between `Resident Wage` and `Employees Wage`?", ["different", "resident wage", "employees wage"], [], "Should explain they are different wage concepts."),
        ("Hard", "routing", "When should the chatbot use flow data instead of federal spending data?", ["directional", "origin", "destination", "flow"], [], "Flow data is for directional movement."),
        ("Hard", "safety", "If the user asks for “the biggest funding source” without specifying whether they mean contracts, grants, direct payments, or a composite, what should the chatbot do?", ["clarify", "default", "explicitly"], [], "Should clarify or state a documented default."),
        ("Hard", "evaluation", "How can you tell if a chatbot answer is likely hallucinating on this project’s data?", ["wrong year", "wrong dataset", "mixing", "unsupported"], [], "Should explain common hallucination signals."),
    ]
    for difficulty, focus, question, must_include, must_not_include, notes in conceptual:
        add_case(
            cases,
            section="routing_schema",
            difficulty=difficulty,
            focus=focus,
            question=question,
            comparison_mode="conceptual",
            caution_expected="required" if focus in {"safety", "evaluation"} else None,
            answer_must_include=must_include,
            answer_must_not_include=must_not_include,
            notes=notes,
        )


def build_acs_cases(cases: list[GoldCase]) -> None:
    metrics = [
        "Total population",
        "Median household income",
        "Below poverty",
        "Education >= Bachelor's",
        "Hispanic",
        "Owner occupied",
        "Age 18-65",
        "Black",
        "Income >$100K",
        "# of household",
    ]
    for metric in metrics:
        quoted = qcol(metric)
        add_case(
            cases,
            section="acs",
            difficulty="Easy",
            focus="ranking",
            question=f"In 2023, which state is highest on `{metric}`?",
            expected_tables=["acs_state"],
            expected_table="acs_state",
            expected_geo="state",
            expected_year="2023",
            expected_metric=metric,
            expected_metric_kind="raw",
            oracle_sql=ranking_sql(table="acs_state", label_col="state", value_expr=quoted, where=["Year = 2023"], limit=1),
            comparison_mode="top1",
        )
        add_case(
            cases,
            section="acs",
            difficulty="Moderate",
            focus="comparison",
            question=f"Compare Maryland and Virginia in 2023 on `{metric}`. Which is higher, and by how much?",
            expected_tables=["acs_state"],
            expected_table="acs_state",
            expected_geo="state",
            expected_year="2023",
            expected_metric=metric,
            oracle_sql=comparison_sql(
                table="acs_state",
                label_col="state",
                value_expr=quoted,
                where=["Year = 2023", lower_in("state", ["Maryland", "Virginia"])],
            ),
            comparison_mode="entity_compare",
        )
        add_case(
            cases,
            section="acs",
            difficulty="Moderate",
            focus="filtered_ranking",
            question=f"Within Maryland, which county is highest in 2023 on `{metric}`?",
            expected_tables=["acs_county"],
            expected_table="acs_county",
            expected_geo="county",
            expected_year="2023",
            expected_metric=metric,
            oracle_sql=ranking_sql(
                table="acs_county",
                label_col="county",
                value_expr=quoted,
                where=["Year = 2023", lower_eq("state", "maryland")],
                limit=1,
            ),
            comparison_mode="top1",
        )
        add_case(
            cases,
            section="acs",
            difficulty="Hard",
            focus="district_ranking",
            question=f"Within Maryland, which congressional district is highest in 2023 on `{metric}`?",
            expected_tables=["acs_congress"],
            expected_table="acs_congress",
            expected_geo="congress",
            expected_year="2023",
            expected_metric=metric,
            oracle_sql=ranking_sql(
                table="acs_congress",
                label_col="cd_118",
                value_expr=quoted,
                where=["Year = 2023", _md_district_filter()],
                limit=1,
            ),
            comparison_mode="top1",
            caution_expected="processed_congress_file",
            answer_should_include_any=["congress", "district"],
        )


def build_gov_cases(cases: list[GoldCase]) -> None:
    metrics = [
        "Total_Liabilities",
        "Total_Assets",
        "Revenue",
        "Expenses",
        "Debt_Ratio",
        "Current_Ratio",
        "Free_Cash_Flow",
        "Net_Position",
        "Net_Pension_Liability",
        "Revenue_per_capita",
    ]
    for metric in metrics:
        col = qcol(metric) if "," in metric else metric
        add_case(
            cases,
            section="gov",
            difficulty="Easy",
            focus="ranking",
            question=f"What state is highest on `{metric}` in Government Finances?",
            expected_tables=["gov_state"],
            expected_table="gov_state",
            expected_geo="state",
            expected_year="Fiscal Year 2023",
            expected_metric=metric,
            oracle_sql=ranking_sql(table="gov_state", label_col="state", value_expr=col, limit=1),
            comparison_mode="top1",
        )
        add_case(
            cases,
            section="gov",
            difficulty="Moderate",
            focus="comparison",
            question=f"Compare Maryland, Virginia, and the District of Columbia on `{metric}`. Rank them highest to lowest.",
            expected_tables=["gov_state"],
            expected_table="gov_state",
            expected_geo="state",
            expected_year="Fiscal Year 2023",
            expected_metric=metric,
            oracle_sql=comparison_sql(
                table="gov_state",
                label_col="state",
                value_expr=col,
                where=[lower_in("state", ["Maryland", "Virginia", "District of Columbia"])],
            ),
            comparison_mode="entity_compare",
        )
        add_case(
            cases,
            section="gov",
            difficulty="Moderate",
            focus="filtered_ranking",
            question=f"Within Maryland, which county is highest on `{metric}`?",
            expected_tables=["gov_county"],
            expected_table="gov_county",
            expected_geo="county",
            expected_year="Fiscal Year 2023",
            expected_metric=metric,
            oracle_sql=ranking_sql(
                table="gov_county",
                label_col="county",
                value_expr=col,
                where=[lower_eq("state", "maryland")],
                limit=1,
            ),
            comparison_mode="top1",
        )
        add_case(
            cases,
            section="gov",
            difficulty="Hard",
            focus="district_ranking",
            question=f"Within Maryland congressional districts, which district is highest on `{metric}`?",
            expected_tables=["gov_congress"],
            expected_table="gov_congress",
            expected_geo="congress",
            expected_year="Fiscal Year 2023",
            expected_metric=metric,
            oracle_sql=ranking_sql(
                table="gov_congress",
                label_col="cd_118",
                value_expr=col,
                where=[_md_district_filter()],
                limit=1,
            ),
            comparison_mode="top1",
            caution_expected="processed_congress_file",
            answer_should_include_any=["processed", "congress", "district"],
        )


def build_finra_cases(cases: list[GoldCase]) -> None:
    metrics = [
        "financial_literacy",
        "financial_constraint",
        "alternative_financing",
        "satisfied",
        "risk_averse",
    ]
    for metric in metrics:
        add_case(
            cases,
            section="finra",
            difficulty="Easy",
            focus="ranking",
            question=f"In 2021, which state is highest on `{metric}`?",
            expected_tables=["finra_state"],
            expected_table="finra_state",
            expected_geo="state",
            expected_year="2021",
            expected_metric=metric,
            oracle_sql=ranking_sql(table="finra_state", label_col="state", value_expr=metric, where=["Year = 2021"], limit=1),
            comparison_mode="top1",
        )
        add_case(
            cases,
            section="finra",
            difficulty="Easy",
            focus="ranking",
            question=f"In 2021, which state is lowest on `{metric}`?",
            expected_tables=["finra_state"],
            expected_table="finra_state",
            expected_geo="state",
            expected_year="2021",
            expected_metric=metric,
            oracle_sql=ranking_sql(table="finra_state", label_col="state", value_expr=metric, where=["Year = 2021"], order="ASC", limit=1),
            comparison_mode="top1",
        )
        add_case(
            cases,
            section="finra",
            difficulty="Moderate",
            focus="comparison",
            question=f"Compare Maryland and Virginia in 2021 on `{metric}`.",
            expected_tables=["finra_state"],
            expected_table="finra_state",
            expected_geo="state",
            expected_year="2021",
            expected_metric=metric,
            oracle_sql=comparison_sql(
                table="finra_state",
                label_col="state",
                value_expr=metric,
                where=["Year = 2021", lower_in("state", ["Maryland", "Virginia"])],
            ),
            comparison_mode="entity_compare",
        )
        add_case(
            cases,
            section="finra",
            difficulty="Moderate",
            focus="filtered_ranking",
            question=f"Within Maryland, which county is highest on `{metric}` in the FINRA data?",
            expected_tables=["finra_county"],
            expected_table="finra_county",
            expected_geo="county",
            expected_year="2021",
            expected_metric=metric,
            oracle_sql=ranking_sql(
                table="finra_county",
                label_col="county",
                value_expr=metric,
                where=["Year = 2021", lower_eq("state", "maryland")],
                limit=1,
            ),
            comparison_mode="top1",
        )
        add_case(
            cases,
            section="finra",
            difficulty="Hard",
            focus="district_ranking",
            question=f"Within Maryland congressional districts, which district is highest on `{metric}` in the FINRA data?",
            expected_tables=["finra_congress"],
            expected_table="finra_congress",
            expected_geo="congress",
            expected_year="2021",
            expected_metric=metric,
            oracle_sql=ranking_sql(
                table="finra_congress",
                label_col="cd_118",
                value_expr=metric,
                where=["Year = 2021", _md_district_filter()],
                limit=1,
            ),
            comparison_mode="top1",
            caution_expected="year_only_2021",
            answer_should_include_any=["2021", "only"],
        )
        add_case(
            cases,
            section="finra",
            difficulty="Hard",
            focus="trend",
            question=f"How has Maryland changed over time on `{metric}` across available FINRA state waves?",
            expected_tables=["finra_state"],
            expected_table="finra_state",
            expected_geo="state",
            expected_metric=metric,
            oracle_sql=trend_sql(
                table="finra_state",
                label_col="Year",
                value_expr=metric,
                where=[lower_eq("state", "maryland")],
            ),
            comparison_mode="trend",
            caution_expected="available_waves_only",
            answer_should_include_any=["2009", "2012", "2015", "2018", "2021"],
        )


def build_contract_static_cases(cases: list[GoldCase]) -> None:
    metrics = [
        "Contracts",
        "Grants",
        "Resident Wage",
        "Direct Payments",
        "Federal Residents",
    ]
    for metric in metrics:
        qmetric = qcol(metric)
        per1000 = f"{metric} Per 1000"
        qper1000 = qcol(per1000)
        add_case(
            cases,
            section="federal_static",
            difficulty="Easy",
            focus="ranking",
            question=f"In 2024, which state is highest on `{metric}`?",
            expected_tables=["contract_state"],
            expected_table="contract_state",
            expected_geo="state",
            expected_year="2024",
            expected_metric=metric,
            expected_limit=5,
            oracle_sql=ranking_sql(
                table="contract_state",
                label_col="state",
                value_expr=qmetric,
                where=["year = '2024'"],
                limit=1,
            ),
            comparison_mode="top1",
        )
        add_case(
            cases,
            section="federal_static",
            difficulty="Easy",
            focus="ranking",
            question=f"In the `2020-2024` period, which state is highest on `{metric}`?",
            expected_tables=["contract_state"],
            expected_table="contract_state",
            expected_geo="state",
            expected_year="2020-2024",
            expected_metric=metric,
            oracle_sql=ranking_sql(
                table="contract_state",
                label_col="state",
                value_expr=qmetric,
                where=["year = '2020-2024'"],
                limit=1,
            ),
            comparison_mode="top1",
        )
        add_case(
            cases,
            section="federal_static",
            difficulty="Moderate",
            focus="comparison",
            question=f"Compare Maryland, Virginia, and the District of Columbia in 2024 on `{metric}`.",
            expected_tables=["contract_state"],
            expected_table="contract_state",
            expected_geo="state",
            expected_year="2024",
            expected_metric=metric,
            oracle_sql=comparison_sql(
                table="contract_state",
                label_col="state",
                value_expr=qmetric,
                where=["year = '2024'", lower_in("state", ["Maryland", "Virginia", "District of Columbia"])],
            ),
            comparison_mode="entity_compare",
        )
        add_case(
            cases,
            section="federal_static",
            difficulty="Moderate",
            focus="filtered_ranking",
            question=f"Within Maryland, which county is highest in 2024 on `{metric}`?",
            expected_tables=["contract_county"],
            expected_table="contract_county",
            expected_geo="county",
            expected_year="2024",
            expected_metric=metric,
            oracle_sql=ranking_sql(
                table="contract_county",
                label_col="county",
                value_expr=qmetric,
                where=["year = '2024'", lower_eq("state", "maryland")],
                limit=1,
            ),
            comparison_mode="top1",
        )
        add_case(
            cases,
            section="federal_static",
            difficulty="Hard",
            focus="district_ranking",
            question=f"Within Maryland congressional districts, which district is highest in 2024 on `{metric}`?",
            expected_tables=["contract_congress"],
            expected_table="contract_congress",
            expected_geo="congress",
            expected_year="2024",
            expected_metric=metric,
            oracle_sql=ranking_sql(
                table="contract_congress",
                label_col="cd_118",
                value_expr=qmetric,
                where=["year = '2024'", _md_district_filter()],
                limit=1,
            ),
            comparison_mode="top1",
        )
        add_case(
            cases,
            section="federal_static",
            difficulty="Moderate",
            focus="normalized_ranking",
            question=f"In 2024, which state is highest on `{per1000}`?",
            expected_tables=["contract_state"],
            expected_table="contract_state",
            expected_geo="state",
            expected_year="2024",
            expected_metric=per1000,
            expected_metric_kind="normalized",
            oracle_sql=ranking_sql(
                table="contract_state",
                label_col="state",
                value_expr=qper1000,
                where=["year = '2024'"],
                limit=1,
            ),
            comparison_mode="top1",
        )
        add_case(
            cases,
            section="federal_static",
            difficulty="Moderate",
            focus="change",
            question=f"For Maryland, how does `{metric}` in 2024 compare with the `2020-2024` period value?",
            expected_tables=["contract_state"],
            expected_table="contract_state",
            expected_geo="state",
            expected_metric=metric,
            oracle_sql=trend_sql(
                table="contract_state",
                label_col="year",
                value_expr=qmetric,
                where=[lower_eq("state", "maryland"), "year IN ('2024', '2020-2024')"],
            ),
            comparison_mode="trend",
            caution_expected="period_vs_year",
            answer_should_include_any=["multi-year", "period", "2020-2024"],
        )
        add_case(
            cases,
            section="federal_static",
            difficulty="Hard",
            focus="top_k",
            question=f"What are the top 5 states in 2024 on `{metric}`?",
            expected_tables=["contract_state"],
            expected_table="contract_state",
            expected_geo="state",
            expected_year="2024",
            expected_metric=metric,
            oracle_sql=ranking_sql(
                table="contract_state",
                label_col="state",
                value_expr=qmetric,
                where=["year = '2024'"],
                limit=5,
            ),
            comparison_mode="topk",
        )
        add_case(
            cases,
            section="federal_static",
            difficulty="Hard",
            focus="unit_safety",
            question=f"If a user asks for the “largest relative exposure” in `{metric}`, what field should be used?",
            expected_tables=["contract_state"],
            expected_table="contract_state",
            expected_metric=per1000,
            comparison_mode="conceptual",
            caution_expected="required",
            answer_must_include=[normalize_text(per1000), "per 1000"],
            notes="Relative exposure should use the stored normalized field.",
        )


def build_agency_cases(cases: list[GoldCase]) -> None:
    comp = composite_spending_expr()
    unsupported_notes = "Repo currently lacks county/congress agency parquet/views; this is a structural coverage gap."

    def agency_rank_question(question: str, state: str, year: str, expr: str, *, limit: int = 5) -> GoldCase:
        return GoldCase(
            id=len(cases) + 1,
            section="agency",
            difficulty="Easy",
            focus="agency_ranking",
            question=question,
            expected_tables=["spending_state_agency"],
            expected_table="spending_state_agency",
            expected_geo="state",
            expected_year=year,
            expected_metric="Contracts + Grants + Resident Wage" if expr == comp else expr.replace('"', ""),
            expected_limit=limit,
            oracle_sql=ranking_sql(
                table="spending_state_agency",
                label_col="agency",
                value_expr=expr,
                where=[lower_eq("state", state), f"year = {sql_string(year)}"],
                limit=limit,
            ),
            comparison_mode="topk",
        )

    cases.extend(
        [
            agency_rank_question(
                "Which agencies account for the most spending in Maryland in 2024?",
                "maryland",
                "2024",
                comp,
            ),
            GoldCase(
                id=len(cases) + 2,
                section="agency",
                difficulty="Easy",
                focus="agency_ranking",
                question="Which agencies account for the most spending in Maryland in the `2020-2024` period?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2020-2024",
                expected_metric="Contracts + Grants + Resident Wage",
                expected_limit=5,
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr=comp,
                    where=[lower_eq("state", "maryland"), "year = '2020-2024'"],
                    limit=5,
                ),
                comparison_mode="topk",
            ),
            GoldCase(
                id=len(cases) + 3,
                section="agency",
                difficulty="Moderate",
                focus="agency_metric_ranking",
                question="Which agencies account for the most contracts in Maryland in 2024?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_metric="Contracts",
                expected_limit=5,
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr="Contracts",
                    where=[lower_eq("state", "maryland"), "year = '2024'"],
                    limit=5,
                ),
                comparison_mode="topk",
            ),
            GoldCase(
                id=len(cases) + 4,
                section="agency",
                difficulty="Moderate",
                focus="agency_metric_ranking",
                question="Which agencies account for the most grants in Maryland in 2024?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_metric="Grants",
                expected_limit=5,
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr="Grants",
                    where=[lower_eq("state", "maryland"), "year = '2024'"],
                    limit=5,
                ),
                comparison_mode="topk",
            ),
            GoldCase(
                id=len(cases) + 5,
                section="agency",
                difficulty="Moderate",
                focus="agency_metric_ranking",
                question="Which agencies account for the most direct payments in Maryland in 2024?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_metric="Direct Payments",
                expected_limit=5,
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr=qcol("Direct Payments"),
                    where=[lower_eq("state", "maryland"), "year = '2024'"],
                    limit=5,
                ),
                comparison_mode="topk",
            ),
            GoldCase(
                id=len(cases) + 6,
                section="agency",
                difficulty="Moderate",
                focus="agency_metric_ranking",
                question="Which agencies account for the most resident wage in Maryland in 2024?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_metric="Resident Wage",
                expected_limit=5,
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr=qcol("Resident Wage"),
                    where=[lower_eq("state", "maryland"), "year = '2024'"],
                    limit=5,
                ),
                comparison_mode="topk",
            ),
            GoldCase(
                id=len(cases) + 7,
                section="agency",
                difficulty="Moderate",
                focus="agency_metric_ranking",
                question="Which agencies account for the most employees in Maryland in 2024?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_metric="Employees",
                expected_limit=5,
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr="Employees",
                    where=[lower_eq("state", "maryland"), "year = '2024'"],
                    limit=5,
                ),
                comparison_mode="topk",
            ),
            GoldCase(
                id=len(cases) + 8,
                section="agency",
                difficulty="Hard",
                focus="comparison",
                question="Compare the top 5 Maryland agencies in 2024 by contracts vs by grants. Which agencies move the most between the two rankings?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                comparison_mode="conceptual",
                answer_should_include_any=["contracts", "grants", "top 5", "rank"],
                notes="Should compare two rankings from the same state-agency table.",
            ),
            GoldCase(
                id=len(cases) + 9,
                section="agency",
                difficulty="Hard",
                focus="composition",
                question="For Maryland in 2024, what share of total agency-defined spending comes from the top 3 agencies?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_metric="Contracts + Grants + Resident Wage",
                oracle_sql=(
                    "WITH ranked AS ("
                    f" SELECT agency, {comp} AS spending_total"
                    " FROM spending_state_agency"
                    " WHERE LOWER(state) = 'maryland' AND year = '2024'"
                    " ORDER BY spending_total DESC"
                    " ), top3 AS (SELECT SUM(spending_total) AS top_total FROM ranked LIMIT 3),"
                    " total AS (SELECT SUM(spending_total) AS overall_total FROM ranked)"
                    " SELECT 100.0 * top_total / overall_total AS value FROM top3 CROSS JOIN total"
                ),
                comparison_mode="scalar",
            ),
        ]
    )

    for question, notes in [
        ("Within Maryland counties in 2024, which counties have the highest Department of Defense contracts?", unsupported_notes),
        ("Within Maryland counties in 2024, which counties have the highest Department of Health and Human Services grants?", unsupported_notes),
        ("Within Maryland congressional districts in 2024, which districts have the highest Department of Defense contracts?", unsupported_notes),
    ]:
        add_case(
            cases,
            section="agency",
            difficulty="Hard",
            focus="filtering",
            question=question,
            expected_tables=["contract_agency_county", "contract_agency_congress"],
            expected_geo="county" if "counties" in question else "congress",
            expected_year="2024",
            comparison_mode="conceptual",
            expected_support="missing_agency_geo_tables",
            caution_expected="required",
            answer_should_include_any=["not available", "current dataset", "state-only"],
            notes=notes,
        )

    cases.extend(
        [
            GoldCase(
                id=len(cases) + 1,
                section="agency",
                difficulty="Moderate",
                focus="top_k",
                question="What are the top 10 agencies in California in 2024 by direct payments?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_metric="Direct Payments",
                expected_limit=10,
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr=qcol("Direct Payments"),
                    where=[lower_eq("state", "california"), "year = '2024'"],
                    limit=10,
                ),
                comparison_mode="topk",
            ),
            GoldCase(
                id=len(cases) + 2,
                section="agency",
                difficulty="Moderate",
                focus="normalized_ranking",
                question="In 2024, which agency has the highest `Contracts Per 1000` in Maryland?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_metric="Contracts Per 1000",
                expected_metric_kind="normalized",
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr=qcol("Contracts Per 1000"),
                    where=[lower_eq("state", "maryland"), "year = '2024'"],
                    limit=1,
                ),
                comparison_mode="top1",
            ),
            GoldCase(
                id=len(cases) + 3,
                section="agency",
                difficulty="Hard",
                focus="ambiguity",
                question="If the user asks “largest federal presence by agency in Maryland,” which metric is more appropriate: `Employees`, `Federal Residents`, or spending?",
                comparison_mode="conceptual",
                caution_expected="required",
                answer_should_include_any=["ambiguous", "employees", "federal residents", "spending"],
                notes="Should explain the ambiguity instead of picking one silently.",
            ),
            GoldCase(
                id=len(cases) + 4,
                section="agency",
                difficulty="Hard",
                focus="time_availability",
                question="Can the chatbot answer agency-by-county questions for 2018?",
                comparison_mode="conceptual",
                caution_expected="required",
                answer_must_include=["no", "2020-2024", "2024"],
                notes="Agency files only cover 2020-2024 and 2024 in this repo.",
            ),
            GoldCase(
                id=len(cases) + 5,
                section="agency",
                difficulty="Moderate",
                focus="difference",
                question="For Maryland in 2024, which agencies rank highly by `Employees` but not by spending?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                comparison_mode="conceptual",
                answer_should_include_any=["employees", "spending", "rank"],
                notes="Should compare employee rank versus composite-spending rank.",
            ),
            GoldCase(
                id=len(cases) + 6,
                section="agency",
                difficulty="Moderate",
                focus="difference",
                question="For Maryland in 2024, which agencies rank highly by `Direct Payments` but not by contracts?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                comparison_mode="conceptual",
                answer_should_include_any=["direct payments", "contracts", "rank"],
                notes="Should compare direct-payment rank versus contract rank.",
            ),
            GoldCase(
                id=len(cases) + 7,
                section="agency",
                difficulty="Hard",
                focus="cross_geography",
                question="For the Department of Defense in 2024, compare Maryland’s state rank on `Contracts` with Maryland’s rank on `Resident Wage`.",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                oracle_sql=(
                    "WITH ranked AS ("
                    " SELECT state,"
                    " RANK() OVER(ORDER BY Contracts DESC) AS contracts_rank,"
                    " RANK() OVER(ORDER BY \"Resident Wage\" DESC) AS resident_wage_rank"
                    " FROM spending_state_agency"
                    " WHERE agency = 'Department of Defense' AND year = '2024'"
                    " )"
                    " SELECT state, contracts_rank, resident_wage_rank FROM ranked WHERE LOWER(state) = 'maryland'"
                ),
                comparison_mode="frame",
            ),
            GoldCase(
                id=len(cases) + 8,
                section="agency",
                difficulty="Hard",
                focus="cross_geography",
                question="For HHS in 2024, which Maryland congressional district is highest on grants, and how does it compare with the top Maryland county?",
                expected_tables=["contract_agency_congress", "contract_agency_county"],
                expected_geo="congress",
                expected_year="2024",
                comparison_mode="conceptual",
                expected_support="missing_agency_geo_tables",
                caution_expected="required",
                answer_should_include_any=["not available", "current dataset", "missing"],
                notes=unsupported_notes,
            ),
        ]
    )

    for state in ["Virginia", "California", "Texas", "District of Columbia"]:
        add_case(
            cases,
            section="agency",
            difficulty="Moderate",
            focus="agency_ranking",
            question=f"Which agencies account for the most spending in {state} in 2024?",
            expected_tables=["spending_state_agency"],
            expected_table="spending_state_agency",
            expected_geo="state",
            expected_year="2024",
            expected_metric="Contracts + Grants + Resident Wage",
            expected_limit=5,
            oracle_sql=ranking_sql(
                table="spending_state_agency",
                label_col="agency",
                value_expr=comp,
                where=[lower_eq("state", state.lower()), "year = '2024'"],
                limit=5,
            ),
            comparison_mode="topk",
        )

    cases.extend(
        [
            GoldCase(
                id=len(cases) + 1,
                section="agency",
                difficulty="Hard",
                focus="share_calc",
                question="For Maryland in 2024, what share of agency spending comes from the top agency?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                oracle_sql=(
                    "WITH base AS ("
                    f" SELECT agency, {comp} AS spending_total"
                    " FROM spending_state_agency"
                    " WHERE LOWER(state) = 'maryland' AND year = '2024'"
                    " ), top1 AS (SELECT MAX(spending_total) AS top_total FROM base),"
                    " total AS (SELECT SUM(spending_total) AS overall_total FROM base)"
                    " SELECT 100.0 * top_total / overall_total AS value FROM top1 CROSS JOIN total"
                ),
                comparison_mode="scalar",
            ),
            GoldCase(
                id=len(cases) + 2,
                section="agency",
                difficulty="Hard",
                focus="share_calc",
                question="For Maryland in the `2020-2024` period, what share of agency spending comes from the top 5 agencies?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2020-2024",
                oracle_sql=(
                    "WITH ranked AS ("
                    f" SELECT agency, {comp} AS spending_total"
                    " FROM spending_state_agency"
                    " WHERE LOWER(state) = 'maryland' AND year = '2020-2024'"
                    " ORDER BY spending_total DESC"
                    " ), top5 AS (SELECT SUM(spending_total) AS top_total FROM ranked LIMIT 5),"
                    " total AS (SELECT SUM(spending_total) AS overall_total FROM ranked)"
                    " SELECT 100.0 * top_total / overall_total AS value FROM top5 CROSS JOIN total"
                ),
                comparison_mode="scalar",
            ),
            GoldCase(
                id=len(cases) + 3,
                section="agency",
                difficulty="Moderate",
                focus="ranking",
                question="Within Maryland counties in 2024, which counties have the highest total agency-defined spending?",
                expected_tables=["contract_agency_county"],
                expected_geo="county",
                expected_year="2024",
                comparison_mode="conceptual",
                expected_support="missing_agency_geo_tables",
                caution_expected="required",
                answer_should_include_any=["not available", "current dataset", "missing"],
                notes=unsupported_notes,
            ),
            GoldCase(
                id=len(cases) + 4,
                section="agency",
                difficulty="Moderate",
                focus="ranking",
                question="Within Maryland congressional districts in 2024, which districts have the highest total agency-defined spending?",
                expected_tables=["contract_agency_congress"],
                expected_geo="congress",
                expected_year="2024",
                comparison_mode="conceptual",
                expected_support="missing_agency_geo_tables",
                caution_expected="required",
                answer_should_include_any=["not available", "current dataset", "missing"],
                notes=unsupported_notes,
            ),
            GoldCase(
                id=len(cases) + 5,
                section="agency",
                difficulty="Hard",
                focus="comparison",
                question="Compare Maryland and Virginia in 2024 on the mix of top 5 agencies by composite spending. How much overlap is there?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                oracle_sql=(
                    "WITH md AS ("
                    f" SELECT agency FROM spending_state_agency WHERE LOWER(state) = 'maryland' AND year = '2024' ORDER BY {comp} DESC LIMIT 5"
                    " ), va AS ("
                    f" SELECT agency FROM spending_state_agency WHERE LOWER(state) = 'virginia' AND year = '2024' ORDER BY {comp} DESC LIMIT 5"
                    " )"
                    " SELECT md.agency FROM md INNER JOIN va USING (agency) ORDER BY md.agency"
                ),
                comparison_mode="frame",
            ),
            GoldCase(
                id=len(cases) + 6,
                section="agency",
                difficulty="Hard",
                focus="comparison",
                question="Which agencies are top-5 in Maryland by grants but not top-5 by contracts in 2024?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                oracle_sql=(
                    "WITH g AS (SELECT agency FROM spending_state_agency WHERE LOWER(state) = 'maryland' AND year = '2024' ORDER BY Grants DESC LIMIT 5),"
                    " c AS (SELECT agency FROM spending_state_agency WHERE LOWER(state) = 'maryland' AND year = '2024' ORDER BY Contracts DESC LIMIT 5)"
                    " SELECT agency FROM g WHERE agency NOT IN (SELECT agency FROM c) ORDER BY agency"
                ),
                comparison_mode="frame",
            ),
            GoldCase(
                id=len(cases) + 7,
                section="agency",
                difficulty="Hard",
                focus="ranking",
                question="For the Department of Agriculture in 2024, which state has the highest grants?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_metric="Grants",
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="state",
                    value_expr="Grants",
                    where=["agency = 'Department of Agriculture'", "year = '2024'"],
                    limit=1,
                ),
                comparison_mode="top1",
            ),
            GoldCase(
                id=len(cases) + 8,
                section="agency",
                difficulty="Hard",
                focus="ranking",
                question="For the Department of Veterans Affairs in 2024, which state has the highest resident wage?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_metric="Resident Wage",
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="state",
                    value_expr=qcol("Resident Wage"),
                    where=["agency = 'Department of Veterans Affairs'", "year = '2024'"],
                    limit=1,
                ),
                comparison_mode="top1",
            ),
            GoldCase(
                id=len(cases) + 9,
                section="agency",
                difficulty="Hard",
                focus="ranking",
                question="For NASA in 2024, which congressional district nationally is highest on contracts?",
                expected_tables=["contract_agency_congress"],
                expected_geo="congress",
                expected_year="2024",
                comparison_mode="conceptual",
                expected_support="missing_agency_geo_tables",
                caution_expected="required",
                answer_should_include_any=["not available", "current dataset", "missing"],
                notes=unsupported_notes,
            ),
            GoldCase(
                id=len(cases) + 10,
                section="agency",
                difficulty="Hard",
                focus="ranking",
                question="For the Department of Education in 2024, which counties nationally are highest on grants?",
                expected_tables=["contract_agency_county"],
                expected_geo="county",
                expected_year="2024",
                comparison_mode="conceptual",
                expected_support="missing_agency_geo_tables",
                caution_expected="required",
                answer_should_include_any=["not available", "current dataset", "missing"],
                notes=unsupported_notes,
            ),
            GoldCase(
                id=len(cases) + 11,
                section="agency",
                difficulty="Hard",
                focus="safety",
                question="If the chatbot uses `/api/agencies` default behavior for “top agencies by spending,” what mistake could it make?",
                comparison_mode="conceptual",
                caution_expected="required",
                answer_should_include_any=["contracts", "default", "grants", "resident wage"],
                notes="Should note the trap of equating generic spending with contracts only.",
            ),
        ]
    )


def build_breakdown_cases(cases: list[GoldCase]) -> None:
    comp = composite_spending_expr()
    for metric in ["Contracts", "Grants", "Resident Wage", "Direct Payments"]:
        add_case(
            cases,
            section="breakdown",
            difficulty="Easy",
            focus="ranking",
            question=f"In 2024, which state is highest on Federal Spending Breakdown `{metric}`?",
            expected_tables=["spending_state"],
            expected_table="spending_state",
            expected_geo="state",
            expected_year="2024",
            expected_metric=metric,
            oracle_sql=ranking_sql(
                table="spending_state",
                label_col="state",
                value_expr=qcol(metric),
                where=["year = '2024'"],
                limit=1,
            ),
            comparison_mode="top1",
        )

    cases.extend(
        [
            GoldCase(
                id=len(cases) + 1,
                section="breakdown",
                difficulty="Moderate",
                focus="comparison",
                question="Compare Maryland and Virginia in 2024 on Contracts, Grants, and Resident Wage.",
                expected_tables=["spending_state"],
                expected_table="spending_state",
                expected_geo="state",
                expected_year="2024",
                oracle_sql=multi_metric_compare_sql(
                    table="spending_state",
                    label_col="state",
                    metrics=["Contracts", "Grants", "Resident Wage"],
                    where=["year = '2024'", lower_in("state", ["maryland", "virginia"])],
                    order_by="state",
                ),
                comparison_mode="frame",
            ),
            GoldCase(
                id=len(cases) + 2,
                section="breakdown",
                difficulty="Moderate",
                focus="composition",
                question="For Maryland in 2024, which agencies make up the top 10 spending bars in the breakdown view?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_limit=10,
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr=comp,
                    where=[lower_eq("state", "maryland"), "year = '2024'"],
                    limit=10,
                ),
                comparison_mode="topk",
            ),
            GoldCase(
                id=len(cases) + 3,
                section="breakdown",
                difficulty="Moderate",
                focus="composition",
                question="For Maryland in 2024, which agencies make up the top 10 jobs bars in the breakdown view?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_limit=10,
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr="Employees",
                    where=[lower_eq("state", "maryland"), "year = '2024'"],
                    limit=10,
                ),
                comparison_mode="topk",
            ),
            GoldCase(
                id=len(cases) + 4,
                section="breakdown",
                difficulty="Moderate",
                focus="calculation",
                question="For Maryland in 2024, what is the total breakdown spending of the top agency?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr=comp,
                    where=[lower_eq("state", "maryland"), "year = '2024'"],
                    limit=1,
                ),
                comparison_mode="top1",
            ),
            GoldCase(
                id=len(cases) + 5,
                section="breakdown",
                difficulty="Moderate",
                focus="calculation",
                question="For Maryland in 2024, what percent of the top agency’s composite spending comes from contracts vs grants vs resident wage?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                oracle_sql=(
                    "WITH top1 AS ("
                    " SELECT agency, Contracts, Grants, \"Resident Wage\","
                    f" {comp} AS spending_total"
                    " FROM spending_state_agency"
                    " WHERE LOWER(state) = 'maryland' AND year = '2024'"
                    " ORDER BY spending_total DESC"
                    " LIMIT 1"
                    " )"
                    " SELECT agency,"
                    " 100.0 * Contracts / spending_total AS contracts_pct,"
                    " 100.0 * Grants / spending_total AS grants_pct,"
                    " 100.0 * \"Resident Wage\" / spending_total AS resident_wage_pct"
                    " FROM top1"
                ),
                comparison_mode="frame",
            ),
            GoldCase(
                id=len(cases) + 6,
                section="breakdown",
                difficulty="Hard",
                focus="calculation",
                question="For Maryland in 2024, what share of statewide composite spending is represented by the top 10 agencies?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                oracle_sql=(
                    "WITH ranked AS ("
                    f" SELECT agency, {comp} AS spending_total"
                    " FROM spending_state_agency"
                    " WHERE LOWER(state) = 'maryland' AND year = '2024'"
                    " ORDER BY spending_total DESC"
                    " ), top10 AS (SELECT SUM(spending_total) AS top_total FROM ranked LIMIT 10),"
                    " total AS (SELECT SUM(spending_total) AS overall_total FROM ranked)"
                    " SELECT 100.0 * top_total / overall_total AS value FROM top10 CROSS JOIN total"
                ),
                comparison_mode="scalar",
            ),
            GoldCase(
                id=len(cases) + 7,
                section="breakdown",
                difficulty="Hard",
                focus="difference",
                question="Which agencies are in the top 10 by jobs for Maryland in 2024 but not in the top 10 by composite spending?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                oracle_sql=(
                    "WITH j AS (SELECT agency FROM spending_state_agency WHERE LOWER(state) = 'maryland' AND year = '2024' ORDER BY Employees DESC LIMIT 10),"
                    " s AS ("
                    f" SELECT agency FROM spending_state_agency WHERE LOWER(state) = 'maryland' AND year = '2024' ORDER BY {comp} DESC LIMIT 10"
                    " ) SELECT agency FROM j WHERE agency NOT IN (SELECT agency FROM s) ORDER BY agency"
                ),
                comparison_mode="frame",
            ),
            GoldCase(
                id=len(cases) + 8,
                section="breakdown",
                difficulty="Hard",
                focus="difference",
                question="Which agencies are in the top 10 by composite spending for Maryland in 2024 but not in the top 10 by direct payments?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                oracle_sql=(
                    "WITH s AS ("
                    f" SELECT agency FROM spending_state_agency WHERE LOWER(state) = 'maryland' AND year = '2024' ORDER BY {comp} DESC LIMIT 10"
                    " ), d AS (SELECT agency FROM spending_state_agency WHERE LOWER(state) = 'maryland' AND year = '2024' ORDER BY \"Direct Payments\" DESC LIMIT 10)"
                    " SELECT agency FROM s WHERE agency NOT IN (SELECT agency FROM d) ORDER BY agency"
                ),
                comparison_mode="frame",
            ),
            GoldCase(
                id=len(cases) + 9,
                section="breakdown",
                difficulty="Hard",
                focus="state_compare",
                question="For California in 2024, which agencies dominate composite spending?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_limit=5,
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr=comp,
                    where=[lower_eq("state", "california"), "year = '2024'"],
                    limit=5,
                ),
                comparison_mode="topk",
            ),
            GoldCase(
                id=len(cases) + 10,
                section="breakdown",
                difficulty="Hard",
                focus="state_compare",
                question="For Virginia in 2024, which agencies dominate jobs?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                expected_limit=5,
                oracle_sql=ranking_sql(
                    table="spending_state_agency",
                    label_col="agency",
                    value_expr="Employees",
                    where=[lower_eq("state", "virginia"), "year = '2024'"],
                    limit=5,
                ),
                comparison_mode="topk",
            ),
            GoldCase(
                id=len(cases) + 11,
                section="breakdown",
                difficulty="Hard",
                focus="unit_safety",
                question="If the user asks for “largest funding category” in a state, should the chatbot return a channel or an agency?",
                comparison_mode="conceptual",
                caution_expected="required",
                answer_should_include_any=["channel", "agency", "ambiguous"],
            ),
            GoldCase(
                id=len(cases) + 12,
                section="breakdown",
                difficulty="Moderate",
                focus="period_compare",
                question="For Maryland, how does the agency composition in `2020-2024` compare with 2024?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                comparison_mode="conceptual",
                answer_should_include_any=["2020-2024", "2024", "agency"],
                caution_expected="required",
            ),
            GoldCase(
                id=len(cases) + 13,
                section="breakdown",
                difficulty="Hard",
                focus="cross_state_compare",
                question="Which agencies appear in the top 5 composite-spending agencies for both Maryland and Virginia in 2024?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                oracle_sql=(
                    "WITH md AS ("
                    f" SELECT agency FROM spending_state_agency WHERE LOWER(state) = 'maryland' AND year = '2024' ORDER BY {comp} DESC LIMIT 5"
                    " ), va AS ("
                    f" SELECT agency FROM spending_state_agency WHERE LOWER(state) = 'virginia' AND year = '2024' ORDER BY {comp} DESC LIMIT 5"
                    " ) SELECT md.agency FROM md INNER JOIN va USING (agency) ORDER BY md.agency"
                ),
                comparison_mode="frame",
            ),
            GoldCase(
                id=len(cases) + 14,
                section="breakdown",
                difficulty="Hard",
                focus="ratio",
                question="For Maryland in 2024, what is the ratio of top-agency composite spending to top-agency jobs?",
                expected_tables=["spending_state_agency"],
                expected_table="spending_state_agency",
                expected_geo="state",
                expected_year="2024",
                oracle_sql=(
                    "WITH top_spend AS ("
                    f" SELECT agency, {comp} AS spending_total FROM spending_state_agency WHERE LOWER(state) = 'maryland' AND year = '2024' ORDER BY spending_total DESC LIMIT 1"
                    " ), top_jobs AS ("
                    " SELECT agency, Employees AS jobs_total FROM spending_state_agency WHERE LOWER(state) = 'maryland' AND year = '2024' ORDER BY Employees DESC LIMIT 1"
                    " ) SELECT spending_total / jobs_total AS value FROM top_spend CROSS JOIN top_jobs"
                ),
                comparison_mode="scalar",
            ),
            GoldCase(
                id=len(cases) + 15,
                section="breakdown",
                difficulty="Hard",
                focus="safety",
                question="If a user asks “Which agency spends the most in Maryland?” should the answer include Direct Payments by default?",
                comparison_mode="conceptual",
                caution_expected="required",
                answer_should_include_any=["no", "contracts", "grants", "resident wage"],
            ),
            GoldCase(
                id=len(cases) + 16,
                section="breakdown",
                difficulty="Hard",
                focus="safety",
                question="If a user asks for county-level breakdown data, how should the chatbot respond?",
                comparison_mode="conceptual",
                caution_expected="required",
                answer_should_include_any=["state-only", "state only", "contract_agency", "not available"],
            ),
        ]
    )


def build_flow_cases(cases: list[GoldCase]) -> None:
    displayed_external = "LOWER(rcpt_state_name) <> LOWER(subawardee_state_name)"
    add_case(
        cases,
        section="flow",
        difficulty="Easy",
        focus="routing",
        question="What dataset should the chatbot use to answer “Where are subcontract dollars flowing into Maryland from?”",
        comparison_mode="conceptual",
        answer_should_include_any=["flow", "fund flow", "subcontract"],
    )
    add_case(
        cases,
        section="flow",
        difficulty="Easy",
        focus="definition",
        question="What is an inflow in the fund-flow dashboard?",
        comparison_mode="conceptual",
        answer_should_include_any=["into", "selected geography", "destination"],
    )
    add_case(
        cases,
        section="flow",
        difficulty="Easy",
        focus="definition",
        question="What is an outflow in the fund-flow dashboard?",
        comparison_mode="conceptual",
        answer_should_include_any=["leaving", "selected geography", "origin"],
    )
    add_case(
        cases,
        section="flow",
        difficulty="Moderate",
        focus="state_ranking",
        question="Which states are the biggest inflow sources into Maryland at the state level?",
        expected_tables=["state_flow"],
        expected_table="state_flow",
        expected_geo="state",
        oracle_sql=ranking_sql(
            table="state_flow",
            label_col="rcpt_state_name",
            value_expr="SUM(subaward_amount_year)",
            where=["LOWER(subawardee_state_name) = 'maryland'", displayed_external],
            group_by=["rcpt_state_name"],
            limit=5,
        ),
        comparison_mode="topk",
        expected_limit=5,
    )
    add_case(
        cases,
        section="flow",
        difficulty="Moderate",
        focus="state_ranking",
        question="Which states are the biggest outflow destinations from Maryland at the state level?",
        expected_tables=["state_flow"],
        expected_table="state_flow",
        expected_geo="state",
        oracle_sql=ranking_sql(
            table="state_flow",
            label_col="subawardee_state_name",
            value_expr="SUM(subaward_amount_year)",
            where=["LOWER(rcpt_state_name) = 'maryland'", displayed_external],
            group_by=["subawardee_state_name"],
            limit=5,
        ),
        comparison_mode="topk",
        expected_limit=5,
    )
    add_case(
        cases,
        section="flow",
        difficulty="Moderate",
        focus="agency_ranking",
        question="For Maryland inflows, which agencies account for the most subcontract flow?",
        expected_tables=["state_flow"],
        expected_table="state_flow",
        expected_geo="state",
        oracle_sql=ranking_sql(
            table="state_flow",
            label_col="agency_name",
            value_expr="SUM(subaward_amount_year)",
            where=["LOWER(subawardee_state_name) = 'maryland'", displayed_external],
            group_by=["agency_name"],
            limit=5,
        ),
        comparison_mode="topk",
        expected_limit=5,
    )
    add_case(
        cases,
        section="flow",
        difficulty="Moderate",
        focus="industry_ranking",
        question="For Maryland inflows, which industries account for the most subcontract flow?",
        expected_tables=["state_flow"],
        expected_table="state_flow",
        expected_geo="state",
        oracle_sql=ranking_sql(
            table="state_flow",
            label_col="naics_2digit_title",
            value_expr="SUM(subaward_amount_year)",
            where=["LOWER(subawardee_state_name) = 'maryland'", displayed_external],
            group_by=["naics_2digit_title"],
            limit=5,
        ),
        comparison_mode="topk",
        expected_limit=5,
    )
    add_case(
        cases,
        section="flow",
        difficulty="Moderate",
        focus="largest_flow",
        question="What is the single largest displayed flow involving Maryland at the state level?",
        expected_tables=["state_flow"],
        expected_table="state_flow",
        expected_geo="state",
        oracle_sql=(
            "WITH flows AS ("
            " SELECT rcpt_state_name || ' -> ' || subawardee_state_name AS label,"
            " SUM(subaward_amount_year) AS value"
            " FROM state_flow"
            " WHERE (LOWER(rcpt_state_name) = 'maryland' OR LOWER(subawardee_state_name) = 'maryland')"
            f" AND {displayed_external}"
            " GROUP BY 1"
            " ) SELECT label, value FROM flows ORDER BY value DESC LIMIT 1"
        ),
        comparison_mode="top1",
    )
    add_case(
        cases,
        section="flow",
        difficulty="Moderate",
        focus="internal_flows",
        question="How much internal flow does Maryland have, and is it shown on the map?",
        expected_tables=["state_flow"],
        expected_table="state_flow",
        expected_geo="state",
        oracle_sql=scalar_sql(
            "COALESCE((SELECT SUM(subaward_amount_year) FROM state_flow WHERE LOWER(rcpt_state_name) = 'maryland' AND LOWER(subawardee_state_name) = 'maryland'), 0)"
        ),
        comparison_mode="scalar",
        caution_expected="required",
        answer_should_include_any=["not shown", "excluded", "map"],
    )
    add_case(
        cases,
        section="flow",
        difficulty="Moderate",
        focus="difference",
        question="What is the difference between total flows and displayed flows in the fund-flow dashboard?",
        comparison_mode="conceptual",
        caution_expected="required",
        answer_should_include_any=["internal", "displayed", "excluded"],
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="county_ranking",
        question="At the county level, which Maryland counties receive the largest inflows from Virginia?",
        expected_tables=["county_flow"],
        expected_table="county_flow",
        expected_geo="county",
        oracle_sql=ranking_sql(
            table="county_flow",
            label_col="subawardee_cty_name",
            value_expr="SUM(subaward_amount)",
            where=["LOWER(subawardee_state) = 'maryland'", "LOWER(rcpt_state) = 'virginia'", "LOWER(rcpt_full_name) <> LOWER(subawardee_full_name)"],
            group_by=["subawardee_cty_name"],
            limit=5,
        ),
        comparison_mode="topk",
        expected_limit=5,
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="county_ranking",
        question="At the county level, which Maryland counties send the largest outflows to Virginia?",
        expected_tables=["county_flow"],
        expected_table="county_flow",
        expected_geo="county",
        oracle_sql=ranking_sql(
            table="county_flow",
            label_col="rcpt_cty_name",
            value_expr="SUM(subaward_amount)",
            where=["LOWER(rcpt_state) = 'maryland'", "LOWER(subawardee_state) = 'virginia'", "LOWER(rcpt_full_name) <> LOWER(subawardee_full_name)"],
            group_by=["rcpt_cty_name"],
            limit=5,
        ),
        comparison_mode="topk",
        expected_limit=5,
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="agency_filter",
        question="For Department of Defense flows only, which Maryland counties have the largest inflows?",
        expected_tables=["county_flow"],
        expected_table="county_flow",
        expected_geo="county",
        oracle_sql=ranking_sql(
            table="county_flow",
            label_col="subawardee_cty_name",
            value_expr="SUM(subaward_amount)",
            where=["LOWER(subawardee_state) = 'maryland'", "agency_name = 'Department of Defense'", "LOWER(rcpt_full_name) <> LOWER(subawardee_full_name)"],
            group_by=["subawardee_cty_name"],
            limit=5,
        ),
        comparison_mode="topk",
        expected_limit=5,
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="industry_filter",
        question="For aerospace-related flows, which states contribute the most inflow into Maryland?",
        expected_tables=["state_flow"],
        expected_table="state_flow",
        expected_geo="state",
        comparison_mode="conceptual",
        caution_expected="required",
        answer_should_include_any=["industry", "naics", "aerospace"],
        notes="This is semantically ambiguous because there is no literal aerospace category in the current state_flow file.",
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="year_filter",
        question="At the county level, what are the top Maryland inflows between 2020 and 2024?",
        expected_tables=["county_flow"],
        expected_table="county_flow",
        expected_geo="county",
        expected_year="2020-2024",
        oracle_sql=ranking_sql(
            table="county_flow",
            label_col="subawardee_cty_name",
            value_expr="SUM(subaward_amount)",
            where=["LOWER(subawardee_state) = 'maryland'", "act_dt_fis_yr BETWEEN 2020 AND 2024", "LOWER(rcpt_full_name) <> LOWER(subawardee_full_name)"],
            group_by=["subawardee_cty_name"],
            limit=5,
        ),
        comparison_mode="topk",
        expected_limit=5,
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="year_filter",
        question="At the congressional level, which Maryland districts receive the largest inflows in 2024?",
        expected_tables=["congress_flow"],
        expected_table="congress_flow",
        expected_geo="congress",
        expected_year="2024",
        oracle_sql=ranking_sql(
            table="congress_flow",
            label_col="subawardee_cd_name",
            value_expr="SUM(subaward_amount)",
            where=["LOWER(subawardee_state) = 'maryland'", "act_dt_fis_yr = 2024", "LOWER(rcpt_full_name) <> LOWER(subawardee_full_name)"],
            group_by=["subawardee_cd_name"],
            limit=5,
        ),
        comparison_mode="topk",
        expected_limit=5,
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="time_safety",
        question="Can the chatbot answer state-level flow questions for a specific year like 2018 in the same way as county-level flow?",
        comparison_mode="conceptual",
        caution_expected="required",
        answer_should_include_any=["cautious", "state flow", "year", "not the same"],
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="agency_compare",
        question="Compare top Maryland inflow agencies vs top Maryland outflow agencies. Are they the same?",
        comparison_mode="conceptual",
        expected_tables=["state_flow"],
        expected_table="state_flow",
        answer_should_include_any=["inflow", "outflow", "agency"],
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="industry_compare",
        question="Compare top Maryland inflow industries vs top Maryland outflow industries.",
        comparison_mode="conceptual",
        expected_tables=["state_flow"],
        expected_table="state_flow",
        answer_should_include_any=["inflow", "outflow", "industry"],
    )
    add_case(
        cases,
        section="flow",
        difficulty="Moderate",
        focus="national_ranking",
        question="Which states are involved in the most Maryland-related displayed flow amount?",
        expected_tables=["state_flow"],
        expected_table="state_flow",
        expected_geo="state",
        oracle_sql=(
            "WITH raw AS ("
            " SELECT rcpt_state_name AS counterpart_state, SUM(subaward_amount_year) AS amount"
            " FROM state_flow WHERE LOWER(subawardee_state_name) = 'maryland' AND LOWER(rcpt_state_name) <> LOWER(subawardee_state_name)"
            " GROUP BY 1"
            " UNION ALL "
            " SELECT subawardee_state_name AS counterpart_state, SUM(subaward_amount_year) AS amount"
            " FROM state_flow WHERE LOWER(rcpt_state_name) = 'maryland' AND LOWER(rcpt_state_name) <> LOWER(subawardee_state_name)"
            " GROUP BY 1"
            " ) SELECT counterpart_state, SUM(amount) AS value FROM raw GROUP BY counterpart_state ORDER BY value DESC LIMIT 5"
        ),
        comparison_mode="topk",
        expected_limit=5,
    )
    add_case(
        cases,
        section="flow",
        difficulty="Moderate",
        focus="national_ranking",
        question="Which Maryland counties are involved in the most total displayed flow amount?",
        expected_tables=["county_flow"],
        expected_table="county_flow",
        expected_geo="county",
        oracle_sql=(
            "WITH raw AS ("
            " SELECT rcpt_cty_name AS county, SUM(subaward_amount) AS amount"
            " FROM county_flow WHERE LOWER(rcpt_state) = 'maryland' AND LOWER(rcpt_full_name) <> LOWER(subawardee_full_name) GROUP BY 1"
            " UNION ALL "
            " SELECT subawardee_cty_name AS county, SUM(subaward_amount) AS amount"
            " FROM county_flow WHERE LOWER(subawardee_state) = 'maryland' AND LOWER(rcpt_full_name) <> LOWER(subawardee_full_name) GROUP BY 1"
            " ) SELECT county, SUM(amount) AS value FROM raw GROUP BY county ORDER BY value DESC LIMIT 5"
        ),
        comparison_mode="topk",
        expected_limit=5,
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="largest_pair",
        question="What is the largest origin-destination county pair involving Maryland?",
        expected_tables=["county_flow"],
        expected_table="county_flow",
        expected_geo="county",
        oracle_sql=(
            "SELECT rcpt_cty_name || ' -> ' || subawardee_cty_name AS label, SUM(subaward_amount) AS value"
            " FROM county_flow"
            " WHERE (LOWER(rcpt_state) = 'maryland' OR LOWER(subawardee_state) = 'maryland')"
            " AND LOWER(rcpt_full_name) <> LOWER(subawardee_full_name)"
            " GROUP BY 1 ORDER BY value DESC LIMIT 1"
        ),
        comparison_mode="top1",
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="largest_pair",
        question="What is the largest district-to-district flow involving Maryland?",
        expected_tables=["congress_flow"],
        expected_table="congress_flow",
        expected_geo="congress",
        oracle_sql=(
            "SELECT rcpt_cd_name || ' -> ' || subawardee_cd_name AS label, SUM(subaward_amount) AS value"
            " FROM congress_flow"
            " WHERE (LOWER(rcpt_state) = 'maryland' OR LOWER(subawardee_state) = 'maryland')"
            " AND LOWER(rcpt_full_name) <> LOWER(subawardee_full_name)"
            " GROUP BY 1 ORDER BY value DESC LIMIT 1"
        ),
        comparison_mode="top1",
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="quantiles",
        question="Into which flow quintile does Maryland’s largest displayed flow fall?",
        comparison_mode="conceptual",
        caution_expected="required",
        answer_should_include_any=["q5", "> $1b", "quintile", "1b"],
    )
    add_case(
        cases,
        section="flow",
        difficulty="Moderate",
        focus="filter_semantics",
        question="If a user selects Maryland and Direction = Inflow, which side of the flow must equal Maryland?",
        comparison_mode="conceptual",
        answer_should_include_any=["destination", "selected geography", "maryland"],
    )
    add_case(
        cases,
        section="flow",
        difficulty="Moderate",
        focus="filter_semantics",
        question="If a user selects Maryland and Direction = Outflow, which side of the flow must equal Maryland?",
        comparison_mode="conceptual",
        answer_should_include_any=["origin", "selected geography", "maryland"],
    )
    add_case(
        cases,
        section="flow",
        difficulty="Moderate",
        focus="safety",
        question="If the user asks for “spending by agency in Maryland,” should the chatbot use fund flow data?",
        comparison_mode="conceptual",
        caution_expected="required",
        answer_should_include_any=["no", "subcontract", "flow", "total federal spending"],
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="cross_dimension",
        question="For Maryland inflows, which agency-industry combination appears most important?",
        expected_tables=["state_flow"],
        expected_table="state_flow",
        expected_geo="state",
        oracle_sql=(
            "SELECT agency_name || ' / ' || naics_2digit_title AS label, SUM(subaward_amount_year) AS value"
            " FROM state_flow WHERE LOWER(subawardee_state_name) = 'maryland' AND LOWER(rcpt_state_name) <> LOWER(subawardee_state_name)"
            " GROUP BY 1 ORDER BY value DESC LIMIT 1"
        ),
        comparison_mode="top1",
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="cross_dimension",
        question="For Maryland outflows, which county-agency combinations are largest?",
        expected_tables=["county_flow"],
        expected_table="county_flow",
        expected_geo="county",
        oracle_sql=(
            "SELECT rcpt_cty_name || ' / ' || agency_name AS label, SUM(subaward_amount) AS value"
            " FROM county_flow WHERE LOWER(rcpt_state) = 'maryland' AND LOWER(rcpt_full_name) <> LOWER(subawardee_full_name)"
            " GROUP BY 1 ORDER BY value DESC LIMIT 1"
        ),
        comparison_mode="top1",
    )
    add_case(
        cases,
        section="flow",
        difficulty="Hard",
        focus="safety",
        question="If the chatbot reports a largest flow that is internal to the same location and claims it appears on the map, is that correct?",
        comparison_mode="conceptual",
        caution_expected="required",
        answer_should_include_any=["no", "internal", "excluded", "map"],
    )


def build_cross_dataset_cases(cases: list[GoldCase]) -> None:
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Moderate",
        focus="cross_dataset",
        question="Among the top 5 states by Resident Wage in 2024, which also have high financial literacy in 2021?",
        expected_tables=["contract_state", "finra_state"],
        expected_table="contract_state",
        expected_geo="state",
        oracle_sql=(
            "WITH top5 AS (SELECT state, \"Resident Wage\" FROM contract_state WHERE year = '2024' ORDER BY \"Resident Wage\" DESC LIMIT 5)"
            " SELECT t.state, t.\"Resident Wage\", f.financial_literacy"
            " FROM top5 t JOIN finra_state f ON LOWER(t.state) = LOWER(f.state)"
            " WHERE f.Year = 2021 ORDER BY t.\"Resident Wage\" DESC"
        ),
        comparison_mode="frame",
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Moderate",
        focus="cross_dataset",
        question="Compare Maryland and Virginia on federal contracts in 2024, median household income in 2023, and financial literacy in 2021.",
        expected_tables=["contract_state", "acs_state", "finra_state"],
        expected_geo="state",
        oracle_sql=(
            "WITH c AS (SELECT state, Contracts FROM contract_state WHERE year = '2024'),"
            " a AS (SELECT state, \"Median household income\" AS median_household_income FROM acs_state WHERE Year = 2023),"
            " f AS (SELECT state, financial_literacy FROM finra_state WHERE Year = 2021)"
            " SELECT c.state, c.Contracts, a.median_household_income, f.financial_literacy"
            " FROM c JOIN a ON LOWER(c.state) = LOWER(a.state) JOIN f ON LOWER(c.state) = LOWER(f.state)"
            " WHERE LOWER(c.state) IN ('maryland', 'virginia') ORDER BY c.state"
        ),
        comparison_mode="frame",
    )
    for question in [
        "Do states with high Direct Payments in 2024 also have high poverty rates in 2023?",
        "Do Maryland counties with higher financial constraint scores in 2021 also show higher direct payments in 2024?",
    ]:
        add_case(
            cases,
            section="cross_dataset",
            difficulty="Moderate" if "states" in question else "Hard",
            focus="cross_dataset",
            question=question,
            comparison_mode="conceptual",
            caution_expected="required",
            answer_should_include_any=["descriptive", "correlation", "not causal", "comparison"],
        )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Hard",
        focus="cross_dataset",
        question="Within Maryland counties, do counties with higher federal resident wages in 2024 also tend to have higher median household income in 2023?",
        expected_tables=["contract_county", "acs_county"],
        expected_geo="county",
        oracle_sql=(
            "SELECT corr(c.\"Resident Wage\", a.\"Median household income\") AS value"
            " FROM contract_county c JOIN acs_county a"
            " ON LOWER(c.state) = LOWER(a.state) AND c.county_fips = a.fips"
            " WHERE c.year = '2024' AND a.Year = 2023 AND LOWER(c.state) = 'maryland'"
        ),
        comparison_mode="scalar",
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Hard",
        focus="cross_dataset",
        question="Within Maryland counties, compare the top 10 by federal contracts in 2024 with the top 10 by education >= bachelor's in 2023. How much overlap is there?",
        expected_tables=["contract_county", "acs_county"],
        expected_geo="county",
        oracle_sql=(
            "WITH c AS (SELECT county FROM contract_county WHERE year = '2024' AND LOWER(state) = 'maryland' ORDER BY Contracts DESC LIMIT 10),"
            " a AS (SELECT county FROM acs_county WHERE Year = 2023 AND LOWER(state) = 'maryland' ORDER BY \"Education >= Bachelor's\" DESC LIMIT 10)"
            " SELECT c.county FROM c INNER JOIN a USING (county) ORDER BY c.county"
        ),
        comparison_mode="frame",
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Hard",
        focus="cross_dataset",
        question="Among Maryland congressional districts, which districts rank highly on both grants in 2024 and financial literacy in 2021?",
        expected_tables=["contract_congress", "finra_congress"],
        expected_geo="congress",
        oracle_sql=(
            "SELECT c.cd_118, c.Grants, f.financial_literacy"
            " FROM contract_congress c JOIN finra_congress f ON UPPER(c.cd_118) = UPPER(f.cd_118)"
            " WHERE c.year = '2024' AND f.Year = 2021 AND UPPER(c.cd_118) LIKE 'MD-%'"
            " ORDER BY c.Grants DESC, f.financial_literacy DESC LIMIT 5"
        ),
        comparison_mode="frame",
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Moderate",
        focus="cross_dataset",
        question="Compare California and Texas on total liabilities, federal grants, and financial literacy.",
        expected_tables=["gov_state", "contract_state", "finra_state"],
        expected_geo="state",
        oracle_sql=(
            "WITH g AS (SELECT state, Total_Liabilities FROM gov_state),"
            " c AS (SELECT state, Grants FROM contract_state WHERE year = '2024'),"
            " f AS (SELECT state, financial_literacy FROM finra_state WHERE Year = 2021)"
            " SELECT g.state, g.Total_Liabilities, c.Grants, f.financial_literacy"
            " FROM g JOIN c ON LOWER(g.state) = LOWER(c.state) JOIN f ON LOWER(g.state) = LOWER(f.state)"
            " WHERE LOWER(g.state) IN ('california', 'texas') ORDER BY g.state"
        ),
        comparison_mode="frame",
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Moderate",
        focus="cross_dataset",
        question="Which states are top-10 in both federal contracts (2024) and total liabilities (FY2023)?",
        expected_tables=["contract_state", "gov_state"],
        expected_geo="state",
        oracle_sql=(
            "WITH c AS (SELECT state FROM contract_state WHERE year = '2024' ORDER BY Contracts DESC LIMIT 10),"
            " g AS (SELECT state FROM gov_state ORDER BY Total_Liabilities DESC LIMIT 10)"
            " SELECT c.state FROM c INNER JOIN g USING (state) ORDER BY c.state"
        ),
        comparison_mode="frame",
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Hard",
        focus="cross_dataset",
        question="Within Maryland, which county is simultaneously strong on owner occupancy (2023) and low on financial constraint (2021)?",
        comparison_mode="conceptual",
        caution_expected="required",
        answer_should_include_any=["owner", "constraint", "criteria", "defined"],
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Hard",
        focus="cross_dataset",
        question="Within Maryland, which counties show both high grants in 2024 and high alternative financing in 2021?",
        expected_tables=["contract_county", "finra_county"],
        expected_geo="county",
        oracle_sql=(
            "SELECT c.county, c.Grants, f.alternative_financing"
            " FROM contract_county c JOIN finra_county f"
            " ON LOWER(c.state) = LOWER(f.state) AND c.county_fips = f.fips"
            " WHERE c.year = '2024' AND f.Year = 2021 AND LOWER(c.state) = 'maryland'"
            " ORDER BY c.Grants DESC, f.alternative_financing DESC LIMIT 10"
        ),
        comparison_mode="frame",
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Hard",
        focus="cross_dataset",
        question="Among states with the highest Debt_Ratio, which also receive the most direct payments?",
        expected_tables=["gov_state", "contract_state"],
        expected_geo="state",
        oracle_sql=(
            "WITH d AS (SELECT state FROM gov_state ORDER BY Debt_Ratio DESC LIMIT 10),"
            " p AS (SELECT state FROM contract_state WHERE year = '2024' ORDER BY \"Direct Payments\" DESC LIMIT 10)"
            " SELECT d.state FROM d INNER JOIN p USING (state) ORDER BY d.state"
        ),
        comparison_mode="frame",
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Hard",
        focus="cross_dataset",
        question="Within Maryland districts, compare grants in 2024, bachelor's attainment in 2023, and financial literacy in 2021. Which district looks strongest across all three?",
        comparison_mode="conceptual",
        caution_expected="required",
        answer_should_include_any=["criteria", "grants", "bachelor", "financial literacy"],
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Moderate",
        focus="cross_dataset",
        question="Does the top state by liabilities also rank highly on federal grants?",
        expected_tables=["gov_state", "contract_state"],
        expected_geo="state",
        oracle_sql=(
            "WITH top_liab AS (SELECT state FROM gov_state ORDER BY Total_Liabilities DESC LIMIT 1),"
            " ranked_grants AS (SELECT state, RANK() OVER(ORDER BY Grants DESC) AS grants_rank FROM contract_state WHERE year = '2024')"
            " SELECT t.state, r.grants_rank FROM top_liab t JOIN ranked_grants r USING (state)"
        ),
        comparison_mode="frame",
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Moderate",
        focus="cross_dataset",
        question="Does Maryland’s leadership on Resident Wage in 2024 also show up in Employees Wage?",
        expected_tables=["contract_state"],
        expected_geo="state",
        oracle_sql=(
            "WITH ranked AS ("
            " SELECT state,"
            " RANK() OVER(ORDER BY \"Resident Wage\" DESC) AS resident_wage_rank,"
            " RANK() OVER(ORDER BY \"Employees Wage\" DESC) AS employees_wage_rank"
            " FROM contract_state WHERE year = '2024'"
            " ) SELECT state, resident_wage_rank, employees_wage_rank FROM ranked WHERE LOWER(state) = 'maryland'"
        ),
        comparison_mode="frame",
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Hard",
        focus="cross_dataset",
        question="For Maryland, are the counties with the largest agency-defined spending also the counties with the largest subcontract inflows?",
        expected_support="missing_agency_geo_tables",
        comparison_mode="conceptual",
        caution_expected="required",
        answer_should_include_any=["not available", "agency-defined spending", "county"],
        notes="County agency detail is missing in the current repo.",
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Hard",
        focus="cross_dataset",
        question="For Maryland, which agencies are top in spending but not top in subcontract flow involvement?",
        expected_tables=["spending_state_agency", "state_flow"],
        expected_geo="state",
        oracle_sql=(
            "WITH s AS ("
            f" SELECT agency FROM spending_state_agency WHERE LOWER(state) = 'maryland' AND year = '2024' ORDER BY {composite_spending_expr()} DESC LIMIT 5"
            " ), f AS ("
            " SELECT agency_name AS agency FROM state_flow"
            " WHERE LOWER(rcpt_state_name) = 'maryland' OR LOWER(subawardee_state_name) = 'maryland'"
            " GROUP BY agency_name ORDER BY SUM(subaward_amount_year) DESC LIMIT 5"
            " ) SELECT agency FROM s WHERE agency NOT IN (SELECT agency FROM f) ORDER BY agency"
        ),
        comparison_mode="frame",
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Hard",
        focus="cross_dataset",
        question="Can you rank Maryland districts by a custom score that combines grants (2024), financial literacy (2021), and bachelor's attainment (2023)?",
        comparison_mode="conceptual",
        caution_expected="required",
        answer_should_include_any=["weight", "normalize", "criteria", "custom score"],
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Hard",
        focus="cross_dataset",
        question="Which states are high on both government revenue per capita and federal contracts per 1000?",
        comparison_mode="conceptual",
        caution_expected="required",
        answer_should_include_any=["revenue per capita", "contracts per 1000", "normalized"],
    )
    add_case(
        cases,
        section="cross_dataset",
        difficulty="Hard",
        focus="cross_dataset",
        question="Which Maryland counties are most exposed if you define exposure as high federal contracts, high financial constraint, and lower median household income?",
        comparison_mode="conceptual",
        caution_expected="required",
        answer_should_include_any=["define", "criteria", "weight", "exposure"],
    )


def build_robustness_cases(cases: list[GoldCase]) -> None:
    questions = [
        ("What was Maryland’s Government Finances liabilities in 2021?", ["not available", "fiscal year 2023"]),
        ("What was county-level FINRA financial literacy in Maryland in 2018?", ["not available", "2021"]),
        ("What were agency-level county spending values in 2017?", ["not available", "2020-2024", "2024"]),
        ("Which agencies account for the most spending in Maryland?", ["contracts", "grants", "resident wage", "default"]),
        ("Which state is most dependent on federal money?", ["ambiguous", "contracts", "grants", "direct payments", "per 1000"]),
        ("Which state is strongest economically?", ["not a single metric", "proxy", "define"]),
        ("What is the biggest funding source in Maryland?", ["ambiguous", "agency", "channel", "flow"]),
        ("Can the chatbot add `Employees` to `Contracts` to create a single “impact score” without explanation?", ["no", "mixes counts and dollars"]),
        ("Can the chatbot compare `Debt_Ratio` directly with `Total_Liabilities` and say one state is better because one number is larger?", ["no", "different units", "interpret"]),
        ("If the user asks “Top state with liabilities” and the chatbot answers Washington DC, is that correct?", ["no", "california"]),
        ("If the user asks “Which agency spends the most in Maryland?” and the chatbot answers based only on contracts, is that fully correct?", ["no", "contracts", "grants", "resident wage"]),
        ("If the chatbot uses county-level Connecticut rows that do not map to the current legacy boundaries, how should it present that?", ["rows exist", "boundary", "connecticut"]),
        ("If the chatbot cannot verify a requested year/level combination, what should it do?", ["unavailable", "not guess"]),
        ("How should the chatbot answer causal questions like “Do direct payments cause lower poverty?”", ["not causal", "descriptive", "correlation"]),
        ("How should the chatbot answer when a user asks for an unsupported custom metric?", ["define", "transparent", "unsupported"]),
    ]
    for question, expected in questions:
        add_case(
            cases,
            section="robustness",
            difficulty="Hard" if "How should" in question or "If the chatbot" in question else "Easy",
            focus="robustness",
            question=question,
            comparison_mode="conceptual",
            caution_expected="required",
            answer_should_include_any=expected,
        )


def build_cases() -> list[GoldCase]:
    cases: list[GoldCase] = []
    build_section_1(cases)
    build_acs_cases(cases)
    build_gov_cases(cases)
    build_finra_cases(cases)
    build_contract_static_cases(cases)
    build_agency_cases(cases)
    build_breakdown_cases(cases)
    build_flow_cases(cases)
    build_cross_dataset_cases(cases)
    build_robustness_cases(cases)
    return cases


def _start_server(host: str, port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["USE_LLM_FORMATTER"] = env.get("USE_LLM_FORMATTER", "1")
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            host,
            "--port",
            str(port),
        ],
        cwd=ROOT_DIR,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def _stop_server(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def _wait_for_health(base_url: str, timeout_s: float) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with httpx.Client(base_url=base_url, timeout=1.5) as client:
                response = client.get("/health")
                if response.status_code == 200 and response.json().get("status") == "ok":
                    return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


async def ask_case(client: httpx.AsyncClient, sem: asyncio.Semaphore, case: GoldCase) -> dict[str, Any]:
    payload = {"question": case.question, "history": []}
    queued_started = time.perf_counter()
    async with sem:
        queue_wait_s = round(time.perf_counter() - queued_started, 3)
        started = time.perf_counter()
        status_code: int | None = None
        body: dict[str, Any] = {}
        error: str | None = None
        try:
            response = await client.post("/api/ask", json=payload)
            status_code = response.status_code
            if response.status_code == 200:
                body = response.json()
                error = body.get("error")
            else:
                error = f"HTTP_{response.status_code}"
        except Exception as exc:  # noqa: BLE001
            error = f"REQUEST_FAILED: {exc}"
        latency_s = round(time.perf_counter() - started, 3)
    return {
        "id": case.id,
        "question": case.question,
        "status_code": status_code,
        "queue_wait_s": queue_wait_s,
        "latency_s": latency_s,
        "error": error,
        "answer": body.get("answer") or "",
        "sql": body.get("sql"),
        "data": body.get("data") or [],
        "row_count": int(body.get("row_count") or 0),
    }


def answer_keywords_score(case: GoldCase, answer: str) -> tuple[int, list[str]]:
    notes: list[str] = []
    text = normalize_text(answer)
    if case.comparison_mode != "conceptual" and not case.caution_expected:
        return 2, notes
    score = 2
    for token in case.answer_must_include:
        if normalize_text(token) not in text:
            score -= 1
            notes.append(f"missing keyword: {token}")
    if case.answer_should_include_any:
        if not any(normalize_text(token) in text for token in case.answer_should_include_any):
            score -= 1
            notes.append("missing expected caveat/detail")
    for token in case.answer_must_not_include:
        if token and normalize_text(token) in text:
            score = min(score, 0)
            notes.append(f"contains forbidden token: {token}")
    return max(score, 0), notes


def score_routing(case: GoldCase, result: dict[str, Any]) -> tuple[int, list[str]]:
    notes: list[str] = []
    if result.get("error"):
        return 0, ["request error"]
    if case.comparison_mode == "conceptual":
        score, kw_notes = answer_keywords_score(case, result.get("answer", ""))
        return score, kw_notes
    sql = normalize_text(result.get("sql") or "")
    score = 0
    if any(normalize_text(table) in sql for table in case.expected_tables or ([case.expected_table] if case.expected_table else [])):
        score += 1
    else:
        notes.append("expected table not present in SQL")
    if case.expected_year:
        if case.expected_table and case.expected_table.startswith("gov_"):
            score += 1
        elif normalize_text(case.expected_year) in sql:
            score += 1
        else:
            notes.append("expected year/period not present in SQL")
    else:
        score += 1
    return min(score, 2), notes


def score_units(case: GoldCase, result: dict[str, Any]) -> tuple[int, list[str]]:
    notes: list[str] = []
    if result.get("error"):
        return 0, ["request error"]
    sql = normalize_text(result.get("sql") or "")
    if case.comparison_mode == "conceptual":
        score, kw_notes = answer_keywords_score(case, result.get("answer", ""))
        return score, kw_notes
    if case.expected_metric_kind == "normalized":
        if "per 1000" in sql or "_per_capita" in sql or "per_capita" in sql:
            return 2, notes
        return 0, ["expected normalized metric, but SQL did not use one"]
    if case.expected_metric and case.expected_metric in {"Employees", "Federal Residents"}:
        if any(token in sql for token in ("contracts", "grants", "direct payments", "resident wage")) and case.focus in {"ranking", "comparison"}:
            return 0, ["mixed count metric with dollar metric"]
    if case.expected_metric and normalize_text(case.expected_metric) in sql:
        return 2, notes
    if case.expected_metric and case.expected_metric == "Contracts + Grants + Resident Wage":
        if "resident wage" in sql and "contracts" in sql and "grants" in sql:
            return 2, notes
    return 1, ["metric expression not explicit in SQL"]


def _to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _execute_oracle(case: GoldCase) -> pd.DataFrame:
    if not case.oracle_sql:
        return pd.DataFrame()
    return execute_oracle_query(case.oracle_sql)


def _compare_top1(actual: pd.DataFrame, expected: pd.DataFrame) -> tuple[int, list[str]]:
    notes: list[str] = []
    if expected.empty:
        return 0, ["oracle empty"]
    if actual.empty:
        return 0, ["actual empty"]
    actual_n = normalize_columns(actual)
    expected_n = normalize_columns(expected)
    a_label = detect_label_column(actual_n)
    e_label = detect_label_column(expected_n)
    if not a_label or not e_label:
        return 0, ["could not detect label column"]
    if normalize_text(str(actual_n.iloc[0][a_label])) != normalize_text(str(expected_n.iloc[0][e_label])):
        return 0, [f"top label mismatch: got {actual_n.iloc[0][a_label]}, expected {expected_n.iloc[0][e_label]}"]
    a_num = detect_numeric_columns(actual_n)
    e_num = detect_numeric_columns(expected_n)
    if a_num and e_num:
        if approx_equal(float(actual_n.iloc[0][a_num[0]]), float(expected_n.iloc[0][e_num[0]]), rel_tol=1e-3, abs_tol=1e-3):
            return 2, notes
        return 1, ["top label matched but value differed"]
    return 2, notes


def _compare_topk(actual: pd.DataFrame, expected: pd.DataFrame, k: int) -> tuple[int, list[str]]:
    notes: list[str] = []
    if actual.empty or expected.empty:
        return 0, ["actual or oracle empty"]
    actual_n = normalize_columns(actual).head(k)
    expected_n = normalize_columns(expected).head(k)
    a_label = detect_label_column(actual_n)
    e_label = detect_label_column(expected_n)
    if not a_label or not e_label:
        return 0, ["could not detect label column"]
    actual_labels = [normalize_text(str(v)) for v in actual_n[a_label].tolist()]
    expected_labels = [normalize_text(str(v)) for v in expected_n[e_label].tolist()]
    if actual_labels == expected_labels:
        return 2, notes
    overlap = len(set(actual_labels) & set(expected_labels))
    if overlap >= max(1, k - 1):
        return 1, [f"partial label overlap {overlap}/{k}"]
    return 0, [f"top-k mismatch; overlap {overlap}/{k}"]


def _compare_entity_compare(actual: pd.DataFrame, expected: pd.DataFrame) -> tuple[int, list[str]]:
    notes: list[str] = []
    if actual.empty or expected.empty:
        return 0, ["actual or oracle empty"]
    actual_n = normalize_columns(actual)
    expected_n = normalize_columns(expected)
    a_label = detect_label_column(actual_n)
    e_label = detect_label_column(expected_n)
    if not a_label or not e_label:
        return 0, ["missing label column"]
    actual_labels = [normalize_text(str(v)) for v in actual_n[a_label].tolist()]
    expected_labels = [normalize_text(str(v)) for v in expected_n[e_label].tolist()]
    if actual_labels == expected_labels:
        return 2, notes
    if set(actual_labels) == set(expected_labels):
        return 1, ["entity set matched but order differed"]
    return 0, ["entity set mismatch"]


def _compare_trend(actual: pd.DataFrame, expected: pd.DataFrame) -> tuple[int, list[str]]:
    if actual.empty or expected.empty:
        return 0, ["actual or oracle empty"]
    actual_n = normalize_columns(actual)
    expected_n = normalize_columns(expected)
    if "year" not in actual_n.columns or "year" not in expected_n.columns:
        return 0, ["trend output missing year column"]
    actual_years = actual_n["year"].astype(str).tolist()
    expected_years = expected_n["year"].astype(str).tolist()
    if actual_years == expected_years:
        return 2, []
    if set(actual_years) == set(expected_years):
        return 1, ["year set matched but ordering differed"]
    return 0, ["trend years mismatch"]


def _compare_scalar(actual: pd.DataFrame, expected: pd.DataFrame) -> tuple[int, list[str]]:
    if actual.empty or expected.empty:
        return 0, ["actual or oracle empty"]
    actual_n = normalize_columns(actual)
    expected_n = normalize_columns(expected)
    a_num = detect_numeric_columns(actual_n)
    e_num = detect_numeric_columns(expected_n)
    if not a_num or not e_num:
        return 0, ["scalar comparison missing numeric column"]
    actual_value = float(actual_n.iloc[0][a_num[0]])
    expected_value = float(expected_n.iloc[0][e_num[0]])
    if approx_equal(actual_value, expected_value, rel_tol=1e-3, abs_tol=1e-3):
        return 2, []
    if approx_equal(actual_value, expected_value, rel_tol=5e-2, abs_tol=1e-2):
        return 1, [f"scalar close but not exact: got {actual_value}, expected {expected_value}"]
    return 0, [f"scalar mismatch: got {actual_value}, expected {expected_value}"]


def _compare_frame_exactish(actual: pd.DataFrame, expected: pd.DataFrame) -> tuple[int, list[str]]:
    if actual.empty or expected.empty:
        return 0, ["actual or oracle empty"]
    actual_n = normalize_columns(actual)
    expected_n = normalize_columns(expected)
    shared = [col for col in expected_n.columns if col in actual_n.columns]
    if not shared:
        return 0, ["no shared columns between actual and oracle"]
    actual_s = actual_n[shared].copy()
    expected_s = expected_n[shared].copy()
    for col in shared:
        if pd.api.types.is_numeric_dtype(expected_s[col]):
            actual_s[col] = actual_s[col].astype(float).round(6)
            expected_s[col] = expected_s[col].astype(float).round(6)
        else:
            actual_s[col] = actual_s[col].astype(str).map(normalize_text)
            expected_s[col] = expected_s[col].astype(str).map(normalize_text)
    if actual_s.head(len(expected_s)).equals(expected_s):
        return 2, []
    label_col = detect_label_column(expected_s)
    if label_col and label_col in actual_s.columns:
        actual_labels = set(actual_s[label_col].head(len(expected_s)).tolist())
        expected_labels = set(expected_s[label_col].tolist())
        if actual_labels == expected_labels:
            return 1, ["frame labels matched but metric values/order differed"]
    return 0, ["frame mismatch"]


def score_math(case: GoldCase, result: dict[str, Any]) -> tuple[int, list[str]]:
    notes: list[str] = []
    if result.get("error"):
        return 0, ["request error"]
    if case.comparison_mode == "conceptual":
        score, kw_notes = answer_keywords_score(case, result.get("answer", ""))
        return score, kw_notes
    if case.expected_support != "supported":
        answer = normalize_text(result.get("answer", ""))
        if "not available" in answer or "current dataset" in answer or "state-only" in answer:
            return 1, ["coverage gap surfaced honestly"]
        return 0, ["expected unsupported coverage to be acknowledged"]
    expected = _execute_oracle(case)
    actual = _to_frame(result.get("data") or [])
    if case.comparison_mode == "top1":
        return _compare_top1(actual, expected)
    if case.comparison_mode == "topk":
        return _compare_topk(actual, expected, case.expected_limit or 5)
    if case.comparison_mode == "entity_compare":
        return _compare_entity_compare(actual, expected)
    if case.comparison_mode == "trend":
        return _compare_trend(actual, expected)
    if case.comparison_mode == "scalar":
        return _compare_scalar(actual, expected)
    if case.comparison_mode == "frame":
        return _compare_frame_exactish(actual, expected)
    return (2, notes) if not actual.empty else (0, ["no actual data"])


def score_relevance(case: GoldCase, result: dict[str, Any]) -> tuple[int, list[str]]:
    notes: list[str] = []
    if result.get("error"):
        return 0, ["request error"]
    answer = normalize_text(result.get("answer", ""))
    if not answer:
        return 0, ["empty answer"]
    score = 2
    if case.focus in {"comparison", "filtered_ranking", "district_ranking", "ranking", "top_k", "change", "trend"} and result.get("row_count", 0) == 0:
        return 0, ["zero rows for data question"]
    if case.expected_geo == "county" and "county" not in answer and case.comparison_mode == "conceptual":
        score -= 1
    if case.expected_geo == "congress" and "district" not in answer and case.comparison_mode == "conceptual":
        score -= 1
    return max(score, 0), notes


def score_caution(case: GoldCase, result: dict[str, Any]) -> tuple[int, list[str]]:
    notes: list[str] = []
    answer = normalize_text(result.get("answer", ""))
    if case.caution_expected is None:
        return 2, notes
    if case.caution_expected in {"required", "available_waves_only", "period_vs_year", "year_only_2021"}:
        if any(token in answer for token in ("only", "not available", "period", "multi-year", "2021", "2009", "2012", "2015", "2018", "2021", "default")):
            return 2, notes
        return 0, ["expected caveat not stated"]
    if case.caution_expected == "processed_congress_file":
        if any(token in answer for token in ("processed", "congress-level", "district")):
            return 2, notes
        return 0, ["expected congress-file caveat missing"]
    return 1, ["caution not fully scored"]


def evaluate_case(case: GoldCase, result: dict[str, Any]) -> dict[str, Any]:
    routing_score, routing_notes = score_routing(case, result)
    math_score, math_notes = score_math(case, result)
    units_score, units_notes = score_units(case, result)
    relevance_score, relevance_notes = score_relevance(case, result)
    caution_score, caution_notes = score_caution(case, result)
    total = routing_score + math_score + units_score + relevance_score + caution_score
    flags: list[str] = []
    if result.get("error"):
        flags.append("error")
    if case.expected_support != "supported":
        flags.append("coverage_gap")
    if total <= 4:
        flags.append("low_score")
    if caution_score == 0 and case.caution_expected:
        flags.append("missed_caveat")
    if routing_score == 0:
        flags.append("routing_failure")
    if math_score == 0:
        flags.append("math_failure")
    if units_score == 0:
        flags.append("unit_failure")
    return {
        **asdict(case),
        "result": result,
        "scores": {
            "routing": routing_score,
            "math": math_score,
            "units": units_score,
            "relevance": relevance_score,
            "caution": caution_score,
            "total": total,
        },
        "notes": {
            "routing": routing_notes,
            "math": math_notes,
            "units": units_notes,
            "relevance": relevance_notes,
            "caution": caution_notes,
        },
        "flags": flags,
    }


def summarize(evals: list[dict[str, Any]]) -> dict[str, Any]:
    totals = [e["scores"]["total"] for e in evals]
    flag_counts = Counter()
    by_section: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_focus: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in evals:
        flag_counts.update(item["flags"])
        by_section[item["section"]].append(item)
        by_focus[item["focus"]].append(item)

    def stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
        vals = [r["scores"]["total"] for r in rows]
        return {
            "count": len(rows),
            "avg_total": round(statistics.mean(vals), 2) if vals else 0,
            "avg_routing": round(statistics.mean(r["scores"]["routing"] for r in rows), 2) if rows else 0,
            "avg_math": round(statistics.mean(r["scores"]["math"] for r in rows), 2) if rows else 0,
            "avg_units": round(statistics.mean(r["scores"]["units"] for r in rows), 2) if rows else 0,
            "avg_relevance": round(statistics.mean(r["scores"]["relevance"] for r in rows), 2) if rows else 0,
            "avg_caution": round(statistics.mean(r["scores"]["caution"] for r in rows), 2) if rows else 0,
        }

    unsupported = [e for e in evals if e["expected_support"] != "supported"]
    supported = [e for e in evals if e["expected_support"] == "supported"]

    return {
        "total_cases": len(evals),
        "supported_cases": len(supported),
        "coverage_gap_cases": len(unsupported),
        "avg_total": round(statistics.mean(totals), 2) if totals else 0,
        "median_total": round(statistics.median(totals), 2) if totals else 0,
        "perfect_10_count": sum(1 for e in evals if e["scores"]["total"] == 10),
        "gte_8_count": sum(1 for e in evals if e["scores"]["total"] >= 8),
        "lt_5_count": sum(1 for e in evals if e["scores"]["total"] < 5),
        "flag_counts": dict(flag_counts.most_common()),
        "by_section": {k: stats(v) for k, v in sorted(by_section.items())},
        "by_focus": {k: stats(v) for k, v in sorted(by_focus.items())},
        "lowest_cases": sorted(
            [
                {
                    "id": e["id"],
                    "question": e["question"],
                    "section": e["section"],
                    "focus": e["focus"],
                    "score": e["scores"]["total"],
                    "flags": e["flags"],
                    "error": e["result"].get("error"),
                }
                for e in evals
            ],
            key=lambda row: (row["score"], row["id"]),
        )[:25],
    }


def write_report(path: Path, summary: dict[str, Any], evals: list[dict[str, Any]]) -> None:
    lines: list[str] = []
    lines.append("# Gold Pack Evaluation Report")
    lines.append("")
    lines.append(f"- Total cases: **{summary['total_cases']}**")
    lines.append(f"- Supported cases with direct oracle scoring: **{summary['supported_cases']}**")
    lines.append(f"- Coverage-gap cases: **{summary['coverage_gap_cases']}**")
    lines.append(f"- Average total score: **{summary['avg_total']}/10**")
    lines.append(f"- Median total score: **{summary['median_total']}/10**")
    lines.append(f"- Perfect 10s: **{summary['perfect_10_count']}**")
    lines.append(f"- Score >= 8: **{summary['gte_8_count']}**")
    lines.append(f"- Score < 5: **{summary['lt_5_count']}**")
    lines.append("")
    lines.append("## Flag Counts")
    lines.append("")
    for name, count in summary["flag_counts"].items():
        lines.append(f"- `{name}`: {count}")
    lines.append("")
    lines.append("## Section Breakdown")
    lines.append("")
    lines.append("| Section | Count | Avg Total | Routing | Math | Units | Relevance | Caution |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for section, stats_row in summary["by_section"].items():
        lines.append(
            f"| {section} | {stats_row['count']} | {stats_row['avg_total']:.2f} | {stats_row['avg_routing']:.2f} | "
            f"{stats_row['avg_math']:.2f} | {stats_row['avg_units']:.2f} | {stats_row['avg_relevance']:.2f} | {stats_row['avg_caution']:.2f} |"
        )
    lines.append("")
    lines.append("## Lowest-Scoring Cases")
    lines.append("")
    for row in summary["lowest_cases"]:
        lines.append(f"- Q{row['id']} [{row['section']} / {row['focus']}] score={row['score']} flags={','.join(row['flags']) or 'none'}")
        lines.append(f"  - {row['question']}")
        if row.get("error"):
            lines.append(f"  - Error: {row['error']}")
    lines.append("")
    lines.append("## Notable Coverage Gaps")
    lines.append("")
    coverage_gap_cases = [e for e in evals if "coverage_gap" in e["flags"]][:20]
    if not coverage_gap_cases:
        lines.append("- None detected.")
    else:
        for item in coverage_gap_cases:
            lines.append(f"- Q{item['id']}: {item['question']}")
            lines.append(f"  - Expected support: {item['expected_support']}")
    path.write_text("\n".join(lines), encoding="utf-8")


async def run_eval(args: argparse.Namespace) -> tuple[list[dict[str, Any]], dict[str, Any], Path]:
    init_oracle_conn()

    cases = build_cases()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    checkpoint_path = out_dir / f"gold_pack_eval_checkpoint_{ts}.json"

    base_url = f"http://{args.host}:{args.port}"
    proc = _start_server(args.host, args.port)
    try:
        if not _wait_for_health(base_url, args.startup_timeout_s):
            raise RuntimeError(f"Server did not start on {base_url}")
        sem = asyncio.Semaphore(args.concurrency)
        timeout = httpx.Timeout(args.timeout_s)
        limits = httpx.Limits(max_keepalive_connections=args.concurrency * 2, max_connections=args.concurrency * 3)
        async with httpx.AsyncClient(base_url=base_url, timeout=timeout, limits=limits) as bootstrap_client:
            suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            register_payload = {
                "name": "Gold Eval",
                "email": f"gold-eval-{suffix}@example.com",
                "password": "gold-eval-pass",
            }
            reg = await bootstrap_client.post("/api/auth/register", json=register_payload)
            if reg.status_code not in {200, 201}:
                raise RuntimeError(f"Failed to register eval user: HTTP {reg.status_code} {reg.text[:200]}")
            token = reg.json()["token"]
            headers = {"Authorization": f"Bearer {token}"}

        async with httpx.AsyncClient(base_url=base_url, timeout=timeout, limits=limits, headers=headers) as client:
            tasks = [asyncio.create_task(ask_case(client, sem, case)) for case in cases]
            raw_results: dict[int, dict[str, Any]] = {}
            completed = 0
            started = time.perf_counter()
            for coro in asyncio.as_completed(tasks):
                item = await coro
                raw_results[item["id"]] = item
                completed += 1
                if completed % 10 == 0 or completed == len(cases):
                    elapsed = time.perf_counter() - started
                    checkpoint_rows = [raw_results[k] for k in sorted(raw_results)]
                    checkpoint_path.write_text(
                        json.dumps(
                            {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "completed": completed,
                                "total": len(cases),
                                "results": checkpoint_rows,
                            },
                            indent=2,
                            ensure_ascii=False,
                        ),
                        encoding="utf-8",
                    )
                    print(f"Progress: {completed}/{len(cases)} | elapsed={elapsed:.1f}s")
    finally:
        _stop_server(proc)

    evals = [evaluate_case(case, raw_results.get(case.id, {"error": "missing_result", "answer": "", "sql": None, "data": [], "row_count": 0})) for case in cases]
    summary = summarize(evals)

    results_path = out_dir / f"gold_pack_eval_results_{ts}.json"
    summary_path = out_dir / f"gold_pack_eval_summary_{ts}.json"
    report_path = out_dir / f"gold_pack_eval_report_{ts}.md"

    results_path.write_text(json.dumps(evals, indent=2, ensure_ascii=False), encoding="utf-8")
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    write_report(report_path, summary, evals)

    print(f"Results: {results_path}")
    print(f"Summary: {summary_path}")
    print(f"Report:  {report_path}")
    return evals, summary, report_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the gold pack evaluator against the local MOP app.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8011)
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--timeout-s", type=float, default=180.0)
    parser.add_argument("--startup-timeout-s", type=float, default=20.0)
    parser.add_argument("--output-dir", default="reports/gold_pack_eval")
    return parser.parse_args()


def main() -> None:
    asyncio.run(run_eval(parse_args()))


if __name__ == "__main__":
    main()
