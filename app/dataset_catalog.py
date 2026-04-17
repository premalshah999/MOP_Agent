from __future__ import annotations

from pathlib import Path
from typing import Any

from app.metadata_utils import load_manifest


ROOT_DIR = Path(__file__).resolve().parent.parent
UPLOADS_DIR = ROOT_DIR / "data" / "uploads"


_GROUP_DEFINITIONS: dict[str, dict[str, Any]] = {
    "government_finance": {
        "name": "Government Finances",
        "description": "State, county, and congressional-district fiscal position, liabilities, liquidity, revenue, and expenses.",
        "helper": "Best for liabilities, debt ratio, current ratio, revenue, expenses, and pension burdens.",
        "tables": ["gov_state", "gov_county", "gov_congress"],
    },
    "acs": {
        "name": "Census (ACS Demographics)",
        "description": "Demographic, income, education, poverty, and housing indicators across geographies.",
        "helper": "Best for median household income, poverty, education, and ownership patterns.",
        "tables": ["acs_state", "acs_county", "acs_congress"],
    },
    "federal_spending": {
        "name": "Federal Spending",
        "description": "Contracts, grants, direct payments, wages, and workforce-related metrics across geographies.",
        "helper": "Best for national spending totals, 2024 comparisons, and relative exposure questions.",
        "tables": ["contract_state", "contract_county", "contract_congress"],
    },
    "federal_spending_agency": {
        "name": "Federal Spending by Agency",
        "description": "State-level agency detail plus state spending-breakdown totals used by the dashboard.",
        "helper": "Best for questions about which agencies dominate spending in a specific state.",
        "tables": ["spending_state_agency", "spending_state"],
        "notes": [
            "State-level agency detail is available here now.",
            "County and congressional agency downloads are not currently exposed in this runtime.",
        ],
    },
    "finra": {
        "name": "FINRA Financial Literacy",
        "description": "Financial literacy, constraint, satisfaction, alternative financing, and risk aversion indicators.",
        "helper": "Best for household financial capability questions in 2021.",
        "tables": ["finra_state", "finra_county", "finra_congress"],
    },
    "fund_flow": {
        "name": "Fund Flow",
        "description": "Subaward flows between recipient and subawardee geographies, with origin and destination detail.",
        "helper": "Best for subcontract inflow, outflow, and origin-destination questions.",
        "tables": ["state_flow", "county_flow", "congress_flow"],
    },
    "cross_dataset": {
        "name": "Cross-Dataset Analysis",
        "description": "Joined analysis across multiple datasets such as poverty versus grants or literacy versus debt.",
        "helper": "This is an analysis mode, not a single downloadable file.",
        "tables": [],
        "notes": [
            "Use the family downloads above when you want to reproduce a cross-dataset analysis locally.",
        ],
    },
}

_TABLE_LABELS: dict[str, tuple[str, str, str]] = {
    "gov_state": ("State", "state", "Fiscal Year 2023 finance totals by state."),
    "gov_county": ("County", "county", "Fiscal Year 2023 finance totals by county."),
    "gov_congress": ("Congressional District", "congress", "Fiscal Year 2023 finance totals by congressional district."),
    "acs_state": ("State", "state", "ACS 5-year demographic indicators by state."),
    "acs_county": ("County", "county", "ACS 5-year demographic indicators by county."),
    "acs_congress": ("Congressional District", "congress", "ACS 5-year demographic indicators by congressional district."),
    "contract_state": ("State", "state", "Federal spending totals by state."),
    "contract_county": ("County", "county", "Federal spending totals by county."),
    "contract_congress": ("Congressional District", "congress", "Federal spending totals by congressional district."),
    "spending_state": ("State Totals (Breakdown)", "state", "State-only spending-breakdown totals used by the dashboard."),
    "spending_state_agency": ("State Agency Detail", "state", "State-by-agency breakdown used for agency composition views."),
    "finra_state": ("State", "state", "FINRA capability metrics by state."),
    "finra_county": ("County", "county", "FINRA capability metrics by county."),
    "finra_congress": ("Congressional District", "congress", "FINRA capability metrics by congressional district."),
    "state_flow": ("State Flow Records", "state", "State-level subcontract flow records."),
    "county_flow": ("County Flow Records", "county", "County-level subcontract flow records."),
    "congress_flow": ("Congressional Flow Records", "congress", "Congressional-district subcontract flow records."),
}


def _table_downloads(table_name: str, info: dict[str, Any]) -> dict[str, str]:
    downloads: dict[str, str] = {}

    parquet_path = ROOT_DIR / info["path"]
    if parquet_path.exists():
        downloads["parquet"] = f"/api/datasets/download/{table_name}?format=parquet"

    source_file = info.get("source_file")
    if source_file:
        xlsx_path = UPLOADS_DIR / source_file
        if xlsx_path.exists():
            downloads["xlsx"] = f"/api/datasets/download/{table_name}?format=xlsx"

    return downloads


def dataset_download_path(table_name: str, fmt: str) -> tuple[Path, str]:
    manifest = load_manifest()
    info = manifest.get(table_name)
    if not info:
        raise KeyError(f"Unknown dataset table: {table_name}")

    fmt_normalized = fmt.lower()
    if fmt_normalized == "parquet":
        path = ROOT_DIR / info["path"]
        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found for {table_name}")
        return path, f"{table_name}.parquet"

    if fmt_normalized == "xlsx":
        source_file = info.get("source_file")
        if not source_file:
            raise FileNotFoundError(f"No source workbook recorded for {table_name}")
        path = UPLOADS_DIR / source_file
        if not path.exists():
            raise FileNotFoundError(f"Workbook file not found for {table_name}")
        return path, source_file

    raise ValueError("Supported formats are parquet and xlsx.")


def build_dataset_catalog() -> list[dict[str, Any]]:
    manifest = load_manifest()
    datasets: list[dict[str, Any]] = []

    for dataset_id, definition in _GROUP_DEFINITIONS.items():
        tables: list[dict[str, Any]] = []
        for table_name in definition.get("tables", []):
            info = manifest.get(table_name)
            if not info:
                continue
            label, grain, summary = _TABLE_LABELS.get(
                table_name,
                (table_name.replace("_", " ").title(), "table", "Dataset export."),
            )
            tables.append(
                {
                    "tableName": table_name,
                    "label": label,
                    "grain": grain,
                    "summary": summary,
                    "rows": info.get("rows", 0),
                    "columns": list(info.get("columns", [])),
                    "sourceFile": info.get("source_file"),
                    "runtimePath": info.get("path"),
                    "downloads": _table_downloads(table_name, info),
                }
            )

        datasets.append(
            {
                "id": dataset_id,
                "name": definition["name"],
                "description": definition["description"],
                "helper": definition["helper"],
                "notes": list(definition.get("notes", [])),
                "tables": tables,
            }
        )

    return datasets
