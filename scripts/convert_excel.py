from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import pandas as pd

INPUT_DIR = Path("data/uploads")
OUTPUT_DIR = Path("data/parquet")
MANIFEST_PATH = Path("data/schema/manifest.json")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)

# Canonical mapping used by metadata.json and SQL agent
TABLE_FILE_CANDIDATES = {
    "acs_state": ["acs_state.xlsx"],
    "acs_county": ["acs_county.xlsx"],
    "acs_congress": ["acs_congress.xlsx"],
    "gov_state": ["gov_state.xlsx"],
    "gov_county": ["gov_county.xlsx"],
    "gov_congress": ["gov_congress.xlsx"],
    "contract_state": ["contract_state.xlsx"],
    "contract_county": ["contract_county.xlsx"],
    "contract_congress": ["contract_congress.xlsx"],
    "finra_state": ["finra_state.xlsx"],
    "finra_county": ["finra_county.xlsx"],
    "finra_congress": ["finra_congress.xlsx"],
    "spending_state": ["spending_state.xlsx", "federal_spending_breakdown_state.xlsx"],
    "spending_state_agency": ["spending_state_agency.xlsx", "federal_spending_by_agency_state.xlsx"],
    "state_flow": ["state_flow.xlsx", "fund_flow_state.xlsx"],
    "county_flow": ["county_flow.xlsx", "fund_flow_county.xlsx"],
    "congress_flow": ["congress_flow.xlsx", "fund_flow_congressional_district.xlsx"],
    # Optional agency-granular derivatives
    "contract_state_agency": ["contract_state_Mar2.xlsx"],
    "contract_county_agency": ["contract_county_Mar2.xlsx"],
    "contract_cd_agency": ["contract_cd_Mar2.xlsx"],
}


def find_existing_file(candidates: list[str]) -> Optional[Path]:
    for name in candidates:
        path = INPUT_DIR / name
        if path.exists():
            return path
    return None


def main() -> None:
    manifest: dict[str, dict[str, object]] = {}

    for table_name, candidates in TABLE_FILE_CANDIDATES.items():
        in_file = find_existing_file(candidates)
        if in_file is None:
            print(f"  SKIP (not found): {table_name} [{', '.join(candidates)}]")
            continue

        df = pd.read_excel(in_file)
        out_file = OUTPUT_DIR / f"{table_name}.parquet"
        df.to_parquet(out_file, index=False)

        manifest[table_name] = {
            "path": str(out_file).replace("\\", "/"),
            "source_file": in_file.name,
            "rows": int(len(df)),
            "columns": [str(c) for c in df.columns],
        }
        print(f"  OK  {table_name}: {len(df)} rows, {len(df.columns)} cols")

    with MANIFEST_PATH.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nDone. {len(manifest)} tables converted.")
    print(f"Manifest written to {MANIFEST_PATH}")


if __name__ == "__main__":
    main()
