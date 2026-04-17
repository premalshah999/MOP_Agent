from __future__ import annotations

from functools import lru_cache
from typing import Any

from app.metadata_utils import available_tables, load_metadata


_FAMILY_SPECS: dict[str, dict[str, Any]] = {
    "acs": {
        "label": "ACS / Census demographics",
        "description": "Demographic and household characteristics across states, counties, and congressional districts.",
        "table_by_geo": {
            "state": "acs_state",
            "county": "acs_county",
            "congress": "acs_congress",
        },
        "years": {
            "state": ("2010", "2023"),
            "county": ("2010", "2023"),
            "congress": ("2010", "2023"),
        },
    },
    "gov": {
        "label": "Government Finances",
        "description": "Local-government liabilities, assets, revenue, expenses, fiscal ratios, and derived finance metrics.",
        "table_by_geo": {
            "state": "gov_state",
            "county": "gov_county",
            "congress": "gov_congress",
        },
        "years": {
            "state": ("Fiscal Year 2023",),
            "county": ("Fiscal Year 2023",),
            "congress": ("Fiscal Year 2023",),
        },
    },
    "finra": {
        "label": "FINRA financial literacy",
        "description": "Survey-derived financial literacy, constraint, alternative financing, satisfaction, and risk-aversion scores.",
        "table_by_geo": {
            "state": "finra_state",
            "county": "finra_county",
            "congress": "finra_congress",
        },
        "years": {
            "state": ("2009", "2012", "2015", "2018", "2021"),
            "county": ("2021",),
            "congress": ("2021",),
        },
    },
    "contract": {
        "label": "Federal Spending",
        "description": "Federal contracts, grants, direct payments, resident wages, and workforce counts by geography.",
        "table_by_geo": {
            "state": "contract_state",
            "county": "contract_county",
            "congress": "contract_congress",
        },
        "years": {
            "state": ("2020-2024", "2024"),
            "county": ("2020-2024", "2024"),
            "congress": ("2020-2024", "2024"),
        },
    },
    "agency": {
        "label": "Federal Spending by Agency",
        "description": "Agency-level federal spending by geography. Default spending composite is Contracts + Grants + Resident Wage.",
        "table_by_geo": {
            "state": "spending_state_agency",
            "county": "contract_county_agency",
            "congress": "contract_cd_agency",
        },
        "years": {
            "state": ("2020-2024", "2024"),
            "county": ("2020-2024", "2024"),
            "congress": ("2020-2024", "2024"),
        },
    },
    "breakdown": {
        "label": "Federal Spending Breakdown",
        "description": "State-only breakdown tables used for the federal spending map and state agency composition views.",
        "table_by_geo": {
            "state": "spending_state",
            "detail": "spending_state_agency",
        },
        "years": {
            "state": ("2020-2024", "2024"),
            "detail": ("2020-2024", "2024"),
        },
    },
    "flow": {
        "label": "Fund Flow",
        "description": "Directional subcontract flows between origin and destination geographies, with agency and industry context.",
        "table_by_geo": {
            "state": "state_flow",
            "county": "county_flow",
            "congress": "congress_flow",
        },
        "years": {
            "state": ("no explicit state-level year filter in the dashboard",),
            "county": ("year-filtered via act_dt_fis_yr",),
            "congress": ("year-filtered via act_dt_fis_yr",),
        },
    },
}

_SCHEMA_FACTS: dict[str, str] = {
    "cd_118": "The `cd_118` field is the congressional district identifier stored as text, such as `MD-05`. It should be treated as a text key, not cast to an integer.",
    "year_fields": "Year fields are not fully numeric across the project. Examples include `2024`, `2020-2024`, and `Fiscal Year 2023`, so year handling needs to preserve string labels where appropriate.",
    "state_casing": "State names are not formatted consistently across datasets. Some tables use lowercase, some Title Case, and some UPPERCASE, so state values should be normalized before matching.",
    "per_1000": "Stored `Per 1000` and `_per_capita` columns should be treated as published normalized metrics. The chatbot should use those fields directly unless it is explicitly creating and labeling a custom derived metric.",
    "agency_spending_default": 'For generic "spending by agency" questions, the default spending definition is `Contracts + Grants + Resident Wage`. It should not silently include `Direct Payments`, `Employees`, `Federal Residents`, or `Employees Wage`.',
}


def _geo_status(table_name: str, loaded_tables: set[str], documented_tables: set[str]) -> dict[str, bool]:
    return {
        "loaded": table_name in loaded_tables,
        "documented": table_name in documented_tables,
    }


@lru_cache(maxsize=1)
def semantic_catalog() -> dict[str, dict[str, Any]]:
    loaded_tables = available_tables()
    metadata_tables = set(load_metadata().get("tables", {}).keys())
    out: dict[str, dict[str, Any]] = {}

    for family_key, spec in _FAMILY_SPECS.items():
        geo_catalog: dict[str, dict[str, Any]] = {}
        for geo, table_name in spec["table_by_geo"].items():
            status = _geo_status(table_name, loaded_tables, metadata_tables)
            geo_catalog[geo] = {
                "table": table_name,
                "loaded": status["loaded"],
                "documented": status["documented"],
                "years": tuple(spec["years"].get(geo, ())),
            }

        out[family_key] = {
            "label": spec["label"],
            "description": spec["description"],
            "geographies": geo_catalog,
            "available_geographies": [geo for geo, info in geo_catalog.items() if info["loaded"]],
            "documented_geographies": [geo for geo, info in geo_catalog.items() if info["documented"]],
            "missing_runtime_geographies": [geo for geo, info in geo_catalog.items() if info["documented"] and not info["loaded"]],
        }

    return out


def family_info(family_key: str) -> dict[str, Any]:
    return semantic_catalog().get(family_key, {})


def runtime_table_loaded(table_name: str) -> bool:
    return table_name in available_tables()


def schema_fact(key: str) -> str | None:
    return _SCHEMA_FACTS.get(key)


def schema_facts() -> dict[str, str]:
    return dict(_SCHEMA_FACTS)
