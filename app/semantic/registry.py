from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Any

from app.paths import MANIFEST_PATH, METADATA_PATH
from app.semantic.models import DatasetDefinition, DimensionDefinition, MetricDefinition, SemanticRegistrySnapshot


_SPECIAL_IDENTIFIER = re.compile(r"[^A-Za-z0-9_]")
REGISTRY_VERSION = "semantic-registry-v1"


def quote_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    if _SPECIAL_IDENTIFIER.search(name) or not name or name[0].isdigit():
        return f'"{escaped}"'
    return escaped


def mart_view_name(table_name: str) -> str:
    return f"mart_{table_name}"


def _load_json(path) -> dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def _dimension(
    id_: str,
    column: str,
    label: str,
    *,
    description: str = "",
    synonyms: list[str] | None = None,
) -> DimensionDefinition:
    return DimensionDefinition(
        id=id_,
        column=column,
        label=label,
        description=description,
        synonyms=synonyms or [],
    )


def _metric(
    id_: str,
    label: str,
    sql: str,
    *,
    description: str,
    unit: str = "value",
    aggregation: str = "sum",
    synonyms: list[str] | None = None,
    default_for: list[str] | None = None,
    semantic_concept: str | None = None,
    semantic_variant: str | None = None,
) -> MetricDefinition:
    return MetricDefinition(
        id=id_,
        label=label,
        sql=sql,
        description=description,
        unit=unit,
        aggregation=aggregation,
        synonyms=synonyms or [],
        default_for=default_for or [],
        semantic_concept=semantic_concept,
        semantic_variant=semantic_variant,
    )


def _base_dimensions(table_name: str, columns: list[str]) -> dict[str, DimensionDefinition]:
    dimensions: dict[str, DimensionDefinition] = {}
    if "state" in columns:
        dimensions["state"] = _dimension("state", "state", "State", synonyms=["state", "place"])
    if "county" in columns:
        dimensions["county"] = _dimension("county", "county", "County", synonyms=["county", "counties"])
    if "cd_118" in columns:
        dimensions["congressional_district"] = _dimension(
            "congressional_district",
            "cd_118",
            "Congressional district",
            synonyms=["district", "congress", "congressional district"],
        )
    if "agency" in columns:
        dimensions["agency"] = _dimension("agency", "agency", "Agency", synonyms=["agency", "department"])
    if "agency_name" in columns:
        dimensions["agency"] = _dimension("agency", "agency_name", "Agency", synonyms=["agency", "department"])
    if "naics_2digit_title" in columns:
        dimensions["industry"] = _dimension("industry", "naics_2digit_title", "Industry", synonyms=["industry", "sector", "naics"])
    if "year" in columns:
        dimensions["year"] = _dimension("year", "year", "Year", synonyms=["year", "period"])
    if "Year" in columns:
        dimensions["year"] = _dimension("year", "Year", "Year", synonyms=["year", "period"])
    if "act_dt_fis_yr" in columns:
        dimensions["year"] = _dimension("year", "act_dt_fis_yr", "Fiscal year", synonyms=["year", "fiscal year"])
    if "rcpt_state_name" in columns:
        dimensions["source_state"] = _dimension("source_state", "rcpt_state_name", "Prime awardee state", synonyms=["from state", "origin state", "prime state"])
    if "subawardee_state_name" in columns:
        dimensions["destination_state"] = _dimension("destination_state", "subawardee_state_name", "Subawardee state", synonyms=["to state", "destination state", "recipient state"])
    if "rcpt_state" in columns:
        dimensions["source_state"] = _dimension("source_state", "rcpt_state", "Prime awardee state", synonyms=["from state", "origin state", "prime state"])
    if "subawardee_state" in columns:
        dimensions["destination_state"] = _dimension("destination_state", "subawardee_state", "Subawardee state", synonyms=["to state", "destination state", "recipient state"])
    if "rcpt_full_name" in columns:
        dimensions["source_place"] = _dimension("source_place", "rcpt_full_name", "Prime awardee place", synonyms=["from", "origin", "prime awardee"])
    if "subawardee_full_name" in columns:
        dimensions["destination_place"] = _dimension("destination_place", "subawardee_full_name", "Subawardee place", synonyms=["to", "destination", "subawardee"])
    return dimensions


def _family_for(table_name: str) -> str:
    if table_name.startswith("acs_"):
        return "acs"
    if table_name.startswith("gov_"):
        return "government_finance"
    if table_name.startswith("finra_"):
        return "finra"
    if table_name.startswith("contract_"):
        return "federal_funding"
    if table_name.startswith("spending_"):
        return "federal_spending"
    if table_name.endswith("_flow"):
        return "fund_flow"
    return "general"


def _normalize_geography(raw: str, table_name: str, columns: list[str]) -> str:
    value = raw.lower().replace("-", "_")
    if "flow" in table_name:
        if table_name.startswith("state_"):
            return "state"
        if table_name.startswith("county_"):
            return "county"
        return "congress"
    if "congress" in value or "district" in value or "cd_118" in columns:
        return "congress"
    if "county" in value or "county" in columns:
        return "county"
    return "state"


def _label_column_for(table_name: str, columns: list[str]) -> str:
    if table_name == "state_flow":
        return "subawardee_state_name"
    if table_name in {"county_flow", "congress_flow"}:
        return "subawardee_full_name"
    if "county" in columns:
        return "county"
    if "cd_118" in columns:
        return "cd_118"
    if "agency" in columns:
        return "agency"
    return "state"


def _available_years_for(table_name: str) -> list[str | int]:
    if table_name.startswith("acs_"):
        return [2023]
    if table_name == "finra_state":
        return [2009, 2012, 2015, 2018, 2021]
    if table_name.startswith("finra_"):
        return [2021]
    if table_name.startswith("contract_") or table_name.startswith("spending_"):
        return ["2024", "2020-2024"]
    if table_name in {"county_flow", "congress_flow"}:
        return [2018, 2019, 2020, 2021, 2022, 2023, 2024]
    if table_name.startswith("gov_"):
        return ["Fiscal Year 2023"]
    return []


def _dataset_base(table_name: str, manifest: dict[str, Any], metadata: dict[str, Any]) -> DatasetDefinition:
    info = manifest[table_name]
    meta = metadata.get("tables", {}).get(table_name, {})
    columns = list(info.get("columns", []))
    geography = _normalize_geography(str(meta.get("geography") or ""), table_name, columns)
    label_column = _label_column_for(table_name, columns)
    year_column = "year" if "year" in columns else "Year" if "Year" in columns else "act_dt_fis_yr" if "act_dt_fis_yr" in columns else None
    default_year: str | int | None = None
    if table_name.startswith("acs_"):
        default_year = 2023
    elif table_name.startswith("finra_"):
        default_year = 2021
    elif table_name.startswith("contract_") or table_name.startswith("spending_"):
        default_year = "2024"
    elif table_name in {"county_flow", "congress_flow"}:
        default_year = 2024

    return DatasetDefinition(
        id=table_name,
        display_name=table_name.replace("_", " ").title(),
        description=meta.get("description") or f"Curated analytical dataset for `{table_name}`.",
        table_name=table_name,
        view_name=mart_view_name(table_name),
        grain=meta.get("grain") or f"One row per {geography} / period where applicable.",
        geography=geography,
        family=_family_for(table_name),
        year_column=year_column,
        default_year=default_year,
        available_years=_available_years_for(table_name),
        label_column=label_column,
        dimensions=_base_dimensions(table_name, columns),
        columns=columns,
        caveats=list(meta.get("caveats", [])),
    )


def _contract_metrics(columns: list[str]) -> dict[str, MetricDefinition]:
    metrics: dict[str, MetricDefinition] = {}
    components = [col for col in ("Contracts", "Grants", "Resident Wage", "Direct Payments") if col in columns]
    if components:
        expr = " + ".join(f"COALESCE({quote_identifier(col)}, 0)" for col in components)
        metrics["total_federal_funding"] = _metric(
            "total_federal_funding",
            "Total federal funding",
            f"({expr})",
            description=(
                "Default broad federal funding metric: the sum of Contracts, Grants, Resident Wage, and Direct Payments where available."
            ),
            unit="dollars",
            aggregation="sum",
            synonyms=[
                "funding", "federal funding", "federal money", "federal spending", "total funding",
                "spending", "federal support", "public funding", "government funding", "money received",
            ],
            default_for=["funding", "federal funding", "federal money", "federal spending", "money"],
        )
    per_1000_components = [
        col
        for col in (
            "Contracts Per 1000",
            "Grants Per 1000",
            "Resident Wage Per 1000",
            "Direct Payments Per 1000",
        )
        if col in columns
    ]
    if per_1000_components:
        per_1000_expr = " + ".join(f"COALESCE({quote_identifier(col)}, 0)" for col in per_1000_components)
        metrics["total_federal_funding_per_1000"] = _metric(
            "total_federal_funding_per_1000",
            "Total federal funding per 1,000",
            f"({per_1000_expr})",
            description=(
                "Default broad federal funding intensity metric: the sum of per-1,000 Contracts, Grants, "
                "Resident Wage, and Direct Payments where available."
            ),
            unit="dollars per 1,000 residents",
            aggregation="avg",
            synonyms=[
                "funding per 1000",
                "federal funding per 1000",
                "federal money per 1000",
                "federal spending per 1000",
                "funding per resident",
                "federal funding per resident",
                "per capita federal funding",
            ],
            default_for=["funding per 1000", "federal funding per 1000", "funding per resident"],
        )
    metric_specs = {
        "Contracts": ("contracts", "Contracts", "dollars", ["contracts", "contract", "contract funding", "procurement", "deals", "deal", "biggest deals", "awards", "award obligations"]),
        "Grants": ("grants", "Grants", "dollars", ["grants", "grant", "grant funding", "grant money"]),
        "Resident Wage": ("resident_wage", "Resident wage", "dollars", ["resident wage", "resident wages", "wages", "payroll"]),
        "Direct Payments": ("direct_payments", "Direct payments", "dollars", ["direct payments", "direct payment", "payments", "benefits"]),
        "Contracts Per 1000": ("contracts_per_1000", "Contracts per 1,000", "dollars per 1,000 residents", ["contracts per 1000", "contracts per thousand", "contract funding per resident"]),
        "Grants Per 1000": ("grants_per_1000", "Grants per 1,000", "dollars per 1,000 residents", ["grants per 1000", "grants per thousand", "grant funding per resident"]),
        "Resident Wage Per 1000": ("resident_wage_per_1000", "Resident wage per 1,000", "dollars per 1,000 residents", ["resident wage per 1000", "wages per 1000", "wages per resident"]),
        "Direct Payments Per 1000": ("direct_payments_per_1000", "Direct payments per 1,000", "dollars per 1,000 residents", ["direct payments per 1000", "payments per 1000", "payments per resident"]),
        "Federal Residents": ("federal_residents", "Federal residents", "count", ["federal residents", "federal resident population"]),
        "Federal Residents Per 1000": ("federal_residents_per_1000", "Federal residents per 1,000", "people per 1,000 residents", ["federal residents per 1000", "federal residents per thousand"]),
        "Employees": ("employees", "Employees", "count", ["employees", "employee", "federal employees", "federal jobs", "federal employment", "jobs"]),
        "Employees Per 1000": ("employees_per_1000", "Employees per 1,000", "employees per 1,000 residents", ["employees per 1000", "employees per thousand", "federal jobs per resident", "jobs per capita"]),
        "Employees Wage": ("employees_wage", "Employees wage", "dollars", ["employee wages", "employees wage", "federal wages", "federal payroll"]),
        "Employees Wage Per 1000": ("employees_wage_per_1000", "Employees wage per 1,000", "dollars per 1,000 residents", ["employee wages per 1000", "federal wages per thousand", "payroll per resident"]),
    }
    for column, (id_, label, unit, synonyms) in metric_specs.items():
        if column in columns:
            metrics[id_] = _metric(
                id_,
                label,
                quote_identifier(column),
                description=f"Native `{column}` field.",
                unit=unit,
                aggregation="avg" if "per 1,000" in unit else "sum",
                synonyms=synonyms,
            )
    return metrics


def _acs_metrics(columns: list[str]) -> dict[str, MetricDefinition]:
    mapping = {
        "Total population": ("population", "Total population", "count", ["population", "people", "residents"], "population", "count"),
        "Age 18-65": ("working_age_share", "Age 18-65 share", "percent", ["working age", "age 18 65"], "working_age", "share"),
        "Median household income": ("median_household_income", "Median household income", "dollars", ["income", "household income", "median income", "median household income", "hh income", "earnings"], "median_household_income", "amount"),
        "Below poverty": ("poverty_rate", "Poverty rate", "percent", ["poverty", "poverty rate", "below poverty", "poor", "low income"], "poverty", "share"),
        "Education >= High School": ("high_school_attainment", "High school attainment", "percent", ["high school", "education", "high school degree", "high school diploma"], "high_school_attainment", "share"),
        "Education >= Bachelor's": ("bachelors_attainment", "Bachelor's attainment", "percent", ["bachelor", "bachelors", "bachelor degree", "college degree", "college attainment", "education"], "bachelors_attainment", "share"),
        "Education >= Graduate": ("graduate_attainment", "Graduate degree attainment", "percent", ["graduate degree", "advanced degree", "masters", "postgraduate"], "graduate_attainment", "share"),
        "White": ("white_share", "White population share", "percent", ["white share", "white percentage", "white percent", "white population share", "white ratio"], "white_population", "share"),
        "Black": ("black_share", "Black population share", "percent", ["black share", "black percentage", "black percent", "black population share", "black ratio", "african american share"], "black_population", "share"),
        "Asian": ("asian_share", "Asian population share", "percent", ["asian share", "asian percentage", "asian percent", "asian population share", "asian ratio"], "asian_population", "share"),
        "Hispanic": ("hispanic_share", "Hispanic population share", "percent", ["hispanic share", "hispanic percentage", "hispanic percent", "hispanic population share", "latino share", "latina share", "latinx share"], "hispanic_population", "share"),
        "Income >$50K": ("income_over_50k", "Households over $50K income", "percent", ["income over 50k"], "income_over_50k", "share"),
        "Income >$100K": ("income_over_100k", "Households over $100K income", "percent", ["income over 100k"], "income_over_100k", "share"),
        "Income >$200K": ("income_over_200k", "Households over $200K income", "percent", ["income over 200k", "high income"], "income_over_200k", "share"),
        "Owner occupied": ("owner_occupied", "Owner-occupied housing share", "percent", ["homeownership", "home ownership", "owner occupied", "owners"], "owner_occupied", "share"),
        "Renter occupied": ("renter_occupied", "Renter-occupied housing share", "percent", ["renters", "renter occupied", "rental", "renting"], "renter_occupied", "share"),
    }
    metrics = {
        id_: _metric(
            id_,
            label,
            quote_identifier(column),
            description=f"ACS field `{column}`.",
            unit=unit,
            aggregation="avg" if unit == "percent" else "sum",
            synonyms=synonyms,
            semantic_concept=concept,
            semantic_variant=variant,
        )
        for column, (id_, label, unit, synonyms, concept, variant) in mapping.items()
        if column in columns
    }
    count_specs = {
        "White": (
            "white_population_count",
            "White population count",
            ["white population", "white residents", "white people", "white population amount", "number of white residents", "absolute white population"],
            "white_population",
        ),
        "Black": (
            "black_population_count",
            "Black population count",
            ["black population", "black residents", "black people", "african american population", "black population amount", "number of black residents", "absolute black population"],
            "black_population",
        ),
        "Asian": (
            "asian_population_count",
            "Asian population count",
            ["asian population", "asian residents", "asian people", "asian population amount", "number of asian residents", "amount of asian population", "absolute asian population"],
            "asian_population",
        ),
        "Hispanic": (
            "hispanic_population_count",
            "Hispanic population count",
            ["hispanic population", "hispanic residents", "latino population", "latina population", "latinx population", "hispanic population amount", "number of hispanic residents", "absolute hispanic population"],
            "hispanic_population",
        ),
    }
    if "Total population" in columns:
        for column, (id_, label, synonyms, concept) in count_specs.items():
            if column in columns:
                metrics[id_] = _metric(
                    id_,
                    label,
                    f"(({quote_identifier(column)} / 100.0) * {quote_identifier('Total population')})",
                    description=f"Derived ACS count: `{column}` share multiplied by `Total population`.",
                    unit="count",
                    aggregation="sum",
                    synonyms=synonyms,
                    semantic_concept=concept,
                    semantic_variant="count",
                )
    return metrics


def _gov_metrics(columns: list[str]) -> dict[str, MetricDefinition]:
    mapping = {
        "Total_Liabilities": ("total_liabilities", "Total liabilities", "dollars", ["liability", "liabilities", "total liability", "total liabilities", "government liabilities"]),
        "Total_Liabilities_per_capita": ("liabilities_per_capita", "Liabilities per capita", "dollars per person", ["liability per capita", "liabilities per capita", "liabilities per person", "p liability"]),
        "Current_Assets": ("current_assets", "Current assets", "dollars", ["current asset", "current assets"]),
        "Current_Assets_per_capita": (
            "current_assets_per_capita",
            "Current assets per capita",
            "dollars per person",
            ["current asset per capita", "current assets per capita", "current assets per person"],
        ),
        "Total_Assets": (
            "total_assets",
            "Total assets",
            "dollars",
            ["asset", "assets", "total asset", "total assets", "government assets"],
        ),
        "Total_Assets_per_capita": (
            "total_assets_per_capita",
            "Total assets per capita",
            "dollars per person",
            ["asset per capita", "assets per capita", "per capita asset", "per capita assets", "p asset", "p assets", "assets per person"],
        ),
        "Revenue": ("revenue", "Revenue", "dollars", ["revenue", "revenues", "income received", "government revenue"]),
        "Revenue_per_capita": ("revenue_per_capita", "Revenue per capita", "dollars per person", ["revenue per capita", "revenue per person", "p revenue"]),
        "Expenses": ("expenses", "Expenses", "dollars", ["expense", "expenses", "expenditure", "expenditures", "spending by government"]),
        "Expenses_per_capita": ("expenses_per_capita", "Expenses per capita", "dollars per person", ["expense per capita", "expenses per capita", "expenditures per capita", "p expense"]),
        "Debt_Ratio": ("debt_ratio", "Debt ratio", "ratio", ["debt ratio", "debt burden", "debt level", "debt"]),
        "Current_Ratio": ("current_ratio", "Current ratio", "ratio", ["current ratio", "liquidity ratio", "liquidity"]),
        "Free_Cash_Flow": ("free_cash_flow", "Free cash flow", "dollars", ["free cash flow", "cash flow", "fcf"]),
        "Free_Cash_Flow_per_capita": ("free_cash_flow_per_capita", "Free cash flow per capita", "dollars per person", ["free cash flow per capita", "cash flow per capita", "fcf per capita"]),
        "Net_Position": ("net_position", "Net position", "dollars", ["net position", "net assets", "fiscal position"]),
        "Net_Position_per_capita": ("net_position_per_capita", "Net position per capita", "dollars per person", ["net position per capita", "net assets per capita"]),
        "Net_Pension_Liability": ("net_pension_liability", "Net pension liability", "dollars", ["pension liability", "net pension liability", "pension debt"]),
        "Net_Pension_Liability_per_capita": ("net_pension_liability_per_capita", "Net pension liability per capita", "dollars per person", ["pension liability per capita", "pension debt per capita"]),
        "POPULATION": ("population", "Population", "count", ["population"]),
    }
    return {
        id_: _metric(
            id_,
            label,
            quote_identifier(column),
            description=f"Government finance field `{column}`.",
            unit=unit,
            aggregation="avg" if unit in {"ratio"} or "per person" in unit else "sum",
            synonyms=synonyms,
        )
        for column, (id_, label, unit, synonyms) in mapping.items()
        if column in columns
    }


def _finra_metrics(columns: list[str]) -> dict[str, MetricDefinition]:
    mapping = {
        "financial_literacy": ("financial_literacy", "Financial literacy", "score", ["financial literacy", "literacy", "fin lit", "financial knowledge", "money knowledge"]),
        "financial_constraint": ("financial_constraint", "Financial constraint", "score", ["financial constraint", "constraint", "financial stress", "financial hardship", "financially constrained"]),
        "alternative_financing": ("alternative_financing", "Alternative financing", "score", ["alternative financing", "alternative finance", "payday", "nonbank financing"]),
        "satisfied": ("financial_satisfaction", "Financial satisfaction", "score", ["satisfaction", "financial satisfaction", "satisfied", "happy financially"]),
        "risk_averse": ("risk_averse", "Risk aversion", "score", ["risk aversion", "risk averse", "risk avoidant"]),
    }
    return {
        id_: _metric(id_, label, quote_identifier(column), description=f"FINRA field `{column}`.", unit=unit, aggregation="avg", synonyms=synonyms)
        for column, (id_, label, unit, synonyms) in mapping.items()
        if column in columns
    }


def _flow_metrics(columns: list[str]) -> dict[str, MetricDefinition]:
    amount_col = "subaward_amount_year" if "subaward_amount_year" in columns else "subaward_amount" if "subaward_amount" in columns else None
    if not amount_col:
        return {}
    return {
        "subaward_amount": _metric(
            "subaward_amount",
            "Subaward amount",
            quote_identifier(amount_col),
            description="Directional subcontract / subaward funding amount.",
            unit="dollars",
            aggregation="sum",
            synonyms=[
                "subaward", "subawards", "subcontract", "subcontracts", "flow", "flows",
                "inflow", "outflow", "fund flow", "fund flows", "subcontract inflow",
                "subcontract outflow", "prime awardee", "subawardee", "sent to", "received from",
            ],
            default_for=["subaward", "subcontract", "fund flow", "flow", "inflow", "outflow"],
        )
    }


def _default_metric_variant(metric: MetricDefinition) -> str:
    unit = metric.unit.lower()
    if "per 1,000" in unit or "per 1000" in unit:
        return "per_1000"
    if "per person" in unit or "per capita" in unit or "per resident" in unit:
        return "per_capita"
    if unit == "percent":
        return "share"
    if unit == "ratio":
        return "ratio"
    if unit == "count":
        return "count"
    if unit == "dollars":
        return "amount"
    return "value"


def _link_metric_variants(metrics: dict[str, MetricDefinition]) -> None:
    for metric in metrics.values():
        if metric.semantic_concept is None:
            metric.semantic_concept = metric.id
        if metric.semantic_variant is None:
            metric.semantic_variant = _default_metric_variant(metric)

    suffix_variants = {
        "_per_capita": "per_capita",
        "_per_1000": "per_1000",
    }
    for metric_id, metric in metrics.items():
        for suffix, variant in suffix_variants.items():
            if not metric_id.endswith(suffix):
                continue
            base_id = metric_id[: -len(suffix)]
            base_metric = metrics.get(base_id)
            if not base_metric:
                continue
            metric.semantic_concept = base_metric.semantic_concept or base_id
            metric.semantic_variant = variant
            base_metric.semantic_concept = metric.semantic_concept
            if base_metric.semantic_variant is None or base_metric.semantic_variant == "value":
                base_metric.semantic_variant = _default_metric_variant(base_metric)

    concept_groups: dict[str, dict[str, str]] = {}
    for metric in metrics.values():
        if not metric.semantic_concept or not metric.semantic_variant:
            continue
        concept_groups.setdefault(metric.semantic_concept, {})[metric.semantic_variant] = metric.id

    for metric in metrics.values():
        if metric.semantic_concept:
            metric.related_variants = dict(concept_groups.get(metric.semantic_concept, {}))


def _decorate_dataset(dataset: DatasetDefinition) -> DatasetDefinition:
    columns = dataset.columns
    table = dataset.table_name
    if table.startswith("contract_") or table.startswith("spending_"):
        dataset.metrics.update(_contract_metrics(columns))
        dataset.caveats.append("Broad funding defaults to Contracts + Grants + Resident Wage + Direct Payments where those fields are available.")
        dataset.caveats.append("Funding tables expose 2024 and 2020-2024 aggregate rows; the aggregate row is not an annual trend point.")
        if table.startswith("spending_") and "agency" in columns:
            dataset.example_questions.extend([
                "Which agencies provide the most grants to Maryland?",
                "Break down federal funding in Maryland by agency.",
            ])
    elif table.startswith("acs_"):
        dataset.metrics.update(_acs_metrics(columns))
        dataset.caveats.append("ACS percentage/share fields are treated as native processed metrics.")
    elif table.startswith("gov_"):
        dataset.metrics.update(_gov_metrics(columns))
        dataset.caveats.append("Government finance data is a Fiscal Year 2023 slice; do not infer trends from it.")
    elif table.startswith("finra_"):
        dataset.metrics.update(_finra_metrics(columns))
        dataset.caveats.append("County and congressional FINRA data is currently 2021 only.")
    elif table.endswith("_flow"):
        dataset.metrics.update(_flow_metrics(columns))
        dataset.caveats.append("Fund flow means directional subcontract movement, not total federal spending received by a geography.")
        dataset.example_questions.extend([
            "How much subcontract inflow goes to Maryland?",
            "Top states sending subcontract flow to Maryland.",
        ])
    _link_metric_variants(dataset.metrics)
    return dataset


@lru_cache(maxsize=1)
def load_registry() -> SemanticRegistrySnapshot:
    manifest = _load_json(MANIFEST_PATH)
    metadata = _load_json(METADATA_PATH)
    datasets = {
        table_name: _decorate_dataset(_dataset_base(table_name, manifest, metadata))
        for table_name in manifest
    }
    return SemanticRegistrySnapshot(version=REGISTRY_VERSION, datasets=datasets)


def get_dataset(dataset_id: str) -> DatasetDefinition | None:
    return load_registry().datasets.get(dataset_id)


def all_allowed_views() -> set[str]:
    return {dataset.view_name for dataset in load_registry().datasets.values()}
