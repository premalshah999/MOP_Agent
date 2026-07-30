from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException
from fastapi.responses import FileResponse

from app.paths import MANIFEST_PATH, ROOT_DIR
from app.semantic.registry import column_metadata, load_registry, table_metadata


def _human_label(name: str) -> str:
    label = name.replace("_", " ").replace(",", " ").replace("&", "and")
    label = " ".join(label.split())
    if label.casefold() == "subaward amount year":
        return "Subaward amount"
    return label[:1].upper() + label[1:]


def _period_label(dataset: Any) -> str:
    if dataset.id.startswith("gov_"):
        return "FY2023 snapshot"
    if not dataset.year_column:
        return "All available records"
    years = [str(year) for year in dataset.available_years]
    if not years:
        return str(dataset.default_year or "Available period")
    if len(years) > 6 and years[0].isdigit() and years[-1].isdigit():
        return f"{years[0]}–{years[-1]}"
    return ", ".join(years)


def _example_question(dataset: Any, label: str, role: str) -> str:
    if role != "measure":
        return f"What can I analyze by {label.lower()} in the {dataset.display_name} data?"
    year = dataset.default_year
    period = f" in {year}" if year not in (None, "") else ""
    if dataset.id.startswith(("contract_", "spending_")):
        return f"Which states received the most {label.lower()}{period}?"
    if dataset.id.startswith("gov_"):
        return f"Which states have the highest {label.lower()}?"
    if dataset.id.startswith("finra_"):
        return f"Which states rank highest on {label.lower()}{period}?"
    if dataset.id.endswith("_flow"):
        return "Which states receive the most federal subaward funding?"
    return f"Compare states by {label.lower()}{period}."


def _variables(dataset: Any) -> list[dict[str, Any]]:
    metadata = column_metadata(dataset.id)
    variables: list[dict[str, Any]] = []
    for name in dataset.columns:
        if name == "Unnamed: 0":
            continue
        metric = dataset.metrics.get(name)
        dimension = dataset.dimensions.get(name)
        role = "measure" if metric else "dimension"
        meta = metadata.get(name, {})
        label = _human_label(metric.label if metric else (dimension.label if dimension else name))
        description = str(
            (metric.description if metric else (dimension.description if dimension else ""))
            or meta.get("description")
            or f"Field in {dataset.display_name}."
        )
        synonyms = list(metric.synonyms if metric else (dimension.synonyms if dimension else []))
        variables.append(
            {
                "name": name,
                "label": label,
                "role": role,
                "description": description,
                "dataType": str(meta.get("type") or ""),
                "unit": str(metric.unit if metric else (meta.get("unit") or "")),
                "aggregation": metric.aggregation if metric else None,
                "synonyms": synonyms,
                "sampleValues": list(meta.get("sample_values") or [])[:6],
                "exampleQuestion": _example_question(dataset, label, role),
            }
        )
    return variables


def dataset_catalog() -> list[dict[str, Any]]:
    manifest = json.load(MANIFEST_PATH.open())
    registry = load_registry()
    families: dict[str, dict[str, Any]] = {}
    for dataset in registry.datasets.values():
        family = dataset.id.split("_", 1)[0]
        entry = families.setdefault(
            family,
            {
                "id": family,
                "name": family.replace("_", " ").title(),
                "description": f"Curated {family} analytical datasets.",
                "helper": "Downloadable curated tables used by the controlled analytics assistant.",
                "notes": [],
                "tables": [],
            },
        )
        info = manifest[dataset.table_name]
        metadata = table_metadata(dataset.id)
        entry["tables"].append(
            {
                "tableName": dataset.table_name,
                "label": dataset.display_name,
                "grain": dataset.grain,
                "summary": dataset.description,
                "rows": info.get("rows", 0),
                "columns": info.get("columns", []),
                "source": metadata.get("source"),
                "geography": dataset.geography,
                "yearColumn": dataset.year_column,
                "defaultYear": dataset.default_year,
                "availableYears": dataset.available_years,
                "periodLabel": _period_label(dataset),
                "variables": _variables(dataset),
                "notes": list(metadata.get("critical_notes") or []),
                "sourceFile": info.get("source_file"),
                "runtimePath": info.get("path"),
                "downloads": {
                    "parquet": f"/api/datasets/download/{dataset.table_name}?format=parquet",
                    "xlsx": f"/api/datasets/download/{dataset.table_name}?format=xlsx" if info.get("source_file") else None,
                },
            }
        )
    return list(families.values())


def download_path(table_name: str, format_: str) -> FileResponse:
    manifest = json.load(MANIFEST_PATH.open())
    info = manifest.get(table_name)
    if not info:
        raise HTTPException(status_code=404, detail="Unknown table")
    if format_ == "parquet":
        path = ROOT_DIR / info["path"]
    elif format_ == "xlsx" and info.get("source_file"):
        path = ROOT_DIR / "data" / "uploads" / info["source_file"]
    else:
        raise HTTPException(status_code=404, detail="Requested format is unavailable")
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=path.name)
