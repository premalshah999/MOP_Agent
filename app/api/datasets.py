from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from fastapi.responses import FileResponse

from app.paths import MANIFEST_PATH, ROOT_DIR
from app.semantic.registry import load_registry


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
        entry["tables"].append(
            {
                "tableName": dataset.table_name,
                "label": dataset.display_name,
                "grain": dataset.grain,
                "summary": dataset.description,
                "rows": info.get("rows", 0),
                "columns": info.get("columns", []),
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
