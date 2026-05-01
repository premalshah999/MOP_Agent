from __future__ import annotations

import json
import os
from pathlib import Path
from threading import Lock
from typing import Any

import duckdb

from app.paths import MANIFEST_PATH, RUNTIME_DIR, ROOT_DIR
from app.semantic.registry import mart_view_name


DB_PATH = Path(os.getenv("DUCKDB_PATH", str(RUNTIME_DIR / "mop.duckdb"))).expanduser().resolve()
_INIT_LOCK = Lock()
_INITIALIZED = False


def _connect(*, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def initialize_duckdb() -> dict[str, Any]:
    global _INITIALIZED
    with _INIT_LOCK:
        if _INITIALIZED:
            return {"initialized": True, "db_path": str(DB_PATH)}
        manifest = json.load(MANIFEST_PATH.open())
        with _connect(read_only=False) as conn:
            conn.execute("PRAGMA threads=4")
            for table_name, info in manifest.items():
                parquet_path = (ROOT_DIR / info["path"]).resolve()
                escaped_path = str(parquet_path).replace("'", "''")
                conn.execute(
                    f"CREATE OR REPLACE VIEW {mart_view_name(table_name)} AS "
                    f"SELECT * FROM read_parquet('{escaped_path}')"
                )
        _INITIALIZED = True
        return {"initialized": True, "db_path": str(DB_PATH), "registered_view_count": len(manifest)}


def list_registered_views() -> list[str]:
    initialize_duckdb()
    with _connect(read_only=True) as conn:
        return [row[0] for row in conn.execute("SHOW TABLES").fetchall()]


def execute_select(sql: str, *, max_rows: int = 250) -> list[dict[str, Any]]:
    initialize_duckdb()
    wrapped = f"SELECT * FROM ({sql.rstrip(';')}) AS limited_result LIMIT {max_rows}"
    with _connect(read_only=True) as conn:
        df = conn.execute(wrapped).df()
    cleaned = df.astype(object).where(df.notna(), None)
    return cleaned.to_dict(orient="records")
