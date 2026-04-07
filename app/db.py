from __future__ import annotations

import json
import os
import re
from pathlib import Path
from threading import Lock
from typing import Any

import duckdb


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = ROOT_DIR / "data" / "runtime" / "mop.duckdb"
DB_PATH = Path(os.getenv("DUCKDB_PATH", str(DEFAULT_DB_PATH))).expanduser().resolve()
_VALID_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_INIT_LOCK = Lock()
_INITIALIZED = False


def _load_manifest(manifest_path: str = "data/schema/manifest.json") -> dict[str, Any]:
    path = Path(manifest_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Manifest not found: {manifest_path}. Run scripts/convert_excel.py first."
        )
    with path.open() as f:
        return json.load(f)


def _connect(*, read_only: bool) -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def register_all_tables(manifest_path: str = "data/schema/manifest.json") -> dict[str, Any]:
    global _INITIALIZED

    manifest = _load_manifest(manifest_path)

    with _INIT_LOCK:
        with _connect(read_only=False) as conn:
            conn.execute("PRAGMA threads=4")
            for name, info in manifest.items():
                if not _VALID_TABLE_NAME.match(name):
                    raise ValueError(f"Unsafe table name: {name}")

                parquet_path = Path(info["path"]).resolve()
                escaped_path = str(parquet_path).replace("'", "''")
                conn.execute(
                    f"CREATE OR REPLACE VIEW {name} AS "
                    f"SELECT * FROM read_parquet('{escaped_path}')"
                )
                print(f"  Registered: {name}")
        _INITIALIZED = True

    return manifest


def _ensure_initialized() -> None:
    if _INITIALIZED:
        return
    register_all_tables()


def get_registered_tables() -> list[str]:
    _ensure_initialized()
    with _connect(read_only=True) as conn:
        rows = conn.execute("SHOW TABLES").fetchall()
    return [r[0] for r in rows]


def execute_query(sql: str):
    _ensure_initialized()
    with _connect(read_only=True) as conn:
        conn.execute("PRAGMA threads=4")
        return conn.execute(sql).df()
