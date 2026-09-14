from __future__ import annotations

import json
import math
import os
from pathlib import Path
from threading import Event, Lock, Timer
from typing import Any

import duckdb

from app.paths import MANIFEST_PATH, ROOT_DIR, RUNTIME_DIR
from app.semantic.registry import mart_view_name

DB_PATH = Path(os.getenv("DUCKDB_PATH", str(RUNTIME_DIR / "mop.duckdb"))).expanduser().resolve()
_INIT_LOCK = Lock()
_INITIALIZED = False


class QueryTimeoutError(RuntimeError):
    """Raised when DuckDB exceeds the configured execution deadline."""


def _query_timeout_seconds() -> float:
    try:
        return max(0.0, float(os.getenv("QUERY_TIMEOUT_SECONDS", "15")))
    except ValueError:
        return 15.0


def _connect(*, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def initialize_duckdb() -> dict[str, Any]:
    global _INITIALIZED
    with _INIT_LOCK:
        if _INITIALIZED:
            return {"initialized": True, "db_path": str(DB_PATH)}
        manifest = json.load(MANIFEST_PATH.open())
        # Fast path for multi-worker startup: if another process already
        # created every view, verify via a read-only connection and skip the
        # exclusive write lock entirely.
        try:
            with _connect(read_only=True) as conn:
                existing = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
                schemas_match = all(
                    mart_view_name(table) in existing
                    and [
                        row[0]
                        for row in conn.execute(f'DESCRIBE "{mart_view_name(table)}"').fetchall()
                    ]
                    == list(info.get("columns", []))
                    for table, info in manifest.items()
                )
            if schemas_match:
                _INITIALIZED = True
                return {
                    "initialized": True,
                    "db_path": str(DB_PATH),
                    "registered_view_count": len(manifest),
                }
        except Exception:
            pass  # db file missing or writer holds the lock — take the write path
        with _connect(read_only=False) as conn:
            conn.execute("PRAGMA threads=4")
            for table_name, info in manifest.items():
                parquet_path = (ROOT_DIR / info["path"]).resolve()
                escaped_path = str(parquet_path).replace("'", "''")
                projection = ", ".join(
                    f'"{str(column).replace(chr(34), chr(34) * 2)}"'
                    for column in info.get("columns", [])
                )
                conn.execute(
                    f"CREATE OR REPLACE VIEW {mart_view_name(table_name)} AS "
                    f"SELECT {projection} FROM read_parquet('{escaped_path}')"
                )
        _INITIALIZED = True
        return {
            "initialized": True,
            "db_path": str(DB_PATH),
            "registered_view_count": len(manifest),
        }


def list_registered_views() -> list[str]:
    initialize_duckdb()
    with _connect(read_only=True) as conn:
        return [row[0] for row in conn.execute("SHOW TABLES").fetchall()]


def execute_select(sql: str, *, max_rows: int = 250) -> list[dict[str, Any]]:
    initialize_duckdb()
    safe_max_rows = max(1, min(int(max_rows), 5_000))
    statement = sql.strip().rstrip(";").strip()
    wrapped = f"SELECT * FROM ({statement}) AS limited_result LIMIT {safe_max_rows}"
    with _connect(read_only=True) as conn:
        timeout = _query_timeout_seconds()
        timed_out = Event()

        def interrupt() -> None:
            timed_out.set()
            try:
                conn.interrupt()
            except Exception:
                # The query may have completed between the timer firing and
                # the interrupt reaching DuckDB.
                return

        timer = Timer(timeout, interrupt) if timeout > 0 else None
        if timer:
            timer.daemon = True
            timer.start()
        try:
            cursor = conn.execute(wrapped)
            columns = [description[0] for description in cursor.description]
            values = cursor.fetchall()
        except duckdb.InterruptException as exc:
            if timed_out.is_set():
                raise QueryTimeoutError(
                    f"Query exceeded the {timeout:g}-second execution limit"
                ) from exc
            raise
        finally:
            if timer:
                timer.cancel()

    def clean(value: Any) -> Any:
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value

    return [
        {column: clean(value) for column, value in zip(columns, row, strict=True)} for row in values
    ]
