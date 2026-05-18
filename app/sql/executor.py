from __future__ import annotations

import time
from typing import Any

from app.duckdb.connection import execute_select


def execute_sql_bundle(sql_items: list[dict[str, str]], *, max_rows: int = 250) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in sql_items:
        started = time.perf_counter()
        rows = execute_select(item["sql"], max_rows=max_rows)
        results.append(
            {
                "name": item["name"],
                "sql": item["sql"],
                "status": "success",
                "row_count": len(rows),
                "execution_ms": int((time.perf_counter() - started) * 1000),
                "rows": rows,
            }
        )
    return results
