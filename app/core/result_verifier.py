from __future__ import annotations

from typing import Any


def verify_results(executions: list[dict[str, Any]]) -> dict[str, Any]:
    warnings: list[str] = []
    if not executions:
        warnings.append("No SQL executions were produced.")
    for execution in executions:
        if execution.get("status") != "success":
            warnings.append(f"Query `{execution.get('name')}` did not succeed.")
        if execution.get("row_count", 0) == 0:
            warnings.append(f"Query `{execution.get('name')}` returned no rows.")
        rows = execution.get("rows") or []
        if rows and all(row.get("metric_value") is None for row in rows if isinstance(row, dict)):
            warnings.append(f"Query `{execution.get('name')}` returned only null metric values.")
    return {"status": "ok" if not warnings else "warning", "warnings": warnings}
