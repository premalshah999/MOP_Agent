from __future__ import annotations

import pytest

from app.duckdb.connection import QueryTimeoutError, execute_select


def test_execute_select_returns_native_rows_and_enforces_limit() -> None:
    rows = execute_select("SELECT value FROM range(3) AS items(value)", max_rows=2)
    assert rows == [{"value": 0}, {"value": 1}]


def test_execute_select_replaces_non_finite_numbers() -> None:
    rows = execute_select(
        "SELECT CAST('NaN' AS DOUBLE) AS nan_value, "
        "CAST('Infinity' AS DOUBLE) AS infinity_value, 1.5 AS finite_value"
    )
    assert rows == [{"nan_value": None, "infinity_value": None, "finite_value": 1.5}]


def test_execute_select_enforces_deadline_and_recovers(monkeypatch) -> None:
    monkeypatch.setenv("QUERY_TIMEOUT_SECONDS", "0.02")
    with pytest.raises(QueryTimeoutError, match="execution limit"):
        execute_select("SELECT sum(value) FROM range(1000000000000) AS items(value)")

    monkeypatch.setenv("QUERY_TIMEOUT_SECONDS", "1")
    assert execute_select("SELECT 1 AS value") == [{"value": 1}]
