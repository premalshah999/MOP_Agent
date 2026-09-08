from __future__ import annotations

from app.evals import reference


class _FakeCursor:
    description = [("label",), ("value",)]

    def execute(self, sql: str) -> _FakeCursor:
        assert sql == "SELECT test"
        return self

    @staticmethod
    def fetchall() -> list[tuple[object, object]]:
        return [
            ("finite", 4.5),
            ("nan", float("nan")),
            ("infinity", float("inf")),
        ]


def test_reference_executor_uses_native_rows_and_cleans_nonfinite(monkeypatch) -> None:
    monkeypatch.setattr(reference, "_conn", lambda: _FakeCursor())

    assert reference.run_reference_sql("SELECT test") == [
        {"label": "finite", "value": 4.5},
        {"label": "nan", "value": None},
        {"label": "infinity", "value": None},
    ]
