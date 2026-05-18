"""Independent ground-truth engine for the golden suite.

Deliberately self-contained: its own in-memory DuckDB over the same parquet
files, so a bug in the app's execution layer cannot mask itself. Shared by the
pytest suite (tests/ground_truth.py re-exports this) and `run_evals`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

import duckdb
import yaml

from app.paths import MANIFEST_PATH, ROOT_DIR

GOLDEN_PATH = ROOT_DIR / "app" / "evals" / "golden_questions.yaml"


@lru_cache(maxsize=1)
def _conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    manifest = json.loads(MANIFEST_PATH.read_text())
    for table, info in manifest.items():
        path = str((ROOT_DIR / info["path"]).resolve()).replace("'", "''")
        conn.execute(f"CREATE OR REPLACE VIEW mart_{table} AS SELECT * FROM read_parquet('{path}')")
    return conn


def run_reference_sql(sql: str) -> list[dict[str, Any]]:
    df = _conn().execute(sql).df()
    return df.astype(object).where(df.notna(), None).to_dict(orient="records")


def reference_scalar(sql: str) -> Any:
    rows = run_reference_sql(sql)
    return None if not rows else next(iter(rows[0].values()))


@dataclass
class GoldenCase:
    id: str
    question: str
    intent: str
    tables: list[str] = field(default_factory=list)
    must_columns: list[str] = field(default_factory=list)
    no_sql: bool = False
    reference_sql: str | None = None
    expect: dict[str, Any] = field(default_factory=dict)
    faithfulness: bool = False
    note: str = ""

    @property
    def analytical(self) -> bool:
        return self.intent == "ANALYTICAL"


HOLDOUT_PATH = ROOT_DIR / "app" / "evals" / "holdout_questions.yaml"


def load_cases(path: Any) -> list[GoldenCase]:
    raw = yaml.safe_load(open(path).read()) or []
    return [
        GoldenCase(
            id=item["id"],
            question=item["question"],
            intent=item["intent"],
            tables=list(item.get("tables", []) or []),
            must_columns=list(item.get("must_columns", []) or []),
            no_sql=bool(item.get("no_sql", False)),
            reference_sql=item.get("reference_sql"),
            expect=dict(item.get("expect", {}) or {}),
            faithfulness=bool(item.get("faithfulness", False)),
            note=item.get("note", ""),
        )
        for item in raw
    ]


@lru_cache(maxsize=1)
def load_holdout() -> list[GoldenCase]:
    return load_cases(HOLDOUT_PATH)


@lru_cache(maxsize=1)
def load_golden() -> list[GoldenCase]:
    raw = yaml.safe_load(GOLDEN_PATH.read_text()) or []
    return [
        GoldenCase(
            id=item["id"],
            question=item["question"],
            intent=item["intent"],
            tables=list(item.get("tables", []) or []),
            must_columns=list(item.get("must_columns", []) or []),
            no_sql=bool(item.get("no_sql", False)),
            reference_sql=item.get("reference_sql"),
            expect=dict(item.get("expect", {}) or {}),
            faithfulness=bool(item.get("faithfulness", False)),
            note=item.get("note", ""),
        )
        for item in raw
    ]


def cases_by_intent(intent: str) -> list[GoldenCase]:
    return [c for c in load_golden() if c.intent == intent]
