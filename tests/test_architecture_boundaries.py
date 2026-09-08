"""Keep retired answer paths from drifting back into production."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "app"


def test_runtime_does_not_import_evaluation_corpora() -> None:
    forbidden = ("app.evals", "analyst_queries", "verified_queries")
    production_files = [path for path in APP.rglob("*.py") if "evals" not in path.parts]
    offenders = [
        str(path.relative_to(ROOT))
        for path in production_files
        if any(token in path.read_text(encoding="utf-8") for token in forbidden)
    ]
    assert offenders == []


def test_retired_parallel_planners_stay_removed() -> None:
    retired = [
        APP / "core" / "intent.py",
        APP / "core" / "router.py",
        APP / "core" / "verified_queries.py",
    ]
    assert [str(path.relative_to(ROOT)) for path in retired if path.exists()] == []


def test_analyst_query_corpus_is_well_formed() -> None:
    path = APP / "evals" / "analyst_queries.yaml"
    queries = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(queries, list) and queries
    ids = [item["id"] for item in queries]
    assert len(ids) == len(set(ids))
    assert all(item.get("question") and item.get("sql") for item in queries)
    assert all(item["sql"].lstrip().upper().startswith(("SELECT", "WITH")) for item in queries)
