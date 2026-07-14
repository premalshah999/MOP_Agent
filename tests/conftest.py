"""Shared pytest setup: load .env (live key), isolate runtime DBs, helpers."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except Exception:
    pass

os.environ.setdefault("SQLITE_DB_PATH", str(ROOT / "data" / "runtime" / "test_pipeline.sqlite3"))
os.environ.setdefault("DUCKDB_PATH", str(ROOT / "data" / "runtime" / "test_pipeline.duckdb"))
os.environ.setdefault("JWT_SECRET", "test-secret")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "live: requires a real LLM API key")


def llm_available() -> bool:
    try:
        from app.llm import client

        return client.is_live()
    except Exception:
        return False
