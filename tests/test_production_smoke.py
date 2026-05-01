from __future__ import annotations

import os
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
os.environ.setdefault("SQLITE_DB_PATH", str(ROOT / "data" / "runtime" / "test_smoke.sqlite3"))
os.environ.setdefault("DUCKDB_PATH", str(ROOT / "data" / "runtime" / "test_smoke.duckdb"))


class ProductionSmokeTests(unittest.TestCase):
    def test_application_imports_and_pipeline_has_version(self) -> None:
        from app.core.orchestrator import PIPELINE_VERSION
        from app.main import app

        self.assertEqual(PIPELINE_VERSION, "controlled-analytics-v2")
        self.assertEqual(app.title, "MOP Controlled Analytics Assistant")


if __name__ == "__main__":
    unittest.main()
