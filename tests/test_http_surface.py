from __future__ import annotations

import os
import unittest
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
os.environ["SQLITE_DB_PATH"] = str(ROOT / "data" / "runtime" / "test_http.sqlite3")
os.environ["DUCKDB_PATH"] = str(ROOT / "data" / "runtime" / "test_http.duckdb")
os.environ["JWT_SECRET"] = "test-secret"

from app.main import app  # noqa: E402


def _auth(client: TestClient) -> dict[str, str]:
    email = f"tester-{uuid4().hex}@example.com"
    response = client.post(
        "/api/auth/register",
        json={"name": "Tester", "email": email, "password": "secret123"},
    )
    token = response.json()["token"]
    return {"Authorization": f"Bearer {token}"}


class HttpSurfaceTests(unittest.TestCase):
    def test_health_and_dataset_catalog(self) -> None:
        with TestClient(app) as client:
            health = client.get("/health")
            self.assertEqual(health.status_code, 200)
            payload = health.json()
            self.assertEqual(payload["status"], "ok")
            self.assertTrue(payload["checks"]["pipeline_ready"])

            catalog = client.get("/api/datasets")
            self.assertEqual(catalog.status_code, 200)
            datasets = catalog.json()["datasets"]
            self.assertTrue(datasets)
            tables = [table for family in datasets for table in family["tables"]]
            self.assertEqual(len(tables), 17)
            self.assertTrue(
                all(str(table.get("sourceUrl", "")).startswith("https://") for table in tables)
            )
            self.assertTrue(all(table.get("variables") for table in tables))
            flow = next(table for table in tables if table["tableName"] == "state_flow")
            self.assertNotIn("Unnamed: 0", flow["columns"])

            parquet = client.get("/api/datasets/download/state_flow?format=parquet")
            self.assertEqual(parquet.status_code, 200)
            self.assertEqual(parquet.headers["content-type"], "application/vnd.apache.parquet")

            if (ROOT / "frontend" / "dist" / "index.html").exists():
                shared_page = client.get("/share/direct-link-token")
                self.assertEqual(shared_page.status_code, 200)
                self.assertIn("text/html", shared_page.headers["content-type"])

    def test_auth_and_ask_contract_shape(self) -> None:
        """The /api/ask contract shape must stay stable across the rebuild."""
        from app.core.pipeline import PIPELINE_READY

        with TestClient(app) as client:
            headers = _auth(client)
            response = client.post(
                "/api/ask",
                json={"question": "top 10 counties in maryland by grants"},
                headers=headers,
            )
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            for key in (
                "answer",
                "sql",
                "data",
                "row_count",
                "resolution",
                "contract",
                "pipelineTrace",
                "quality",
                "thread_id",
            ):
                self.assertIn(key, payload)
            self.assertIn("tables", payload["contract"])
            from app.llm import client as llm_client

            if PIPELINE_READY and llm_client.is_live():
                self.assertEqual(payload["resolution"], "answered")
                self.assertTrue(payload["sql"])
                self.assertEqual(payload["row_count"], 10)


if __name__ == "__main__":
    unittest.main()
