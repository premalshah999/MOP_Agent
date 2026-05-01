from __future__ import annotations

import os
import unittest
from pathlib import Path

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
os.environ["SQLITE_DB_PATH"] = str(ROOT / "data" / "runtime" / "test_http.sqlite3")
os.environ["DUCKDB_PATH"] = str(ROOT / "data" / "runtime" / "test_http.duckdb")
os.environ["JWT_SECRET"] = "test-secret"

from app.main import app  # noqa: E402


def _auth(client: TestClient) -> dict[str, str]:
    response = client.post(
        "/api/auth/register",
        json={"name": "Tester", "email": "tester@example.com", "password": "secret123"},
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
            self.assertTrue(catalog.json()["datasets"])

    def test_auth_and_ask(self) -> None:
        with TestClient(app) as client:
            headers = _auth(client)
            response = client.post(
                "/api/ask",
                json={"question": "top 10 counties in maryland with maximum funding"},
                headers=headers,
            )
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["resolution"], "answered")
            self.assertEqual(payload["contract"]["metric"], "total_federal_funding")
            self.assertEqual(payload["row_count"], 10)
            self.assertIn("thread_id", payload)


if __name__ == "__main__":
    unittest.main()
