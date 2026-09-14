from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
os.environ["SQLITE_DB_PATH"] = str(ROOT / "data" / "runtime" / "test_http.sqlite3")
os.environ["DUCKDB_PATH"] = str(ROOT / "data" / "runtime" / "test_http.duckdb")
os.environ["JWT_SECRET"] = "test-secret-that-is-at-least-32-bytes"

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
            self.assertEqual(health.headers["cache-control"], "no-store")
            self.assertIn("default-src 'self'", health.headers["content-security-policy"])

            invalid_request_id = client.get("/health", headers={"X-Request-ID": "bad id"})
            self.assertNotEqual(invalid_request_id.headers["x-request-id"], "bad id")
            valid_request_id = client.get("/health", headers={"X-Request-ID": "trace-123"})
            self.assertEqual(valid_request_id.headers["x-request-id"], "trace-123")

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
                self.assertEqual(shared_page.headers["cache-control"], "no-cache")

                asset = next((ROOT / "frontend" / "dist" / "assets").iterdir())
                asset_response = client.get(f"/assets/{asset.name}")
                self.assertEqual(asset_response.status_code, 200)
                self.assertIn("immutable", asset_response.headers["cache-control"])

            boundaries = client.get("/geo/states.geojson")
            self.assertEqual(boundaries.status_code, 200)
            self.assertEqual(boundaries.headers["cache-control"], "public, max-age=86400")

    def test_request_limits_and_deleted_user_tokens(self) -> None:
        from app.storage.sqlite import connect

        with TestClient(app) as client:
            headers = _auth(client)
            oversized = client.post("/api/ask", json={"question": "x" * 8_001}, headers=headers)
            self.assertEqual(oversized.status_code, 422)

            with connect() as connection:
                token = headers["Authorization"].split(" ", 1)[1]
                import jwt

                subject = int(
                    jwt.decode(token, os.environ["JWT_SECRET"], algorithms=["HS256"])["sub"]
                )
                connection.execute("DELETE FROM users WHERE id = ?", (subject,))
                connection.commit()

            self.assertEqual(client.get("/api/auth/me", headers=headers).status_code, 401)

    def test_stream_errors_are_redacted_in_production(self) -> None:
        with TestClient(app) as client:
            headers = _auth(client)
            with (
                patch.dict(os.environ, {"DEBUG_ERRORS": "false"}),
                patch("app.main.answer_question", side_effect=RuntimeError("private SQL detail")),
            ):
                response = client.post(
                    "/api/ask/stream", json={"question": "test failure"}, headers=headers
                )
            self.assertEqual(response.status_code, 200)
            self.assertIn("Analysis failed unexpectedly", response.text)
            self.assertNotIn("private SQL detail", response.text)

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
