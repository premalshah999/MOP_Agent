from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

# Use a temp database for tests — unique name to avoid conflicts with other test files
_TEST_DB = ROOT / "data" / "runtime" / "test_http.sqlite3"
os.environ["SQLITE_DB_PATH"] = str(_TEST_DB)
os.environ["JWT_SECRET"] = "test-secret-key-for-testing"

# Clean before import to ensure fresh state
if _TEST_DB.exists():
    _TEST_DB.unlink()

from app.main import app  # noqa: E402


def _register_user(client: TestClient, name: str = "Test", email: str = "test@example.com", password: str = "testpass123") -> dict:
    resp = client.post("/api/auth/register", json={"name": name, "email": email, "password": password})
    if resp.status_code == 409:
        resp = client.post("/api/auth/login", json={"email": email, "password": password})
    return resp.json()


def _auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


class HttpSurfaceTests(unittest.TestCase):
    def test_health_exposes_readiness_and_security_headers(self) -> None:
        with TestClient(app) as client:
            response = client.get("/health")

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "mop-agent")
        self.assertIn("checks", payload)
        self.assertTrue(payload["checks"]["manifest_present"])
        self.assertGreater(payload["checks"]["registered_table_count"], 0)
        self.assertIn("x-request-id", response.headers)
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["cache-control"], "no-store")

    def test_api_ask_requires_auth(self) -> None:
        with TestClient(app) as client:
            response = client.post("/api/ask", json={"question": "test", "history": []})
        self.assertEqual(response.status_code, 401)

    def test_api_ask_returns_502_when_agent_returns_error_payload(self) -> None:
        with patch("app.main.ask_agent", return_value={"error": "LLM upstream failed", "sql": "SELECT 1", "data": [], "row_count": 0, "answer": ""}):
            with patch("app.main.log_query"):
                with TestClient(app) as client:
                    auth = _register_user(client, email="test_502@example.com")
                    headers = _auth_headers(auth["token"])
                    # Create a thread first
                    thread_resp = client.post("/api/threads", json={"dataset_id": "gov"}, headers=headers)
                    thread_id = thread_resp.json()["thread"]["id"]
                    response = client.post("/api/ask", json={"question": "test", "thread_id": thread_id}, headers=headers)

        payload = response.json()
        self.assertEqual(response.status_code, 502)
        self.assertEqual(payload["error"], "LLM upstream failed")
        self.assertEqual(payload["sql"], "SELECT 1")
        self.assertTrue(payload["request_id"])
        self.assertIn("x-request-id", response.headers)

    def test_api_ask_returns_500_for_unhandled_exception(self) -> None:
        with patch("app.main.ask_agent", side_effect=RuntimeError("boom")):
            with patch("app.main.log_query"):
                with TestClient(app) as client:
                    auth = _register_user(client, email="test_500@example.com")
                    headers = _auth_headers(auth["token"])
                    thread_resp = client.post("/api/threads", json={"dataset_id": "gov"}, headers=headers)
                    thread_id = thread_resp.json()["thread"]["id"]
                    response = client.post("/api/ask", json={"question": "test", "thread_id": thread_id}, headers=headers)

        payload = response.json()
        self.assertEqual(response.status_code, 500)
        self.assertEqual(payload["error"], "Internal server error")
        self.assertEqual(payload["row_count"], 0)
        self.assertTrue(payload["request_id"])

    def test_geo_boundaries_are_served(self) -> None:
        with TestClient(app) as client:
            response = client.get("/geo/states.geojson")

        if response.status_code == 404:
            self.skipTest("Geospatial boundaries not available")
        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["type"], "FeatureCollection")
        self.assertTrue(payload["features"])


class AuthTests(unittest.TestCase):
    def test_register_creates_user_and_returns_token(self) -> None:
        with TestClient(app) as client:
            resp = client.post("/api/auth/register", json={
                "name": "Alice", "email": "alice@example.com", "password": "secret123"
            })
        self.assertEqual(resp.status_code, 201)
        body = resp.json()
        self.assertIn("token", body)
        self.assertEqual(body["user"]["name"], "Alice")
        self.assertEqual(body["user"]["email"], "alice@example.com")

    def test_register_duplicate_email_returns_409(self) -> None:
        with TestClient(app) as client:
            client.post("/api/auth/register", json={
                "name": "Bob", "email": "bob@example.com", "password": "secret123"
            })
            resp = client.post("/api/auth/register", json={
                "name": "Bob2", "email": "bob@example.com", "password": "secret456"
            })
        self.assertEqual(resp.status_code, 409)

    def test_login_with_valid_credentials(self) -> None:
        with TestClient(app) as client:
            client.post("/api/auth/register", json={
                "name": "Carol", "email": "carol@example.com", "password": "secret123"
            })
            resp = client.post("/api/auth/login", json={
                "email": "carol@example.com", "password": "secret123"
            })
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertIn("token", body)
        self.assertEqual(body["user"]["name"], "Carol")

    def test_login_with_wrong_password_returns_401(self) -> None:
        with TestClient(app) as client:
            client.post("/api/auth/register", json={
                "name": "Dave", "email": "dave@example.com", "password": "secret123"
            })
            resp = client.post("/api/auth/login", json={
                "email": "dave@example.com", "password": "wrong"
            })
        self.assertEqual(resp.status_code, 401)

    def test_me_returns_user_profile(self) -> None:
        with TestClient(app) as client:
            reg = client.post("/api/auth/register", json={
                "name": "Eve", "email": "eve@example.com", "password": "secret123"
            }).json()
            resp = client.get("/api/auth/me", headers=_auth_headers(reg["token"]))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["user"]["name"], "Eve")

    def test_me_without_token_returns_401(self) -> None:
        with TestClient(app) as client:
            resp = client.get("/api/auth/me")
        self.assertEqual(resp.status_code, 401)

    def test_register_validates_email(self) -> None:
        with TestClient(app) as client:
            resp = client.post("/api/auth/register", json={
                "name": "X", "email": "not-an-email", "password": "secret123"
            })
        self.assertEqual(resp.status_code, 422)

    def test_register_validates_password_length(self) -> None:
        with TestClient(app) as client:
            resp = client.post("/api/auth/register", json={
                "name": "X", "email": "x@example.com", "password": "short"
            })
        self.assertEqual(resp.status_code, 422)


class ThreadCrudTests(unittest.TestCase):
    def _auth(self, client: TestClient, email: str = "threads@example.com") -> dict:
        return _auth_headers(_register_user(client, email=email)["token"])

    def test_create_and_list_threads(self) -> None:
        with TestClient(app) as client:
            headers = self._auth(client, "threadlist@example.com")
            resp = client.post("/api/threads", json={"dataset_id": "gov", "title": "Test Thread"}, headers=headers)
            self.assertEqual(resp.status_code, 201)
            thread = resp.json()["thread"]
            self.assertEqual(thread["title"], "Test Thread")
            self.assertEqual(thread["datasetId"], "gov")

            resp = client.get("/api/threads", headers=headers)
            self.assertEqual(resp.status_code, 200)
            threads = resp.json()["threads"]
            self.assertGreaterEqual(len(threads), 1)

    def test_get_thread_with_messages(self) -> None:
        with TestClient(app) as client:
            headers = self._auth(client, "threadget@example.com")
            thread = client.post("/api/threads", json={"dataset_id": "gov"}, headers=headers).json()["thread"]
            thread_id = thread["id"]

            resp = client.get(f"/api/threads/{thread_id}", headers=headers)
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["thread"]["id"], thread_id)
            self.assertEqual(resp.json()["thread"]["messages"], [])

    def test_update_thread(self) -> None:
        with TestClient(app) as client:
            headers = self._auth(client, "threadupdate@example.com")
            thread = client.post("/api/threads", json={"dataset_id": "gov"}, headers=headers).json()["thread"]
            thread_id = thread["id"]

            resp = client.put(f"/api/threads/{thread_id}", json={"title": "Updated Title"}, headers=headers)
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["thread"]["title"], "Updated Title")

    def test_delete_thread(self) -> None:
        with TestClient(app) as client:
            headers = self._auth(client, "threaddel@example.com")
            thread = client.post("/api/threads", json={"dataset_id": "gov"}, headers=headers).json()["thread"]
            thread_id = thread["id"]

            resp = client.delete(f"/api/threads/{thread_id}", headers=headers)
            self.assertEqual(resp.status_code, 200)

            resp = client.get(f"/api/threads/{thread_id}", headers=headers)
            self.assertEqual(resp.status_code, 404)

    def test_clear_all_threads(self) -> None:
        with TestClient(app) as client:
            headers = self._auth(client, "threadclear@example.com")
            client.post("/api/threads", json={"dataset_id": "gov"}, headers=headers)
            client.post("/api/threads", json={"dataset_id": "acs"}, headers=headers)

            resp = client.delete("/api/threads", headers=headers)
            self.assertEqual(resp.status_code, 200)

            resp = client.get("/api/threads", headers=headers)
            self.assertEqual(len(resp.json()["threads"]), 0)

    def test_thread_isolation_between_users(self) -> None:
        with TestClient(app) as client:
            h1 = self._auth(client, "user1@example.com")
            h2 = self._auth(client, "user2@example.com")

            thread = client.post("/api/threads", json={"dataset_id": "gov"}, headers=h1).json()["thread"]
            thread_id = thread["id"]

            # User 2 should not see user 1's thread
            resp = client.get(f"/api/threads/{thread_id}", headers=h2)
            self.assertEqual(resp.status_code, 404)

            # User 2 should not be able to delete user 1's thread
            resp = client.delete(f"/api/threads/{thread_id}", headers=h2)
            self.assertEqual(resp.status_code, 404)

    def test_get_thread_messages(self) -> None:
        with TestClient(app) as client:
            headers = self._auth(client, "threadmsg@example.com")
            thread = client.post("/api/threads", json={"dataset_id": "gov"}, headers=headers).json()["thread"]
            thread_id = thread["id"]

            resp = client.get(f"/api/threads/{thread_id}/messages", headers=headers)
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["messages"], [])

    def test_ask_persists_messages_to_thread(self) -> None:
        with patch("app.main.ask_agent", return_value={
            "answer": "Maryland leads.", "sql": "SELECT 1", "data": [{"state": "MD"}], "row_count": 1
        }):
            with TestClient(app) as client:
                headers = self._auth(client, "threadask@example.com")
                thread = client.post("/api/threads", json={"dataset_id": "gov"}, headers=headers).json()["thread"]
                thread_id = thread["id"]

                resp = client.post("/api/ask", json={"question": "Top state?", "thread_id": thread_id}, headers=headers)
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(resp.json()["thread_id"], thread_id)
                self.assertIn("user_message_id", resp.json())
                self.assertIn("assistant_message_id", resp.json())

                # Verify messages persisted
                msgs = client.get(f"/api/threads/{thread_id}/messages", headers=headers).json()["messages"]
                self.assertEqual(len(msgs), 2)
                self.assertEqual(msgs[0]["role"], "user")
                self.assertEqual(msgs[0]["content"], "Top state?")
                self.assertEqual(msgs[1]["role"], "assistant")
                self.assertEqual(msgs[1]["content"], "Maryland leads.")

    def test_ask_auto_creates_thread(self) -> None:
        with patch("app.main.ask_agent", return_value={
            "answer": "Auto thread test.", "sql": None, "data": [], "row_count": 0
        }):
            with TestClient(app) as client:
                headers = self._auth(client, "autothread@example.com")
                resp = client.post("/api/ask", json={"question": "Test question"}, headers=headers)
                self.assertEqual(resp.status_code, 200)
                self.assertIn("thread_id", resp.json())

                # Thread should exist with messages
                thread_id = resp.json()["thread_id"]
                thread = client.get(f"/api/threads/{thread_id}", headers=headers).json()["thread"]
                self.assertEqual(len(thread["messages"]), 2)


if __name__ == "__main__":
    unittest.main()
