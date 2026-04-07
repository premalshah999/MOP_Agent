from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
PORT = 8011
BASE_URL = f"http://127.0.0.1:{PORT}"

load_dotenv(ROOT / ".env")


def _http_get(path: str) -> tuple[int, str, dict[str, str]]:
    request = urllib.request.Request(f"{BASE_URL}{path}", method="GET")
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.status, response.read().decode("utf-8"), dict(response.headers.items())


def _http_post_json(path: str, payload: dict[str, object], token: str | None = None) -> tuple[int, dict[str, object], dict[str, str]]:
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=body,
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=180) as response:
        return response.status, json.loads(response.read().decode("utf-8")), dict(response.headers.items())


def _get_auth_token() -> str:
    """Register or login a test user and return a JWT token."""
    try:
        _, data, _ = _http_post_json("/api/auth/register", {
            "name": "Smoke Test", "email": "smoke@test.com", "password": "smoketest123"
        })
        return data["token"]
    except urllib.error.HTTPError:
        _, data, _ = _http_post_json("/api/auth/login", {
            "email": "smoke@test.com", "password": "smoketest123"
        })
        return data["token"]


def _port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex(("127.0.0.1", port)) == 0


class ProductionSmokeTests(unittest.TestCase):
    server: subprocess.Popen[str] | None = None

    @classmethod
    def setUpClass(cls) -> None:
        os.chdir(ROOT)
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT)
        cls.server = subprocess.Popen(
            [
                sys.executable,
                "scripts/run_local_prod.py",
                "--host",
                "127.0.0.1",
                "--port",
                str(PORT),
            ],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        deadline = time.time() + 60
        last_error = "server did not start"
        while time.time() < deadline:
            if cls.server.poll() is not None:
                output = cls.server.stdout.read() if cls.server.stdout else ""
                raise AssertionError(f"Production server exited early:\n{output}")
            if _port_open(PORT):
                try:
                    status, body, _headers = _http_get("/health")
                    if status == 200 and json.loads(body)["status"] == "ok":
                        return
                except Exception as exc:  # noqa: BLE001
                    last_error = str(exc)
            time.sleep(1)

        raise AssertionError(f"Timed out waiting for production server: {last_error}")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.server and cls.server.poll() is None:
            cls.server.terminate()
            try:
                cls.server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                cls.server.kill()
                cls.server.wait(timeout=10)

    def test_health_endpoint(self) -> None:
        status, body, headers = _http_get("/health")
        payload = json.loads(body)
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "mop-agent")
        self.assertIn("checks", payload)
        self.assertGreater(payload["checks"]["registered_table_count"], 0)
        self.assertIn("x-request-id", headers)
        self.assertEqual(headers["x-content-type-options"], "nosniff")

    def test_root_serves_built_frontend(self) -> None:
        status, body, headers = _http_get("/")
        self.assertEqual(status, 200)
        self.assertIn("<title>Maryland Opportunity</title>", body)
        self.assertIn("/assets/", body)
        self.assertIn("content-security-policy", headers)

    def test_api_ask_smoke(self) -> None:
        if not os.getenv("DEEPSEEK_API_KEY"):
            self.skipTest("DEEPSEEK_API_KEY not set")

        token = _get_auth_token()
        status, payload, headers = _http_post_json(
            "/api/ask",
            {
                "question": "Which 5 states have the highest total liabilities per capita?",
                "history": [],
            },
            token=token,
        )
        answer = str(payload.get("answer") or "")
        sql = str(payload.get("sql") or "")
        row_count = int(payload.get("row_count") or 0)

        self.assertEqual(status, 200)
        self.assertFalse(payload.get("error"))
        self.assertTrue(answer.strip())
        self.assertTrue(sql.strip().upper().startswith("SELECT") or sql.strip().upper().startswith("WITH"))
        self.assertGreaterEqual(row_count, 5)
        self.assertGreaterEqual(len(answer.split()), 90)
        self.assertTrue(str(payload.get("request_id") or "").strip())
        self.assertIn("x-request-id", headers)
        self.assertEqual(headers["cache-control"], "no-store")


if __name__ == "__main__":
    unittest.main()
