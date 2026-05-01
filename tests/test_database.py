from __future__ import annotations

import os
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
TEST_DB = ROOT / "data" / "runtime" / "test_storage.sqlite3"
os.environ["SQLITE_DB_PATH"] = str(TEST_DB)

from app.api.auth import LoginRequest, RegisterRequest, authenticate_user, register_user  # noqa: E402
from app.api.threads import create_message, create_thread, format_message, list_messages  # noqa: E402
from app.storage.sqlite import init_storage  # noqa: E402


class StorageTests(unittest.TestCase):
    def setUp(self) -> None:
        if TEST_DB.exists():
            TEST_DB.unlink()
        init_storage()

    def test_user_and_thread_storage(self) -> None:
        user = register_user(RegisterRequest(name="Alice", email="alice@example.com", password="secret123"))
        authed = authenticate_user(LoginRequest(email="alice@example.com", password="secret123"))
        self.assertEqual(authed["id"], user["id"])

        thread = create_thread(user["id"], "contract_county", "Funding")
        create_message(thread["id"], "assistant", "Answer", {"resolution": "answered", "rowCount": 1})
        messages = list_messages(thread["id"])
        self.assertEqual(len(messages), 1)
        formatted = format_message(messages[0])
        self.assertEqual(formatted["resolution"], "answered")
        self.assertEqual(formatted["rowCount"], 1)


if __name__ == "__main__":
    unittest.main()
