"""Unit tests for the centralized SQLite database module."""
from __future__ import annotations

import os
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

# Use an isolated test database
TEST_DB = ROOT / "data" / "runtime" / "test_db_unit.db"
os.environ["SQLITE_DB_PATH"] = str(TEST_DB)

from app import database as db  # noqa: E402
from app.auth import hash_password  # noqa: E402


class DatabaseSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if TEST_DB.exists():
            TEST_DB.unlink()
        db.init_db()

    @classmethod
    def tearDownClass(cls):
        if TEST_DB.exists():
            TEST_DB.unlink()

    def test_schema_version_is_set(self):
        conn = db.get_connection()
        row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
        self.assertEqual(row[0], db.SCHEMA_VERSION)

    def test_tables_exist(self):
        conn = db.get_connection()
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        self.assertIn("users", tables)
        self.assertIn("threads", tables)
        self.assertIn("messages", tables)
        self.assertIn("schema_version", tables)

    def test_foreign_keys_enabled(self):
        conn = db.get_connection()
        row = conn.execute("PRAGMA foreign_keys").fetchone()
        self.assertEqual(row[0], 1)


class UserCrudTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if TEST_DB.exists():
            TEST_DB.unlink()
        db.init_db()

    @classmethod
    def tearDownClass(cls):
        if TEST_DB.exists():
            TEST_DB.unlink()

    def test_create_and_get_user(self):
        user = db.create_user("alice@test.com", "Alice", hash_password("pass123"))
        self.assertEqual(user["email"], "alice@test.com")
        self.assertEqual(user["name"], "Alice")
        self.assertIn("id", user)

        fetched = db.get_user_by_email("alice@test.com")
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched["name"], "Alice")

        fetched2 = db.get_user_by_id(user["id"])
        self.assertIsNotNone(fetched2)
        self.assertEqual(fetched2["email"], "alice@test.com")

    def test_get_nonexistent_user_returns_none(self):
        self.assertIsNone(db.get_user_by_email("nobody@test.com"))
        self.assertIsNone(db.get_user_by_id(99999))

    def test_duplicate_email_raises(self):
        db.create_user("dup@test.com", "Dup1", hash_password("pass"))
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            db.create_user("dup@test.com", "Dup2", hash_password("pass"))

    def test_email_case_insensitive(self):
        db.create_user("casetest@test.com", "CaseTest", hash_password("pass"))
        user = db.get_user_by_email("CASETEST@test.com")
        self.assertIsNotNone(user)
        self.assertEqual(user["name"], "CaseTest")


class ThreadCrudTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if TEST_DB.exists():
            TEST_DB.unlink()
        db.init_db()
        cls.user = db.create_user("threaduser@test.com", "ThreadUser", hash_password("pass"))
        cls.user_id = cls.user["id"]

    @classmethod
    def tearDownClass(cls):
        if TEST_DB.exists():
            TEST_DB.unlink()

    def test_create_and_get_thread(self):
        thread = db.create_thread("t1", self.user_id, "gov_state", "Test Thread")
        self.assertEqual(thread["id"], "t1")
        self.assertEqual(thread["title"], "Test Thread")
        self.assertEqual(thread["dataset_id"], "gov_state")

        fetched = db.get_thread("t1", self.user_id)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched["title"], "Test Thread")

    def test_list_threads_ordered_by_updated(self):
        db.create_thread("t_first", self.user_id, "gov", "First")
        db.create_thread("t_second", self.user_id, "acs", "Second")

        threads = db.get_threads_for_user(self.user_id)
        self.assertGreaterEqual(len(threads), 2)
        # Most recent first
        dates = [t["updated_at"] for t in threads]
        self.assertEqual(dates, sorted(dates, reverse=True))

    def test_update_thread(self):
        db.create_thread("t_upd", self.user_id, "gov", "Original")
        updated = db.update_thread("t_upd", self.user_id, title="Updated")
        self.assertEqual(updated["title"], "Updated")

    def test_delete_thread(self):
        db.create_thread("t_del", self.user_id, "gov", "ToDelete")
        self.assertTrue(db.delete_thread("t_del", self.user_id))
        self.assertIsNone(db.get_thread("t_del", self.user_id))

    def test_delete_nonexistent_thread(self):
        self.assertFalse(db.delete_thread("nonexistent", self.user_id))

    def test_thread_not_visible_to_other_user(self):
        other = db.create_user("other@test.com", "Other", hash_password("pass"))
        db.create_thread("t_priv", self.user_id, "gov", "Private")
        self.assertIsNone(db.get_thread("t_priv", other["id"]))

    def test_delete_all_threads(self):
        uid = db.create_user("clearall@test.com", "ClearAll", hash_password("pass"))["id"]
        db.create_thread("ca1", uid, "gov", "A")
        db.create_thread("ca2", uid, "acs", "B")
        count = db.delete_all_threads(uid)
        self.assertEqual(count, 2)
        self.assertEqual(len(db.get_threads_for_user(uid)), 0)


class MessageCrudTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if TEST_DB.exists():
            TEST_DB.unlink()
        db.init_db()
        cls.user = db.create_user("msguser@test.com", "MsgUser", hash_password("pass"))
        cls.user_id = cls.user["id"]
        cls.thread = db.create_thread("mt1", cls.user_id, "gov", "Msg Thread")

    @classmethod
    def tearDownClass(cls):
        if TEST_DB.exists():
            TEST_DB.unlink()

    def test_create_and_list_messages(self):
        db.create_message("m1", "mt1", "user", "Hello?")
        db.create_message("m2", "mt1", "assistant", "Hi there!", sql_query="SELECT 1", row_count=1)

        msgs = db.get_messages_for_thread("mt1")
        self.assertEqual(len(msgs), 2)
        self.assertEqual(msgs[0]["role"], "user")
        self.assertEqual(msgs[0]["content"], "Hello?")
        self.assertEqual(msgs[1]["role"], "assistant")
        self.assertEqual(msgs[1]["sql_query"], "SELECT 1")
        self.assertEqual(msgs[1]["row_count"], 1)

    def test_messages_ordered_by_created_at(self):
        db.create_thread("mt_ord", self.user_id, "gov", "Order Test")
        db.create_message("mo1", "mt_ord", "user", "First")
        db.create_message("mo2", "mt_ord", "assistant", "Second")
        db.create_message("mo3", "mt_ord", "user", "Third")

        msgs = db.get_messages_for_thread("mt_ord")
        self.assertEqual([m["content"] for m in msgs], ["First", "Second", "Third"])

    def test_recent_messages_limit(self):
        db.create_thread("mt_rec", self.user_id, "gov", "Recent Test")
        for i in range(10):
            db.create_message(f"mr{i}", "mt_rec", "user", f"Message {i}")

        recent = db.get_recent_messages_for_thread("mt_rec", limit=3)
        self.assertEqual(len(recent), 3)
        # Should be the last 3
        self.assertEqual(recent[0]["content"], "Message 7")

    def test_delete_message(self):
        db.create_thread("mt_delmsg", self.user_id, "gov", "Del Msg Test")
        db.create_message("mdel1", "mt_delmsg", "user", "To delete")
        self.assertTrue(db.delete_message("mdel1", "mt_delmsg"))
        self.assertEqual(len(db.get_messages_for_thread("mt_delmsg")), 0)

    def test_cascade_delete(self):
        db.create_thread("mt_cascade", self.user_id, "gov", "Cascade Test")
        db.create_message("mc1", "mt_cascade", "user", "Will be deleted")
        db.create_message("mc2", "mt_cascade", "assistant", "Also deleted")

        db.delete_thread("mt_cascade", self.user_id)
        msgs = db.get_messages_for_thread("mt_cascade")
        self.assertEqual(len(msgs), 0)

    def test_message_data_json(self):
        db.create_thread("mt_json", self.user_id, "gov", "JSON Test")
        import json
        data = json.dumps([{"state": "MD", "value": 42}])
        db.create_message("mj1", "mt_json", "assistant", "Result", data_json=data, row_count=1)

        msgs = db.get_messages_for_thread("mt_json")
        self.assertEqual(msgs[0]["data_json"], data)

    def test_thread_count(self):
        uid = db.create_user("countuser@test.com", "Count", hash_password("pass"))["id"]
        db.create_thread("ct1", uid, "gov", "A")
        db.create_thread("ct2", uid, "acs", "B")
        self.assertEqual(db.get_thread_count_for_user(uid), 2)

    def test_message_count(self):
        db.create_thread("mt_count", self.user_id, "gov", "Count Msgs")
        db.create_message("mcount1", "mt_count", "user", "A")
        db.create_message("mcount2", "mt_count", "assistant", "B")
        self.assertEqual(db.get_message_count_for_thread("mt_count"), 2)


if __name__ == "__main__":
    unittest.main()
