"""Centralized SQLite database for users, threads, and messages.

Replaces the per-function _get_db() pattern with a single connection pool
and proper schema migration.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("mop_agent.database")

_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_DB_PATH = str(_ROOT / "data" / "runtime" / "mop.sqlite3")


def _db_path() -> Path:
    return Path(os.getenv("SQLITE_DB_PATH", _DEFAULT_DB_PATH))


SCHEMA_VERSION = 1

_SCHEMA_SQL = """
-- Users table
CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    email       TEXT    UNIQUE NOT NULL COLLATE NOCASE,
    name        TEXT    NOT NULL,
    password_hash TEXT  NOT NULL,
    created_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Chat threads
CREATE TABLE IF NOT EXISTS threads (
    id          TEXT    PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title       TEXT    NOT NULL DEFAULT 'New thread',
    dataset_id  TEXT    NOT NULL DEFAULT 'government_finance',
    created_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_threads_user_id ON threads(user_id);
CREATE INDEX IF NOT EXISTS idx_threads_updated ON threads(updated_at DESC);

-- Chat messages
CREATE TABLE IF NOT EXISTS messages (
    id          TEXT    PRIMARY KEY,
    thread_id   TEXT    NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    role        TEXT    NOT NULL CHECK(role IN ('user', 'assistant')),
    content     TEXT    NOT NULL,
    sql_query   TEXT,
    data_json   TEXT,
    row_count   INTEGER DEFAULT 0,
    error       TEXT,
    created_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_messages_thread_id ON messages(thread_id);

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);
"""


def get_connection() -> sqlite3.Connection:
    """Get a SQLite connection with WAL mode and foreign keys.

    Returns a fresh connection each time. Callers in CRUD helpers
    open/close per operation to avoid locking issues.
    """
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_db() -> None:
    """Initialize schema. Safe to call multiple times."""
    conn = get_connection()
    try:
        conn.executescript(_SCHEMA_SQL)
        cur = conn.execute("SELECT MAX(version) FROM schema_version")
        row = cur.fetchone()
        current = row[0] if row[0] is not None else 0
        if current < SCHEMA_VERSION:
            conn.execute("INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
            conn.commit()
            LOGGER.info("database_initialized version=%d path=%s", SCHEMA_VERSION, _db_path())
    finally:
        conn.close()


def _dr(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row else None


def _drs(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(r) for r in rows]


# ── User queries ──

def create_user(email: str, name: str, password_hash: str) -> dict[str, Any]:
    conn = get_connection()
    try:
        conn.execute("INSERT INTO users (email, name, password_hash) VALUES (?, ?, ?)",
                     (email.lower().strip(), name.strip(), password_hash))
        conn.commit()
        row = conn.execute("SELECT id, email, name, created_at FROM users WHERE email = ?",
                          (email.lower().strip(),)).fetchone()
        return dict(row)
    finally:
        conn.close()


def get_user_by_email(email: str) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email.lower().strip(),)).fetchone()
        return _dr(row)
    finally:
        conn.close()


def get_user_by_id(user_id: int) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        row = conn.execute("SELECT id, email, name, created_at FROM users WHERE id = ?", (user_id,)).fetchone()
        return _dr(row)
    finally:
        conn.close()


# ── Thread queries ──

def create_thread(thread_id: str, user_id: int, dataset_id: str, title: str = "New thread") -> dict[str, Any]:
    conn = get_connection()
    try:
        conn.execute("INSERT INTO threads (id, user_id, title, dataset_id) VALUES (?, ?, ?, ?)",
                     (thread_id, user_id, title, dataset_id))
        conn.commit()
        row = conn.execute("SELECT * FROM threads WHERE id = ?", (thread_id,)).fetchone()
        return dict(row)
    finally:
        conn.close()


def get_threads_for_user(user_id: int, limit: int = 50) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        rows = conn.execute("SELECT * FROM threads WHERE user_id = ? ORDER BY updated_at DESC LIMIT ?",
                           (user_id, limit)).fetchall()
        return _drs(rows)
    finally:
        conn.close()


def get_thread(thread_id: str, user_id: int) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM threads WHERE id = ? AND user_id = ?",
                          (thread_id, user_id)).fetchone()
        return _dr(row)
    finally:
        conn.close()


def update_thread(thread_id: str, user_id: int, **fields: Any) -> dict[str, Any] | None:
    allowed = {"title", "dataset_id"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return get_thread(thread_id, user_id)

    set_parts = []
    values = []
    for k, v in updates.items():
        set_parts.append(f"{k} = ?")
        values.append(v)
    set_parts.append("updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')")
    values.extend([thread_id, user_id])

    conn = get_connection()
    try:
        conn.execute(f"UPDATE threads SET {', '.join(set_parts)} WHERE id = ? AND user_id = ?", values)
        conn.commit()
    finally:
        conn.close()
    return get_thread(thread_id, user_id)


def delete_thread(thread_id: str, user_id: int) -> bool:
    conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM threads WHERE id = ? AND user_id = ?", (thread_id, user_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def delete_all_threads(user_id: int) -> int:
    conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM threads WHERE user_id = ?", (user_id,))
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


# ── Message queries ──

def create_message(
    message_id: str, thread_id: str, role: str, content: str,
    sql_query: str | None = None, data_json: str | None = None,
    row_count: int = 0, error: str | None = None,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO messages (id, thread_id, role, content, sql_query, data_json, row_count, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (message_id, thread_id, role, content, sql_query, data_json, row_count, error),
        )
        conn.execute("UPDATE threads SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?", (thread_id,))
        conn.commit()
        row = conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
        return dict(row)
    finally:
        conn.close()


def get_messages_for_thread(thread_id: str, limit: int = 200) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        rows = conn.execute("SELECT * FROM messages WHERE thread_id = ? ORDER BY created_at ASC LIMIT ?",
                           (thread_id, limit)).fetchall()
        return _drs(rows)
    finally:
        conn.close()


def get_recent_messages_for_thread(thread_id: str, limit: int = 20) -> list[dict[str, Any]]:
    """Get the most recent N messages for building LLM history context."""
    conn = get_connection()
    try:
        total = conn.execute("SELECT COUNT(*) FROM messages WHERE thread_id = ?", (thread_id,)).fetchone()[0]
        offset = max(0, total - limit)
        rows = conn.execute(
            "SELECT * FROM messages WHERE thread_id = ? ORDER BY created_at ASC, rowid ASC LIMIT ? OFFSET ?",
            (thread_id, limit, offset),
        ).fetchall()
        return _drs(rows)
    finally:
        conn.close()


def delete_message(message_id: str, thread_id: str) -> bool:
    conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM messages WHERE id = ? AND thread_id = ?", (message_id, thread_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def get_thread_count_for_user(user_id: int) -> int:
    conn = get_connection()
    try:
        return conn.execute("SELECT COUNT(*) FROM threads WHERE user_id = ?", (user_id,)).fetchone()[0]
    finally:
        conn.close()


def get_message_count_for_thread(thread_id: str) -> int:
    conn = get_connection()
    try:
        return conn.execute("SELECT COUNT(*) FROM messages WHERE thread_id = ?", (thread_id,)).fetchone()[0]
    finally:
        conn.close()
