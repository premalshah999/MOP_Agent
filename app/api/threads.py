from __future__ import annotations

from typing import Any
from uuid import uuid4

from app.storage.sqlite import connect, payload_dumps, payload_loads, row_dict, rows_dict


def create_thread(user_id: int, dataset_id: str = "contract_county", title: str = "New thread") -> dict[str, Any]:
    thread_id = uuid4().hex
    with connect() as conn:
        conn.execute(
            "INSERT INTO threads (id, user_id, title, dataset_id) VALUES (?, ?, ?, ?)",
            (thread_id, user_id, title, dataset_id),
        )
        conn.commit()
        thread = row_dict(conn.execute("SELECT * FROM threads WHERE id = ?", (thread_id,)).fetchone())
    assert thread is not None
    return thread


def get_thread(thread_id: str, user_id: int) -> dict[str, Any] | None:
    with connect() as conn:
        return row_dict(conn.execute("SELECT * FROM threads WHERE id = ? AND user_id = ?", (thread_id, user_id)).fetchone())


def list_threads(user_id: int) -> list[dict[str, Any]]:
    with connect() as conn:
        return rows_dict(conn.execute("SELECT * FROM threads WHERE user_id = ? ORDER BY updated_at DESC LIMIT 100", (user_id,)).fetchall())


def update_thread(thread_id: str, user_id: int, *, title: str | None = None, dataset_id: str | None = None) -> dict[str, Any] | None:
    thread = get_thread(thread_id, user_id)
    if not thread:
        return None
    with connect() as conn:
        conn.execute(
            "UPDATE threads SET title = COALESCE(?, title), dataset_id = COALESCE(?, dataset_id), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ? AND user_id = ?",
            (title, dataset_id, thread_id, user_id),
        )
        conn.commit()
    return get_thread(thread_id, user_id)


def delete_thread(thread_id: str, user_id: int) -> bool:
    with connect() as conn:
        cur = conn.execute("DELETE FROM threads WHERE id = ? AND user_id = ?", (thread_id, user_id))
        conn.commit()
        return cur.rowcount > 0


def delete_all_threads(user_id: int) -> int:
    with connect() as conn:
        cur = conn.execute("DELETE FROM threads WHERE user_id = ?", (user_id,))
        conn.commit()
        return cur.rowcount


def create_message(thread_id: str, role: str, content: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    message_id = uuid4().hex
    with connect() as conn:
        conn.execute(
            "INSERT INTO messages (id, thread_id, role, content, payload_json) VALUES (?, ?, ?, ?, ?)",
            (message_id, thread_id, role, content, payload_dumps(payload)),
        )
        conn.execute("UPDATE threads SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?", (thread_id,))
        conn.commit()
        row = row_dict(conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone())
    assert row is not None
    return row


def list_messages(thread_id: str) -> list[dict[str, Any]]:
    with connect() as conn:
        return rows_dict(conn.execute("SELECT * FROM messages WHERE thread_id = ? ORDER BY created_at ASC, rowid ASC", (thread_id,)).fetchall())


def format_message(message: dict[str, Any]) -> dict[str, Any]:
    payload = payload_loads(message.get("payload_json"))
    formatted = {
        "id": message["id"],
        "role": message["role"],
        "content": message["content"],
        "ts": message["created_at"],
    }
    formatted.update(payload)
    return formatted


def format_thread(thread: dict[str, Any], *, messages: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    payload = {
        "id": thread["id"],
        "title": thread["title"],
        "datasetId": thread["dataset_id"],
        "createdAt": thread["created_at"],
        "updatedAt": thread["updated_at"],
    }
    if messages is not None:
        payload["messages"] = [format_message(message) for message in messages]
    return payload
