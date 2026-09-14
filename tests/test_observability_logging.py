from __future__ import annotations

import json

from app.observability.logging import append_jsonl


def test_jsonl_logs_are_private_and_complete(tmp_path) -> None:
    log_path = tmp_path / "events.jsonl"
    payload = {"request_id": "trace-123", "status": "answered"}

    append_jsonl(log_path, payload)

    assert log_path.stat().st_mode & 0o777 == 0o600
    assert json.loads(log_path.read_text(encoding="utf-8")) == payload
