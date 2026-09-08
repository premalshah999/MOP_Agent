from __future__ import annotations

import json
from pathlib import Path

from app.observability.drift_report import build_report


def test_drift_report_separates_output_variance_from_backend_drift(tmp_path: Path) -> None:
    path = tmp_path / "llm.jsonl"
    rows = [
        {
            "purpose": "planner",
            "request_fingerprint": "same-contract",
            "output_fingerprint": "output-a",
            "configured_model": "alias",
            "response_model": "model-a",
            "system_fingerprint": "backend-a",
            "latency_ms": 100,
            "usage": {"total_tokens": 10},
        },
        {
            "purpose": "planner",
            "request_fingerprint": "same-contract",
            "output_fingerprint": "output-b",
            "configured_model": "alias",
            "response_model": "model-b",
            "system_fingerprint": "backend-b",
            "latency_ms": 200,
            "usage": {"total_tokens": 20},
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")

    report = build_report(path)

    assert report["summary"] == {
        "calls": 2,
        "unique_request_contracts": 1,
        "repeated_request_contracts": 1,
        "contracts_with_output_variance": 1,
        "contracts_with_backend_drift": 1,
    }
    assert report["by_purpose"]["planner"]["total_tokens"] == 30
