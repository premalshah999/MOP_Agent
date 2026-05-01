from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from app.core.orchestrator import answer_question


GOLDEN_PATH = Path(__file__).with_name("golden_questions.yaml")


def _load_questions(path: Path = GOLDEN_PATH) -> list[dict[str, Any]]:
    with path.open() as f:
        payload = yaml.safe_load(f) or []
    return list(payload)


def run_golden_evals(path: Path = GOLDEN_PATH) -> dict[str, Any]:
    items = _load_questions(path)
    results: list[dict[str, Any]] = []
    for item in items:
        response = answer_question(item["question"])
        contract = response.get("contract") or {}
        passed = (
            contract.get("contract_type") == item.get("expected_intent")
            and contract.get("family") == item.get("expected_dataset")
            and contract.get("metric") == item.get("expected_metric")
            and (response.get("sql") is None if "no_sql" in item.get("checks", []) else response.get("resolution") == "answered")
        )
        results.append(
            {
                "id": item.get("id"),
                "question": item.get("question"),
                "passed": passed,
                "expected": {
                    "intent": item.get("expected_intent"),
                    "dataset": item.get("expected_dataset"),
                    "metric": item.get("expected_metric"),
                },
                "actual": {
                    "intent": contract.get("contract_type"),
                    "dataset": contract.get("family"),
                    "metric": contract.get("metric"),
                    "resolution": response.get("resolution"),
                    "row_count": response.get("row_count"),
                },
            }
        )
    passed_count = sum(1 for result in results if result["passed"])
    return {"passed": passed_count, "total": len(results), "results": results}


if __name__ == "__main__":
    import json

    print(json.dumps(run_golden_evals(), indent=2))
