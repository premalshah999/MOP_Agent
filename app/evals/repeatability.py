"""Live gate: identical questions must produce identical evidence.

Natural-language phrasing may vary, but the contract, resolution, and returned
rows must not.  This directly detects the production failure mode where the
same query silently changed tables, years, filters, or computation.
"""

from __future__ import annotations

import json
from typing import Any

from app.core.orchestrator import answer_question


QUESTIONS = [
    "how many grant dollars did Maryland receive",
    "top 10 counties in Maryland by grants",
    "compare Maryland vs Virginia on grants",
    "subcontract inflow to Maryland",
    "What is the correlation between federal grant dollars and poverty rate across states?",
]


def _normalized_rows(rows: list[dict[str, Any]]) -> str:
    # SQL is required to stabilize ranking ties, but sorting here makes the
    # signature insensitive to JSON key order and harmless DB serialization.
    normalized = [
        {str(key): value for key, value in sorted(row.items())}
        for row in rows
    ]
    return json.dumps(normalized, sort_keys=True, default=str, separators=(",", ":"))


def _signature(result: dict[str, Any]) -> dict[str, Any]:
    contract = result.get("contract") or {}
    return {
        "resolution": result.get("resolution"),
        "tables": contract.get("tables"),
        "metric": contract.get("metric"),
        "operation": contract.get("operation"),
        "year": contract.get("year"),
        "sort_direction": contract.get("sort_direction"),
        "top_k": contract.get("top_k"),
        "rows": _normalized_rows(result.get("data") or []),
    }


def run(repeats: int = 2) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    for question in QUESTIONS:
        runs = [answer_question(question) for _ in range(repeats)]
        signatures = [_signature(result) for result in runs]
        if any(signature != signatures[0] for signature in signatures[1:]):
            failures.append({"question": question, "signatures": signatures})
        if any(result.get("resolution") != "answered" for result in runs):
            failures.append({
                "question": question,
                "resolutions": [result.get("resolution") for result in runs],
            })
    return failures


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except Exception:
        pass
    failures = run()
    if failures:
        print(json.dumps({"passed": False, "failures": failures}, indent=2))
        return 1
    print(json.dumps({"passed": True, "questions": len(QUESTIONS), "repeats": 2}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
