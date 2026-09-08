"""Live multi-turn regression gate across every loaded dataset family.

Run this before changing providers or prompts. It verifies that follow-ups
preserve or replace the intended entities, measures, periods, ranking shape,
and flow direction using the structured contract persisted after each answer.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from typing import Any

from app.core.pipeline import answer_question


@dataclass(frozen=True)
class Turn:
    question: str
    tables: set[str]
    metrics: set[str] = field(default_factory=set)
    entities: set[str] = field(default_factory=set)
    forbidden_entities: set[str] = field(default_factory=set)
    operation: str | None = None
    flow_direction: str | None = None
    top_k: int | None = None


CONVERSATIONS: dict[str, list[Turn]] = {
    "government_finance": [
        Turn(
            "Compare Maryland and Virginia on total liabilities per capita.",
            {"gov_state"},
            {"total liabilities per capita"},
            {"maryland", "virginia"},
            operation="comparison",
        ),
        Turn(
            "now compare their debt ratios",
            {"gov_state"},
            {"debt ratio"},
            {"maryland", "virginia"},
            operation="comparison",
        ),
    ],
    "acs": [
        Turn(
            "What was Maryland's poverty rate in 2023?",
            {"acs_state"},
            {"below poverty"},
            {"maryland"},
        ),
        Turn(
            "what about Virginia?",
            {"acs_state"},
            {"below poverty"},
            {"virginia"},
            forbidden_entities={"maryland"},
        ),
    ],
    "acs_result_reference": [
        Turn(
            "Which counties have a higher renter population than owner population?",
            {"acs_county"},
            {"renter occupied", "owner occupied"},
            operation="comparison",
        ),
        Turn(
            "Provide the corresponding states of the above counties.",
            {"acs_county"},
            {"renter occupied", "owner occupied"},
        ),
    ],
    "federal_spending": [
        Turn(
            "How much federal grant funding did Maryland receive in 2024?",
            {"contract_state"},
            {"grants"},
            {"maryland"},
        ),
        Turn(
            "and direct payments?",
            {"contract_state"},
            {"direct payments"},
            {"maryland"},
        ),
    ],
    "dataset_metric_correction": [
        Turn(
            "How many employees does contract_static_state dataset have?",
            {"contract_state"},
            {"employees"},
            operation="aggregate",
        ),
        Turn(
            "but the actual number of employees?",
            {"contract_state"},
            {"employees"},
            operation="aggregate",
        ),
    ],
    "agency_spending": [
        Turn(
            "Which 5 agencies provided the most contracts in Maryland in 2024?",
            {"spending_state_agency"},
            {"contracts"},
            {"maryland"},
            operation="ranking",
            top_k=5,
        ),
        Turn(
            "what about grants?",
            {"spending_state_agency"},
            {"grants"},
            {"maryland"},
            operation="ranking",
            top_k=5,
        ),
    ],
    "financial_capability": [
        Turn(
            "Compare Maryland and Virginia on financial literacy in 2021.",
            {"finra_state"},
            {"financial literacy"},
            {"maryland", "virginia"},
            operation="comparison",
        ),
        Turn(
            "and financial constraint?",
            {"finra_state"},
            {"financial constraint"},
            {"maryland", "virginia"},
            operation="comparison",
        ),
    ],
    "subaward_flow": [
        Turn(
            "How much subcontract funding flows out of Maryland?",
            {"state_flow"},
            {"subaward amount year"},
            {"maryland"},
            operation="aggregate",
            flow_direction="outflow",
        ),
        Turn(
            "compare that with Virginia's outflow",
            {"state_flow"},
            {"subaward amount year"},
            {"maryland", "virginia"},
            operation="comparison",
            flow_direction="outflow",
        ),
    ],
    "cross_dataset": [
        Turn(
            "Compare Maryland and Virginia on financial literacy and poverty rate.",
            {"finra_state", "acs_state"},
            {"financial literacy", "below poverty"},
            {"maryland", "virginia"},
            operation="comparison",
        ),
        Turn(
            "add government debt ratio to that comparison",
            {"finra_state", "acs_state", "gov_state"},
            {"financial literacy", "below poverty", "debt ratio"},
            {"maryland", "virginia"},
            operation="comparison",
        ),
    ],
}


def _norm(value: Any) -> str:
    return " ".join(str(value).casefold().replace("_", " ").replace(",", " ").split())


def _contains_all(actual: list[Any], expected: set[str]) -> bool:
    normalized = [_norm(value) for value in actual]
    return all(
        any(_norm(item) in value or value in _norm(item) for value in normalized)
        for item in expected
    )


def _grade(result: dict[str, Any], expected: Turn) -> list[str]:
    problems: list[str] = []
    if result.get("resolution") != "answered":
        problems.append(f"resolution={result.get('resolution')!r}")
    contract = result.get("contract") or {}
    memory = contract.get("context_memory") or {}
    actual_tables = set(memory.get("tables") or contract.get("tables") or [])
    if actual_tables != expected.tables:
        problems.append(f"tables={sorted(actual_tables)} expected={sorted(expected.tables)}")
    if expected.metrics and not _contains_all(memory.get("metrics") or [], expected.metrics):
        problems.append(f"metrics={memory.get('metrics')} expected={sorted(expected.metrics)}")
    actual_entities = memory.get("entities") or []
    if expected.entities and not _contains_all(actual_entities, expected.entities):
        problems.append(f"entities={actual_entities} expected={sorted(expected.entities)}")
    if any(_contains_all(actual_entities, {item}) for item in expected.forbidden_entities):
        problems.append(f"stale entities retained: {sorted(expected.forbidden_entities)}")
    if expected.operation and memory.get("operation") != expected.operation:
        problems.append(f"operation={memory.get('operation')!r} expected={expected.operation!r}")
    if expected.flow_direction and memory.get("flow_direction") != expected.flow_direction:
        problems.append(
            f"flow_direction={memory.get('flow_direction')!r} expected={expected.flow_direction!r}"
        )
    if expected.top_k is not None and memory.get("top_k") != expected.top_k:
        problems.append(f"top_k={memory.get('top_k')!r} expected={expected.top_k!r}")
    return problems


def run(mode: str = "normal") -> dict[str, Any]:
    report: dict[str, Any] = {"mode": mode, "conversations": {}, "passed": True}
    for name, turns in CONVERSATIONS.items():
        history: list[dict[str, Any]] = []
        turn_results: list[dict[str, Any]] = []
        for expected in turns:
            result = answer_question(expected.question, history, mode=mode)
            problems = _grade(result, expected)
            turn_results.append(
                {
                    "question": expected.question,
                    "passed": not problems,
                    "problems": problems,
                    "resolution": result.get("resolution"),
                    "answer_preview": str(result.get("answer") or "")[:800],
                    "sql": result.get("sql"),
                    "context_memory": (result.get("contract") or {}).get("context_memory"),
                    "analysis_contract": (result.get("resultPackage") or {}).get(
                        "analysis_contract"
                    ),
                    "quality": result.get("quality"),
                    "pipeline_stages": [
                        {
                            "name": stage.get("name"),
                            "status": stage.get("status"),
                            "data": stage.get("data"),
                        }
                        for stage in ((result.get("pipelineTrace") or {}).get("stages") or [])
                    ],
                }
            )
            history.extend(
                [
                    {"role": "user", "content": expected.question},
                    {
                        "role": "assistant",
                        "content": result.get("answer") or "",
                        "contract": result.get("contract") or {},
                        "suggested_followups": result.get("suggested_followups") or [],
                    },
                ]
            )
        conversation_passed = all(item["passed"] for item in turn_results)
        report["conversations"][name] = {
            "passed": conversation_passed,
            "turns": turn_results,
        }
        report["passed"] = report["passed"] and conversation_passed
    return report


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except Exception:
        pass
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("normal", "reasoning"), default="normal")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    report = run(args.mode)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        conversations = report["conversations"]
        turns = [turn for item in conversations.values() for turn in item["turns"]]
        passed_turns = sum(bool(turn["passed"]) for turn in turns)
        print(f"Conversation evaluation ({args.mode})")
        print(f"  conversations: {len(conversations)}")
        print(f"  turns:         {passed_turns}/{len(turns)} passed")
        print(f"  GATE: {'PASS' if report['passed'] else 'FAIL'}")
        for name, item in conversations.items():
            if item["passed"]:
                continue
            for turn in item["turns"]:
                if not turn["passed"]:
                    print(f"  ✗ {name}: {turn['question']}: {', '.join(turn['problems'])}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
