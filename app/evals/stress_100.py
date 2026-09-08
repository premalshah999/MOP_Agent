"""Run the 100-question whole-catalog stress benchmark.

The benchmark is intentionally outside the request path. It grades the typed
analysis contract and the evidence-backed response, then preserves every full
answer and SQL statement for manual review.

Usage:
    python -m app.evals.stress_100
    python -m app.evals.stress_100 --workers 4 --limit 10
"""

from __future__ import annotations

import argparse
import json
import math
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

import yaml  # type: ignore[import-untyped]

from app.core.pipeline import answer_question
from app.paths import ROOT_DIR

MANIFEST_PATH = ROOT_DIR / "app" / "evals" / "stress_questions_100.yaml"
REPORT_DIR = ROOT_DIR / "reports" / "stress_100"


def load_cases(path: Path = MANIFEST_PATH) -> list[dict[str, Any]]:
    raw = yaml.safe_load(path.read_text()) or {}
    cases = list(raw.get("cases") or [])
    ids = [str(case.get("id") or "") for case in cases]
    if raw.get("version") != 1 or len(cases) != 100:
        raise ValueError("stress manifest must be version 1 with exactly 100 cases")
    if any(not value for value in ids) or len(ids) != len(set(ids)):
        raise ValueError("all stress case IDs must be non-empty and unique")
    return cases


def _faithfulness(result: dict[str, Any]) -> tuple[bool, str]:
    stages = (result.get("pipelineTrace") or {}).get("stages") or []
    if any(stage.get("name") == "evidence_fallback" for stage in stages):
        return True, "validated evidence fallback"
    verdicts = [
        stage.get("data") or {} for stage in stages if stage.get("name") == "faithfulness_judge"
    ]
    if not verdicts:
        return False, "no faithfulness verdict"
    verdict = verdicts[-1]
    return bool(verdict.get("faithful")), str(verdict.get("reason") or "")


def _grade(case: dict[str, Any], result: dict[str, Any], elapsed_ms: float) -> dict[str, Any]:
    expected = case.get("expect") or {}
    contract = result.get("contract") or {}
    analysis = (result.get("resultPackage") or {}).get("analysis_contract") or {}
    resolution = str(result.get("resolution") or "")
    tables = list(contract.get("tables") or [])
    metrics = list(analysis.get("metric_columns") or [])
    problems: list[str] = []

    accepted_resolutions = expected.get("resolutions") or ["answered"]
    if resolution not in accepted_resolutions:
        problems.append(f"resolution={resolution!r}, expected one of {accepted_resolutions}")

    expected_tables = expected.get("tables")
    if expected_tables is not None and set(tables) != set(expected_tables):
        problems.append(f"tables={tables}, expected={expected_tables}")

    required_metrics = expected.get("metrics") or []
    missing_metrics = [metric for metric in required_metrics if metric not in metrics]
    if missing_metrics:
        problems.append(f"missing metrics={missing_metrics}; selected={metrics}")

    operation = analysis.get("operation") or contract.get("operation")
    if expected.get("operation") and operation != expected["operation"]:
        problems.append(f"operation={operation!r}, expected={expected['operation']!r}")

    direction = analysis.get("flow_direction") or contract.get("flow_direction")
    if expected.get("flow_direction") and direction != expected["flow_direction"]:
        problems.append(f"flow_direction={direction!r}, expected={expected['flow_direction']!r}")

    rows = result.get("data") or []
    if "min_rows" in expected and len(rows) < int(expected["min_rows"]):
        problems.append(f"rows={len(rows)} below {expected['min_rows']}")
    if "max_rows" in expected and len(rows) > int(expected["max_rows"]):
        problems.append(f"rows={len(rows)} above {expected['max_rows']}")
    if "row_count" in expected and len(rows) != int(expected["row_count"]):
        problems.append(f"rows={len(rows)}, expected={expected['row_count']}")

    if expected.get("map") and not (result.get("mapIntent") or {}).get("enabled"):
        problems.append("expected enabled map")
    if expected.get("chart") and not (result.get("chart") or result.get("charts")):
        problems.append("expected chart")

    answered = resolution == "answered"
    if answered and not result.get("sql"):
        problems.append("answered without SQL")
    if answered and not rows:
        problems.append("answered without evidence rows")
    if not str(result.get("answer") or "").strip():
        problems.append("empty answer")

    faithful, faithful_reason = _faithfulness(result) if answered else (True, "not applicable")
    if answered and not faithful:
        problems.append(f"unfaithful: {faithful_reason}")

    quality = result.get("quality") or {}
    return {
        "id": case["id"],
        "dataset": case.get("dataset"),
        "category": case.get("category"),
        "conversation": case.get("conversation"),
        "question": case["question"],
        "passed": not problems,
        "problems": problems,
        "latency_ms": round(elapsed_ms, 1),
        "resolution": resolution,
        "tables": tables,
        "metrics": metrics,
        "operation": operation,
        "flow_direction": direction,
        "row_count": len(rows),
        "faithful": faithful,
        "faithfulness_reason": faithful_reason,
        "quality": quality,
        "answer": result.get("answer"),
        "sql": result.get("sql"),
        "map_intent": result.get("mapIntent"),
        "contract": contract,
        "analysis_contract": analysis,
        "data": rows,
    }


def _run_one(
    case: dict[str, Any], history: list[dict[str, Any]] | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    started = time.perf_counter()
    try:
        result = answer_question(str(case["question"]), history or [])
        graded = _grade(case, result, (time.perf_counter() - started) * 1000)
    except Exception as exc:
        result = {}
        graded = {
            "id": case["id"],
            "dataset": case.get("dataset"),
            "category": case.get("category"),
            "conversation": case.get("conversation"),
            "question": case["question"],
            "passed": False,
            "problems": [f"pipeline exception: {type(exc).__name__}: {exc}"],
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "resolution": "exception",
            "answer": "",
            "sql": None,
        }
    return graded, result


def _run_conversation(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for case in cases:
        graded, result = _run_one(case, history)
        rows.append(graded)
        history.extend(
            [
                {"role": "user", "content": str(case["question"])},
                {
                    "role": "assistant",
                    "content": str(result.get("answer") or ""),
                    "contract": result.get("contract") or {},
                    "suggested_followups": result.get("suggested_followups") or [],
                },
            ]
        )
        history = history[-12:]
    return rows


def _summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    latencies = [float(row["latency_ms"]) for row in results]
    ordered = sorted(latencies)
    by_dataset: dict[str, dict[str, int]] = defaultdict(lambda: {"passed": 0, "total": 0})
    by_category: dict[str, dict[str, int]] = defaultdict(lambda: {"passed": 0, "total": 0})
    for row in results:
        for bucket, key in (
            (by_dataset, row.get("dataset") or "unknown"),
            (by_category, row.get("category") or "unknown"),
        ):
            bucket[key]["total"] += 1
            bucket[key]["passed"] += int(bool(row["passed"]))
    passed = sum(bool(row["passed"]) for row in results)
    return {
        "total": len(results),
        "passed": passed,
        "failed": len(results) - passed,
        "pass_rate": round(passed / max(len(results), 1), 4),
        "faithful_answer_rate": round(
            sum(bool(row.get("faithful")) for row in results if row.get("resolution") == "answered")
            / max(sum(row.get("resolution") == "answered" for row in results), 1),
            4,
        ),
        "latency": {
            "p50_ms": round(median(latencies), 1) if latencies else None,
            "p95_ms": round(ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)], 1)
            if ordered
            else None,
            "max_ms": round(max(latencies), 1) if latencies else None,
        },
        "by_dataset": dict(sorted(by_dataset.items())),
        "by_category": dict(sorted(by_category.items())),
    }


def _write_report(results: list[dict[str, Any]], started_at: str) -> tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    summary = _summary(results)
    payload = {
        "started_at": started_at,
        "completed_at": datetime.now(UTC).isoformat(),
        "summary": summary,
        "results": results,
    }
    json_path = REPORT_DIR / f"stress_100_{timestamp}.json"
    md_path = REPORT_DIR / f"stress_100_{timestamp}.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str))

    lines = [
        "# 100-question chatbot stress report",
        "",
        f"- Passed: **{summary['passed']}/{summary['total']} ({summary['pass_rate']:.1%})**",
        f"- Faithful answered responses: **{summary['faithful_answer_rate']:.1%}**",
        f"- Latency p50 / p95 / max: **{summary['latency']['p50_ms']} / {summary['latency']['p95_ms']} / {summary['latency']['max_ms']} ms**",
        "",
        "## Results",
        "",
    ]
    for row in results:
        marker = "PASS" if row["passed"] else "FAIL"
        lines.extend(
            [
                f"### {row['id']} — {marker}",
                "",
                f"**Question:** {row['question']}",
                "",
                f"**Route:** `{row.get('tables')}` · `{row.get('operation')}` · {row.get('row_count')} rows · {row.get('latency_ms')} ms",
                "",
                *(
                    [f"**Problems:** {'; '.join(row['problems'])}", ""]
                    if row.get("problems")
                    else []
                ),
                "**Answer:**",
                "",
                str(row.get("answer") or "").strip(),
                "",
            ]
        )
    md_path.write_text("\n".join(lines))
    (REPORT_DIR / "latest.json").write_text(json.dumps(payload, indent=2, default=str))
    (REPORT_DIR / "latest.md").write_text("\n".join(lines))
    return json_path, md_path


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv(ROOT_DIR / ".env")
    except Exception:
        pass
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    cases = load_cases()
    if args.limit:
        cases = cases[: args.limit]
    indexed = {case["id"]: index for index, case in enumerate(cases)}
    independent = [case for case in cases if not case.get("conversation")]
    conversations: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for case in cases:
        if case.get("conversation"):
            conversations[str(case["conversation"])].append(case)

    started_at = datetime.now(UTC).isoformat()
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, min(args.workers, 8))) as pool:
        futures = {pool.submit(_run_one, case): case["id"] for case in independent}
        futures.update(
            {
                pool.submit(_run_conversation, group): f"conversation:{name}"
                for name, group in conversations.items()
            }
        )
        for future in as_completed(futures):
            value = future.result()
            if isinstance(value, list):
                results.extend(value)
            else:
                results.append(value[0])
            print(f"[{len(results):03d}/{len(cases):03d}] {futures[future]}", flush=True)

    results.sort(key=lambda row: indexed[row["id"]])
    json_path, md_path = _write_report(results, started_at)
    summary = _summary(results)
    print(json.dumps(summary, indent=2))
    print(f"JSON: {json_path}")
    print(f"Markdown: {md_path}")
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
