"""Reasoning-mode evaluator.

Runs every task in `reasoning_tasks.yaml` through the orchestrator in
mode='reasoning' and grades the answer with an LLM judge on a rubric:
  - groundedness   (0-10) every claim supported by the tool/SQL results
  - completeness   (0-10) addresses the user's question
  - usefulness     (0-10) gives framing/context beyond a bare lookup
  - no_hallucination (0-10) no invented entities, ranks, or numbers

A task passes if its average rubric score >= 7. Gate passes if >=80% of
tasks pass.

Usage:  python -m app.evals.reasoning_eval [--json] [--limit N]
"""

from __future__ import annotations

import argparse
import json
from typing import Any

import yaml  # type: ignore[import-untyped]

from app.core.orchestrator import answer_question
from app.llm import client
from app.paths import ROOT_DIR

TASKS_PATH = ROOT_DIR / "app" / "evals" / "reasoning_tasks.yaml"
TASK_PASS_THRESHOLD = 7.0
GATE_PASS_THRESHOLD = 0.80


_JUDGE_SYSTEM = """You grade an analyst's answer to a US public-policy data question.

You see the user's question, the answer, and the data the analyst's tools actually
returned (last SQL rows, last SQL statement). Score each dimension 0-10:

- groundedness: every number, rank, name, comparison in the answer is supported
  by the rows. Reasonable rounding/unit formatting ($1.2B for 1234000000) is fine.
- completeness: the answer addresses what the user actually asked.
- usefulness: it adds the context the question implies (peer position for
  "is X high?", a delta for "compare", a rank for "where does X stand"),
  not just a bare number.
- no_hallucination: 10 if zero fabricated entities/numbers/claims; lower if any.

Return ONLY JSON:
{"groundedness": <0-10>, "completeness": <0-10>, "usefulness": <0-10>,
 "no_hallucination": <0-10>, "reason": "<one short sentence>"}"""


def _load_tasks() -> list[dict[str, Any]]:
    return list(yaml.safe_load(TASKS_PATH.read_text()) or [])


def _judge(
    question: str,
    answer: str,
    sql: str | None,
    rows: list[dict[str, Any]],
    tool_results: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    # Compact every tool result to keep the judge prompt bounded.
    tool_trail = []
    for tr in (tool_results or []):
        res = tr.get("result", {}) or {}
        if tr.get("name") == "run_sql":
            tool_trail.append({"tool": "run_sql", "sql": res.get("sql", "")[:200], "rows": res.get("rows", [])[:15], "error": res.get("error")})
        elif tr.get("name") == "peer_stats":
            tool_trail.append({"tool": "peer_stats", "args": tr.get("args"), "stats": res.get("stats"), "top5": res.get("top5"), "bottom5": res.get("bottom5")})
        elif tr.get("name") == "distinct_values":
            tool_trail.append({"tool": "distinct_values", "args": tr.get("args"), "count": res.get("count"), "sample": res.get("values", [])[:10]})
        elif tr.get("name") == "get_schema":
            tool_trail.append({"tool": "get_schema", "args": tr.get("args")})
    user = (
        f"QUESTION: {question}\n\nANSWER:\n{answer}\n\n"
        f"LAST SQL:\n{sql or '(none)'}\n\n"
        f"LAST ROWS (up to 30):\n{json.dumps(rows[:30], default=str, indent=2)}\n\n"
        f"FULL TOOL TRAIL (peer_stats and other tools agent used as evidence):\n"
        f"{json.dumps(tool_trail, default=str, indent=2)[:6000]}\n\n"
        "Grade the answer."
    )
    try:
        raw = client.chat_json(
            [{"role": "system", "content": _JUDGE_SYSTEM}, {"role": "user", "content": user}],
            temperature=0.0,
            max_tokens=300,
            purpose="reasoning_judge",
        )
    except client.LLMError as exc:
        return {"error": str(exc), "groundedness": 0, "completeness": 0, "usefulness": 0, "no_hallucination": 0}
    return {
        "groundedness": float(raw.get("groundedness", 0)),
        "completeness": float(raw.get("completeness", 0)),
        "usefulness": float(raw.get("usefulness", 0)),
        "no_hallucination": float(raw.get("no_hallucination", 0)),
        "reason": str(raw.get("reason", "")),
    }


def run_reasoning_evals(limit: int | None = None) -> dict[str, Any]:
    tasks = _load_tasks()
    if limit:
        tasks = tasks[:limit]
    results: list[dict[str, Any]] = []
    for task in tasks:
        q = task["question"]
        try:
            r = answer_question(q, mode="reasoning")
        except Exception as exc:
            results.append(
                {"id": task["id"], "question": q, "error": f"pipeline: {exc}", "avg": 0.0, "pass": False}
            )
            continue
        sql = r.get("sql")
        rows = r.get("data") or []
        answer = r.get("answer", "") or ""
        tool_results = (r.get("resultPackage") or {}).get("tool_results") or []
        scores = _judge(q, answer, sql, rows, tool_results)
        avg = round(
            (scores["groundedness"] + scores["completeness"] + scores["usefulness"] + scores["no_hallucination"]) / 4,
            2,
        )
        agent_stage = next((s for s in r.get("pipelineTrace", {}).get("stages", []) if s.get("name") == "reasoning_agent"), None)
        results.append(
            {
                "id": task["id"],
                "question": q,
                "stopped": (agent_stage or {}).get("data", {}).get("stopped"),
                "tool_calls": (agent_stage or {}).get("data", {}).get("tool_calls"),
                "answer_preview": answer[:160].replace("\n", " "),
                **scores,
                "avg": avg,
                "pass": avg >= TASK_PASS_THRESHOLD,
            }
        )
    n = len(results)
    passed = sum(1 for r in results if r["pass"])
    pass_rate = passed / n if n else 1.0
    return {
        "summary": {
            "tasks": n,
            "passed": passed,
            "pass_rate": round(pass_rate, 3),
            "task_pass_threshold": TASK_PASS_THRESHOLD,
            "gate_threshold": GATE_PASS_THRESHOLD,
            "gate": pass_rate >= GATE_PASS_THRESHOLD,
        },
        "results": results,
    }


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except Exception:
        pass
    p = argparse.ArgumentParser()
    p.add_argument("--json", action="store_true")
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()
    report = run_reasoning_evals(limit=args.limit)
    if args.json:
        print(json.dumps(report, indent=2))
        return 0 if report["summary"]["gate"] else 1
    s = report["summary"]
    print("Reasoning evaluation")
    print(f"  tasks: {s['tasks']} | passed: {s['passed']} | rate: {s['pass_rate']:.0%} "
          f"(threshold {s['gate_threshold']:.0%})")
    print(f"  GATE: {'PASS' if s['gate'] else 'FAIL'}")
    for r in report["results"]:
        flag = "✓" if r["pass"] else "✗"
        print(
            f"  {flag} {r['id']} avg={r.get('avg', 0):.1f} "
            f"(g={r.get('groundedness', 0)} c={r.get('completeness', 0)} "
            f"u={r.get('usefulness', 0)} nh={r.get('no_hallucination', 0)}) "
            f"calls={r.get('tool_calls')} stopped={r.get('stopped')}"
        )
        if not r["pass"]:
            print(f"      reason: {r.get('reason', '')[:120]}")
            print(f"      answer: {r.get('answer_preview', '')}")
    return 0 if s["gate"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
