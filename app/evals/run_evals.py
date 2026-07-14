"""Strict golden evaluation for the LLM-grounded pipeline.

For every golden case it runs the real pipeline and grades:
  - intent bucket
  - routed tables (ANALYTICAL only)
  - answer correctness vs an independent reference query
  - faithfulness (LLM judge, when flagged)

Exits non-zero if any threshold is missed (CI gate).

Usage:  python -m app.evals.run_evals [--json]
"""

from __future__ import annotations

import json
import re
import sys
from typing import Any

from app.core.orchestrator import answer_question
from app.evals.faithfulness import judge_faithfulness
from app.evals.reference import GoldenCase, load_golden, reference_scalar, run_reference_sql

THRESHOLDS = {
    "intent_accuracy": 0.90,
    "routing_accuracy": 0.90,
    "generation_pass_rate": 0.85,
    "faithfulness_pass_rate": 0.90,
}


def _numbers(text: str) -> list[float]:
    out: list[float] = []
    for tok in re.findall(r"-?\d[\d,]*\.?\d*", text or ""):
        try:
            out.append(float(tok.replace(",", "")))
        except ValueError:
            pass
    return out


def _check_expectation(case: GoldenCase, result: dict[str, Any]) -> tuple[bool, str]:
    data = result.get("data") or []
    blob = (result.get("answer") or "") + " " + json.dumps(data, default=str)
    exp = case.expect
    if "row_count" in exp and len(data) != exp["row_count"]:
        return False, f"rows {len(data)} != {exp['row_count']}"
    if "min_rows" in exp and len(data) < exp["min_rows"]:
        return False, f"rows {len(data)} < {exp['min_rows']}"
    if exp.get("top_label"):
        ref = run_reference_sql(case.reference_sql)
        label = str(list(ref[0].values())[0]).lower()
        if label not in blob.lower():
            return False, f"top label {label!r} missing"
    if exp.get("scalar"):
        ref = float(reference_scalar(case.reference_sql))
        tol = float(exp.get("rel_tol", 0.01))
        cands = _numbers(blob) + [
            float(k["value"]) for k in result.get("key_numbers", [])
            if str(k.get("value")).replace(".", "", 1).lstrip("-").isdigit()
        ]
        if not any(abs(c - ref) <= tol * max(1.0, abs(ref)) for c in cands):
            return False, f"no value ≈ {ref}"
    for needle in exp.get("answer_contains", []):
        if needle.lower() not in (result.get("answer") or "").lower():
            return False, f"answer missing {needle!r}"
    return True, "ok"


def run_golden_evals() -> dict[str, Any]:
    cases = load_golden()
    results: list[dict[str, Any]] = []
    intent_ok = routing_ok = routing_total = gen_ok = gen_total = faith_ok = faith_total = 0

    for case in cases:
        res = answer_question(case.question)
        got_intent = (res.get("contract") or {}).get("contract_type")
        intent_match = got_intent == case.intent
        intent_ok += intent_match
        row: dict[str, Any] = {
            "id": case.id,
            "question": case.question,
            "intent_expected": case.intent,
            "intent_got": got_intent,
            "intent_ok": intent_match,
        }

        if case.intent == "ANALYTICAL":
            routing_total += 1
            got_tables = set((res.get("contract") or {}).get("tables") or [])
            r_ok = got_tables == set(case.tables)
            routing_ok += r_ok
            row["tables_expected"] = case.tables
            row["tables_got"] = sorted(got_tables)
            row["routing_ok"] = r_ok

            gen_total += 1
            has_sql = bool(res.get("sql"))
            exp_ok, why = _check_expectation(case, res) if has_sql else (False, "no sql")
            g_ok = has_sql and exp_ok
            gen_ok += g_ok
            row["generation_ok"] = g_ok
            row["generation_detail"] = why

            if case.faithfulness and g_ok:
                faith_total += 1
                verdict = judge_faithfulness(
                    case.question, res.get("answer", ""), res.get("data") or [], res.get("sql") or ""
                )
                faith_ok += bool(verdict.get("faithful"))
                row["faithful"] = bool(verdict.get("faithful"))
                row["faithful_reason"] = verdict.get("reason")
        else:
            row["no_sql_ok"] = res.get("sql") is None

        results.append(row)

    n = len(cases)
    summary = {
        "total": n,
        "intent_accuracy": round(intent_ok / n, 3),
        "routing_accuracy": round(routing_ok / routing_total, 3) if routing_total else 1.0,
        "generation_pass_rate": round(gen_ok / gen_total, 3) if gen_total else 1.0,
        "faithfulness_pass_rate": round(faith_ok / faith_total, 3) if faith_total else 1.0,
    }
    summary["thresholds"] = THRESHOLDS
    summary["passed_gate"] = all(summary[k] >= v for k, v in THRESHOLDS.items())
    return {"summary": summary, "results": results}


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except Exception:
        pass
    report = run_golden_evals()
    s = report["summary"]
    if "--json" in sys.argv:
        print(json.dumps(report, indent=2))
    else:
        print("Golden evaluation")
        print(f"  cases: {s['total']}")
        print(f"  intent accuracy:       {s['intent_accuracy']:.0%}  (>= {THRESHOLDS['intent_accuracy']:.0%})")
        print(f"  routing accuracy:      {s['routing_accuracy']:.0%}  (>= {THRESHOLDS['routing_accuracy']:.0%})")
        print(f"  generation pass rate:  {s['generation_pass_rate']:.0%}  (>= {THRESHOLDS['generation_pass_rate']:.0%})")
        print(f"  faithfulness pass:     {s['faithfulness_pass_rate']:.0%}  (>= {THRESHOLDS['faithfulness_pass_rate']:.0%})")
        print(f"  GATE: {'PASS' if s['passed_gate'] else 'FAIL'}")
        for r in report["results"]:
            flags = []
            if not r["intent_ok"]:
                flags.append(f"intent {r['intent_got']}!={r['intent_expected']}")
            if r.get("routing_ok") is False:
                flags.append(f"route {r['tables_got']}!={r['tables_expected']}")
            if r.get("generation_ok") is False:
                flags.append(f"gen:{r['generation_detail']}")
            if r.get("faithful") is False:
                flags.append("unfaithful")
            if r.get("no_sql_ok") is False:
                flags.append("leaked SQL")
            if flags:
                print(f"  ✗ {r['id']}: {'; '.join(flags)}")
    return 0 if s["passed_gate"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
