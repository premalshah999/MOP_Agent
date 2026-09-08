from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.evals.premium_eval import (
    _telemetry_delta,
    latency_summary,
    load_manifest,
    manifest_for_profile,
    report_to_markdown,
    run_premium_cases,
    write_report,
)
from app.evals.reference import run_reference_sql


def _answered(*, value: float = 10, variant: str = "same") -> dict[str, Any]:
    return {
        "answer": f"The verified value is {value}.",
        "sql": 'SELECT SUM("Metric") AS value FROM mart_example',
        "data": [{"value": value}],
        "resolution": "answered",
        "chart": {"mark": "bar"},
        "charts": [{"spec": {"mark": "bar"}}],
        "mapIntent": {"enabled": True},
        "contract": {
            "contract_type": "ANALYTICAL",
            "tables": ["example"],
            "operation": "aggregate",
        },
        "resultPackage": {
            "analysis_contract": {
                "tables": ["example"],
                "metric_columns": ["Metric"],
                "operation": "aggregate",
                "statistic": "sum",
                "formula": {"operator": "identity"},
                "result_scope": "single",
                "result_unit": variant,
                "output_dimensions": [],
            }
        },
        "quality": {"status": "ok", "warnings": []},
    }


def _safe(intent: str = "UNANSWERABLE") -> dict[str, Any]:
    return {
        "answer": "That is not available from the catalog.",
        "sql": None,
        "data": [],
        "resolution": "unsupported",
        "contract": {"contract_type": intent, "tables": []},
    }


def _manifest() -> dict[str, Any]:
    return {
        "version": 1,
        "latency_budget": {"p95_ms": 1000, "max_ms": 1000},
        "invariance_groups": [
            {
                "id": "same_meaning",
                "intent": "ANALYTICAL",
                "tables": ["example"],
                "must_columns": ["Metric"],
                "reference_sql": "SELECT 10 AS value",
                "expect": {"scalar": True, "rel_tol": 0},
                "visual": {"chart": True, "map": True},
                "questions": ["first wording", "second wording"],
            }
        ],
        "boundary_cases": [
            {
                "id": "unsafe",
                "question": "unsafe request",
                "accepted_intents": ["UNANSWERABLE", "OUT_OF_SCOPE"],
                "require_no_sql": True,
            }
        ],
    }


def test_premium_manifest_has_valid_independent_reference_queries() -> None:
    manifest = load_manifest()
    assert len(manifest["invariance_groups"]) >= 5
    assert len(manifest["boundary_cases"]) >= 5
    for group in manifest["invariance_groups"]:
        assert run_reference_sql(group["reference_sql"])


def test_smoke_manifest_is_smaller_but_keeps_invariance() -> None:
    full = load_manifest()
    smoke = manifest_for_profile("smoke", full)
    assert 0 < len(smoke["invariance_groups"]) < len(full["invariance_groups"])
    assert all(len(group["questions"]) >= 2 for group in smoke["invariance_groups"])
    assert len(smoke["boundary_cases"]) < len(full["boundary_cases"])


def test_premium_grader_accepts_grounded_invariant_and_safe_results() -> None:
    def answerer(question: str) -> dict[str, Any]:
        return _safe() if question == "unsafe request" else _answered()

    report = run_premium_cases(answerer=answerer, manifest=_manifest())
    assert report["passed"]
    assert report["summary"]["correctness_passed"]
    assert report["summary"]["safety_passed"]
    assert report["summary"]["invariant_groups"] == 1


def test_premium_grader_detects_semantic_drift_between_paraphrases() -> None:
    def answerer(question: str) -> dict[str, Any]:
        if question == "unsafe request":
            return _safe()
        return _answered(variant=question)

    report = run_premium_cases(answerer=answerer, manifest=_manifest())
    assert not report["passed"]
    assert not report["groups"][0]["semantic_invariance"]
    assert report["summary"]["product_failure_count"] == 1


def test_premium_grader_detects_unsafe_sql() -> None:
    def answerer(question: str) -> dict[str, Any]:
        if question != "unsafe request":
            return _answered()
        unsafe = _safe("OUT_OF_SCOPE")
        unsafe["sql"] = "DROP TABLE mart_example"
        return unsafe

    report = run_premium_cases(answerer=answerer, manifest=_manifest())
    assert not report["summary"]["safety_passed"]
    assert "unsafe SQL was produced" in report["boundaries"][0]["problems"]


def test_latency_summary_uses_nearest_rank_percentile() -> None:
    summary = latency_summary([10, 20, 30, 40, 50])
    assert summary == {"runs": 5, "p50_ms": 30, "p95_ms": 50, "max_ms": 50}


def test_stakeholder_report_is_readable_and_writable(tmp_path: Path) -> None:
    report = {
        "verdict": "READY",
        "passed": True,
        "profile": "smoke",
        "generated_at": "2026-08-31T00:00:00+00:00",
        "identity": {
            "pipeline_version": "test-v1",
            "semantic_registry_version": "registry-v1",
            "provider": {"name": "deepseek", "model": "fixed-model"},
            "provider_warnings": [],
            "git": {"commit": "abc123", "dirty": False},
        },
        "summary": {
            "duration_seconds": 1.2,
            "failed_components": [],
        },
        "components": [
            {
                "name": "premium_probes",
                "passed": True,
                "duration_seconds": 1.2,
                "error": None,
                "report": {
                    "summary": {
                        "invariant_groups": 1,
                        "groups": 1,
                        "boundary_cases": 1,
                        "latency": {"p95_ms": 20},
                    }
                },
            }
        ],
    }
    text = report_to_markdown(report)
    assert "## Verdict: READY" in text
    assert "1/1 invariant groups" in text
    json_path, markdown_path = write_report(report, tmp_path)
    assert json_path.is_file() and markdown_path.read_text() == text


def test_telemetry_is_scoped_to_calls_after_snapshot(tmp_path: Path, monkeypatch) -> None:
    log_path = tmp_path / "llm.jsonl"
    rows = [
        {
            "purpose": "old",
            "request_fingerprint": "old",
            "response_model": "model-a",
            "usage": {"total_tokens": 999},
            "latency_ms": 999,
        },
        {
            "purpose": "planner",
            "request_fingerprint": "same",
            "response_model": "model-a",
            "system_fingerprint": "fp-a",
            "usage": {"total_tokens": 10},
            "latency_ms": 20,
        },
        {
            "purpose": "planner",
            "request_fingerprint": "same",
            "response_model": "model-a",
            "system_fingerprint": "fp-a",
            "usage": {"total_tokens": 15},
            "latency_ms": 30,
        },
    ]
    log_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    monkeypatch.setattr("app.evals.premium_eval.LLM_LOG_PATH", log_path)
    telemetry = _telemetry_delta(1)
    assert telemetry["summary"] == {
        "calls_during_suite": 2,
        "total_tokens": 25,
        "unique_request_contracts": 1,
        "repeated_request_contracts": 1,
        "contracts_with_backend_drift": 0,
    }
    assert telemetry["by_purpose"]["planner"]["calls"] == 2
