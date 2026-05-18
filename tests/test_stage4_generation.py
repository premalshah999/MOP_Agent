"""Stage 4 — Generation (SQL + grounded answer).

For every ANALYTICAL golden case: the pipeline must produce executable SQL whose
result matches the independent reference query, and (when flagged) an answer that
an LLM judge rates faithful to the returned rows. Non-analytical cases must NOT
emit SQL.
"""

from __future__ import annotations

import json
import re

import pytest

from tests.conftest import llm_available
from tests.ground_truth import (
    cases_by_intent,
    load_golden,
    reference_scalar,
    run_reference_sql,
)

orch = pytest.importorskip("app.core.orchestrator", reason="Pipeline not rebuilt yet")

pytestmark = pytest.mark.skipif(
    not getattr(orch, "PIPELINE_READY", False),
    reason="LLM-grounded pipeline not wired yet (Phase 2)",
)


def _numbers(text: str) -> list[float]:
    out: list[float] = []
    for tok in re.findall(r"-?\d[\d,]*\.?\d*", text or ""):
        try:
            out.append(float(tok.replace(",", "")))
        except ValueError:
            pass
    return out


def _result_blob(result: dict) -> str:
    return (result.get("answer") or "") + " " + json.dumps(result.get("data") or [], default=str)


def _assert_expectation(case, result: dict) -> None:
    data = result.get("data") or []
    expect = case.expect
    if "row_count" in expect:
        assert len(data) == expect["row_count"], f"{case.id}: rows {len(data)} != {expect['row_count']}"
    if "min_rows" in expect:
        assert len(data) >= expect["min_rows"], f"{case.id}: rows {len(data)} < {expect['min_rows']}"
    if expect.get("top_label"):
        ref = run_reference_sql(case.reference_sql)
        label = str(list(ref[0].values())[0]).lower()
        assert label in _result_blob(result).lower(), (
            f"{case.id}: reference top label {label!r} not found in answer/data"
        )
    if expect.get("scalar"):
        ref = float(reference_scalar(case.reference_sql))
        tol = float(expect.get("rel_tol", 0.01))
        candidates = _numbers(_result_blob(result)) + [
            float(k["value"]) for k in result.get("key_numbers", [])
            if str(k.get("value")).replace(".", "", 1).lstrip("-").isdigit()
        ]
        ok = any(abs(c - ref) <= tol * max(1.0, abs(ref)) for c in candidates)
        assert ok, f"{case.id}: no value ≈ reference {ref} (rel_tol {tol}); saw {candidates[:8]}"
    for needle in expect.get("answer_contains", []):
        assert needle.lower() in (result.get("answer") or "").lower(), (
            f"{case.id}: answer missing {needle!r}"
        )


@pytest.mark.skipif(not llm_available(), reason="no LLM key for generation")
@pytest.mark.parametrize("case", cases_by_intent("ANALYTICAL"), ids=lambda c: c.id)
def test_analytical_generation(case) -> None:
    result = orch.answer_question(case.question)
    assert result.get("resolution") == "answered", f"{case.id}: resolution={result.get('resolution')}"
    assert result.get("sql"), f"{case.id}: no SQL produced"
    _assert_expectation(case, result)

    if case.faithfulness:
        judge = pytest.importorskip("app.evals.faithfulness", reason="judge not implemented")
        verdict = judge.judge_faithfulness(
            case.question, result.get("answer", ""), result.get("data") or [], result.get("sql") or ""
        )
        assert verdict.get("faithful") is True, f"{case.id}: unfaithful — {verdict.get('reason')}"


@pytest.mark.skipif(not llm_available(), reason="no LLM key for generation")
@pytest.mark.parametrize(
    "case",
    [c for c in load_golden() if c.no_sql],
    ids=lambda c: c.id,
)
def test_non_analytical_emits_no_sql(case) -> None:
    result = orch.answer_question(case.question)
    assert not result.get("sql"), f"{case.id} ({case.intent}) wrongly produced SQL"
