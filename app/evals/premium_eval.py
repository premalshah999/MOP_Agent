"""Unified release certification for the conversational data agent.

The premium gate is deliberately outside the production request path. It
combines independent-reference correctness, paraphrase invariance, safe
boundaries, multi-turn continuity, repeatability, reasoning quality, catalog
integrity, latency, and provider telemetry in one reproducible report.

Usage:
    python -m app.evals.premium_eval --profile smoke
    python -m app.evals.premium_eval --profile demo
    python -m app.evals.premium_eval --profile full
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Callable

import yaml  # type: ignore[import-untyped]

from app.core.pipeline import PIPELINE_VERSION, answer_question
from app.evals.reference import GoldenCase
from app.evals.repeatability import _signature
from app.evals.run_evals import _check_expectation
from app.llm.client import active_provider, provider_warnings
from app.observability.logging import LLM_LOG_PATH
from app.paths import MANIFEST_PATH, ROOT_DIR
from app.semantic.audit import build_semantic_coverage_audit
from app.semantic.registry import load_registry

PREMIUM_CASES_PATH = ROOT_DIR / "app" / "evals" / "premium_cases.yaml"
DEFAULT_REPORT_DIR = ROOT_DIR / "reports" / "premium"
Answerer = Callable[..., dict[str, Any]]


@dataclass(frozen=True)
class Profile:
    factual: bool
    repeatability: int
    conversations: tuple[str, ...]
    reasoning: bool


PROFILES = {
    "smoke": Profile(False, 0, (), False),
    "demo": Profile(True, 2, ("normal",), False),
    "full": Profile(True, 5, ("normal", "reasoning"), True),
}


def load_manifest(path: Path = PREMIUM_CASES_PATH) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text()) or {}
    if raw.get("version") != 1:
        raise ValueError("premium manifest version must be 1")
    groups = raw.get("invariance_groups") or []
    boundaries = raw.get("boundary_cases") or []
    ids = [str(item.get("id") or "") for item in [*groups, *boundaries]]
    if not ids or any(not value for value in ids) or len(ids) != len(set(ids)):
        raise ValueError("premium case IDs must be present and unique")
    for group in groups:
        questions = group.get("questions") or []
        if len(questions) < 2:
            raise ValueError(f"invariance group {group['id']} needs at least two questions")
        if any(not isinstance(question, str) or not question.strip() for question in questions):
            raise ValueError(f"invariance group {group['id']} has a non-text question")
        if group.get("intent") == "ANALYTICAL" and not group.get("reference_sql"):
            raise ValueError(f"analytical group {group['id']} needs independent reference SQL")
    for case in boundaries:
        if not isinstance(case.get("question"), str) or not case["question"].strip():
            raise ValueError(f"boundary case {case['id']} has a non-text question")
        if not case.get("accepted_intents") or not case.get("require_no_sql"):
            raise ValueError(f"boundary case {case['id']} must define a no-SQL contract")
    return raw


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(fraction * len(ordered)))
    return round(ordered[rank - 1], 1)


def latency_summary(values: list[float]) -> dict[str, float | int | None]:
    return {
        "runs": len(values),
        "p50_ms": round(median(values), 1) if values else None,
        "p95_ms": _percentile(values, 0.95),
        "max_ms": round(max(values), 1) if values else None,
    }


def _provider_failure(result: dict[str, Any] | None, error: Exception | None = None) -> bool:
    # Normal successful results include provider metadata, so scanning the
    # entire payload would label every semantic failure as infrastructure.
    if error is not None:
        blob = str(error).casefold()
    elif (result or {}).get("resolution") == "error":
        blob = " ".join(
            (
                str((result or {}).get("answer") or ""),
                json.dumps((result or {}).get("quality") or {}, default=str),
            )
        ).casefold()
    else:
        return False
    markers = (
        "api key",
        "billing",
        "connection",
        "provider",
        "quota",
        "rate limit",
        "timed out",
        "timeout",
        "unavailable",
        "429",
        "502",
        "503",
        "504",
    )
    return any(marker in blob for marker in markers)


def _visual_problems(result: dict[str, Any], expected: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    if expected.get("chart") and not (result.get("chart") or result.get("charts")):
        problems.append("expected a chart")
    if expected.get("map") and not (result.get("mapIntent") or {}).get("enabled"):
        problems.append("expected an enabled map")
    return problems


def _compact_diagnostic(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "resolution": result.get("resolution"),
        "sql": result.get("sql"),
        "answer_preview": str(result.get("answer") or "")[:800],
        "contract": result.get("contract"),
        "analysis_contract": (result.get("resultPackage") or {}).get("analysis_contract"),
        "quality": result.get("quality"),
    }


def _call(
    answerer: Answerer, question: str, **kwargs: Any
) -> tuple[dict[str, Any], float, Exception | None]:
    started = time.perf_counter()
    try:
        result = answerer(question, **kwargs)
        error = None
    except Exception as exc:  # evaluation must report the failure, not lose the artifact
        result = {}
        error = exc
    elapsed_ms = (time.perf_counter() - started) * 1000
    return result, elapsed_ms, error


def run_premium_cases(
    *,
    answerer: Answerer = answer_question,
    manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = manifest or load_manifest()
    latencies: list[float] = []
    provider_failures = 0
    product_failures = 0
    groups_report: list[dict[str, Any]] = []

    for group in manifest.get("invariance_groups") or []:
        variants: list[dict[str, Any]] = []
        signatures: list[dict[str, Any] | None] = []
        for index, question in enumerate(group["questions"], start=1):
            result, elapsed_ms, error = _call(answerer, str(question))
            latencies.append(elapsed_ms)
            problems: list[str] = []
            failure_kind: str | None = None
            if error:
                problems.append(f"pipeline exception: {error}")
            else:
                got_intent = (result.get("contract") or {}).get("contract_type")
                if got_intent != group["intent"]:
                    problems.append(f"intent={got_intent!r}, expected={group['intent']!r}")
                got_tables = set((result.get("contract") or {}).get("tables") or [])
                if got_tables != set(group.get("tables") or []):
                    problems.append(
                        f"tables={sorted(got_tables)}, expected={sorted(group.get('tables') or [])}"
                    )
                case = GoldenCase(
                    id=f"{group['id']}:{index}",
                    question=str(question),
                    intent=str(group["intent"]),
                    tables=list(group.get("tables") or []),
                    must_columns=list(group.get("must_columns") or []),
                    reference_sql=group.get("reference_sql"),
                    expect=dict(group.get("expect") or {}),
                )
                expectation_ok, detail = _check_expectation(case, result)
                if result.get("resolution") != "answered":
                    problems.append(f"resolution={result.get('resolution')!r}")
                if not result.get("sql"):
                    problems.append("no SQL was produced")
                if not expectation_ok:
                    problems.append(detail)
                problems.extend(_visual_problems(result, group.get("visual") or {}))
            signature = _signature(result) if result.get("resolution") == "answered" else None
            signatures.append(signature)
            if problems:
                failure_kind = "provider" if _provider_failure(result, error) else "product"
                if failure_kind == "provider":
                    provider_failures += 1
                else:
                    product_failures += 1
            row = {
                "question": question,
                "passed": not problems,
                "latency_ms": round(elapsed_ms, 1),
                "problems": problems,
                "failure_kind": failure_kind,
            }
            if problems:
                row["diagnostic"] = _compact_diagnostic(result)
            variants.append(row)

        comparable = [signature for signature in signatures if signature is not None]
        invariant = len(comparable) == len(signatures) and all(
            signature == comparable[0] for signature in comparable[1:]
        )
        # Variant failures are already counted above. A separate product issue
        # exists only when every variant is individually correct but their
        # semantic/evidence contracts still differ.
        if not invariant and all(item["passed"] for item in variants):
            product_failures += 1
        groups_report.append(
            {
                "id": group["id"],
                "category": group.get("category"),
                "passed": all(item["passed"] for item in variants) and invariant,
                "semantic_invariance": invariant,
                "variants": variants,
                **({"signature_diagnostics": signatures} if not invariant else {}),
            }
        )

    boundaries_report: list[dict[str, Any]] = []
    for case in manifest.get("boundary_cases") or []:
        result, elapsed_ms, error = _call(answerer, str(case["question"]))
        latencies.append(elapsed_ms)
        got_intent = (result.get("contract") or {}).get("contract_type")
        problems: list[str] = []
        if error:
            problems.append(f"pipeline exception: {error}")
        if got_intent not in set(case["accepted_intents"]):
            problems.append(f"intent={got_intent!r}, accepted={case['accepted_intents']}")
        if case.get("require_no_sql") and result.get("sql"):
            problems.append("unsafe SQL was produced")
        if result.get("resolution") == "answered":
            problems.append("boundary request was incorrectly marked answered")
        failure_kind = None
        if problems:
            failure_kind = "provider" if _provider_failure(result, error) else "product"
            if failure_kind == "provider":
                provider_failures += 1
            else:
                product_failures += 1
        boundaries_report.append(
            {
                "id": case["id"],
                "category": case.get("category"),
                "question": case["question"],
                "passed": not problems,
                "latency_ms": round(elapsed_ms, 1),
                "problems": problems,
                "failure_kind": failure_kind,
                **({"diagnostic": _compact_diagnostic(result)} if problems else {}),
            }
        )

    timing = latency_summary(latencies)
    budget = manifest.get("latency_budget") or {}
    latency_passed = bool(
        timing["p95_ms"] is not None
        and timing["max_ms"] is not None
        and timing["p95_ms"] <= float(budget.get("p95_ms", math.inf))
        and timing["max_ms"] <= float(budget.get("max_ms", math.inf))
    )
    correctness_passed = all(item["passed"] for item in groups_report)
    safety_passed = all(item["passed"] for item in boundaries_report)
    return {
        "passed": correctness_passed and safety_passed and latency_passed,
        "summary": {
            "groups": len(groups_report),
            "variants": sum(len(item["variants"]) for item in groups_report),
            "invariant_groups": sum(item["semantic_invariance"] for item in groups_report),
            "boundary_cases": len(boundaries_report),
            "correctness_passed": correctness_passed,
            "safety_passed": safety_passed,
            "latency_passed": latency_passed,
            "provider_failure_count": provider_failures,
            "product_failure_count": product_failures,
            "latency": timing,
            "latency_budget": budget,
        },
        "groups": groups_report,
        "boundaries": boundaries_report,
    }


def _component(
    name: str, fn: Callable[[], dict[str, Any]], passed: Callable[[dict[str, Any]], bool]
) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        report = fn()
        ok = passed(report)
        error = None
    except Exception as exc:
        report = {}
        ok = False
        error = str(exc)
    return {
        "name": name,
        "passed": ok,
        "duration_seconds": round(time.perf_counter() - started, 2),
        "error": error,
        "report": report,
    }


def manifest_for_profile(profile: str, manifest: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a representative subset for smoke; demo/full retain every probe."""

    source = manifest or load_manifest()
    if profile != "smoke":
        return source
    return {
        **source,
        "invariance_groups": list(source.get("invariance_groups") or [])[:3],
        "boundary_cases": list(source.get("boundary_cases") or [])[:3],
    }


def _git_metadata() -> dict[str, Any]:
    def command(*args: str) -> str:
        try:
            return subprocess.run(
                ["git", *args],
                cwd=ROOT_DIR,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        except Exception:
            return "unknown"

    return {
        "commit": command("rev-parse", "--short=12", "HEAD"),
        "branch": command("branch", "--show-current"),
        "dirty": bool(command("status", "--porcelain") not in {"", "unknown"}),
    }


def _file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def _telemetry_rows() -> list[dict[str, Any]]:
    if not LLM_LOG_PATH.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in LLM_LOG_PATH.read_text().splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _telemetry_delta(before_calls: int) -> dict[str, Any]:
    """Aggregate only calls made by this sequential certification run."""

    rows = _telemetry_rows()[before_calls:]
    purposes: dict[str, list[dict[str, Any]]] = {}
    contracts: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        purposes.setdefault(str(row.get("purpose") or "unspecified"), []).append(row)
        fingerprint = str(row.get("request_fingerprint") or "")
        if fingerprint:
            contracts.setdefault(fingerprint, []).append(row)
    repeated = [group for group in contracts.values() if len(group) > 1]
    backend_drift = sum(
        len(
            {
                (str(row.get("response_model") or ""), str(row.get("system_fingerprint") or ""))
                for row in group
            }
        )
        > 1
        for group in repeated
    )

    def tokens(group: list[dict[str, Any]]) -> int:
        return sum(int((row.get("usage") or {}).get("total_tokens") or 0) for row in group)

    by_purpose: dict[str, Any] = {}
    for purpose, group in sorted(purposes.items()):
        latencies = [
            float(row["latency_ms"])
            for row in group
            if isinstance(row.get("latency_ms"), (int, float))
        ]
        by_purpose[purpose] = {
            "calls": len(group),
            "total_tokens": tokens(group),
            "latency_ms_p50": latency_summary(latencies)["p50_ms"],
            "latency_ms_p95": latency_summary(latencies)["p95_ms"],
            "response_models": sorted({str(row.get("response_model") or "") for row in group}),
        }
    return {
        "summary": {
            "calls_during_suite": len(rows),
            "total_tokens": tokens(rows),
            "unique_request_contracts": len(contracts),
            "repeated_request_contracts": len(repeated),
            "contracts_with_backend_drift": backend_drift,
        },
        "by_purpose": by_purpose,
    }


def run_release(profile: str = "demo") -> dict[str, Any]:
    selected = PROFILES[profile]
    before_calls = len(_telemetry_rows())
    components: list[dict[str, Any]] = []
    components.append(
        _component(
            "semantic_catalog",
            build_semantic_coverage_audit,
            lambda value: value["summary"]["critical_issue_count"] == 0,
        )
    )
    components.append(
        _component(
            "premium_probes",
            lambda: run_premium_cases(manifest=manifest_for_profile(profile)),
            lambda value: bool(value["passed"]),
        )
    )
    if selected.factual:
        from app.evals.reference import load_golden, load_holdout
        from app.evals.run_evals import run_case_evals

        components.append(
            _component(
                "independent_factual",
                lambda: run_case_evals([*load_golden(), *load_holdout()], "both"),
                lambda value: bool(value["summary"]["passed_gate"]),
            )
        )
    if selected.repeatability:
        from app.evals.repeatability import run as run_repeatability

        components.append(
            _component(
                "same_query_repeatability",
                lambda: run_repeatability(selected.repeatability),
                lambda value: bool(value["passed"]),
            )
        )
    for mode in selected.conversations:
        from app.evals.conversation_eval import run as run_conversations

        components.append(
            _component(
                f"conversation_{mode}",
                lambda mode=mode: run_conversations(mode),
                lambda value: bool(value["passed"]),
            )
        )
    if selected.reasoning:
        from app.evals.reasoning_eval import run_reasoning_evals

        components.append(
            _component(
                "complex_reasoning",
                run_reasoning_evals,
                lambda value: bool(value["summary"]["gate"]),
            )
        )

    passed = all(component["passed"] for component in components)
    failed = [component["name"] for component in components if not component["passed"]]
    return {
        "verdict": "READY" if passed else "NOT READY",
        "passed": passed,
        "profile": profile,
        "generated_at": datetime.now(UTC).isoformat(),
        "identity": {
            "pipeline_version": PIPELINE_VERSION,
            "semantic_registry_version": load_registry().version,
            "catalog_fingerprint": _file_hash(MANIFEST_PATH),
            "evaluation_fingerprint": _file_hash(PREMIUM_CASES_PATH),
            "provider": active_provider(),
            "provider_warnings": provider_warnings(),
            "git": _git_metadata(),
        },
        "summary": {
            "components": len(components),
            "passed_components": sum(component["passed"] for component in components),
            "failed_components": failed,
            "duration_seconds": round(sum(c["duration_seconds"] for c in components), 2),
        },
        "components": components,
        "telemetry": _telemetry_delta(int(before_calls)),
    }


def report_to_markdown(report: dict[str, Any]) -> str:
    identity = report["identity"]
    lines = [
        "# Premium Demo Readiness Report",
        "",
        f"## Verdict: {report['verdict']}",
        "",
        f"- Profile: `{report['profile']}`",
        f"- Generated: {report['generated_at']}",
        f"- Pipeline: `{identity['pipeline_version']}`",
        f"- Semantic registry: `{identity['semantic_registry_version']}`",
        f"- Provider/model: `{identity['provider']['name']}` / `{identity['provider']['model']}`",
        f"- Git commit: `{identity['git']['commit']}` ({'dirty' if identity['git']['dirty'] else 'clean'})",
        f"- Duration: {report['summary']['duration_seconds']} seconds",
        "",
        "## Release Gates",
        "",
        "| Gate | Result | Duration | Key result |",
        "|---|---:|---:|---|",
    ]
    for component in report["components"]:
        name = component["name"]
        result = component.get("report") or {}
        detail = ""
        if name == "premium_probes" and result:
            summary = result["summary"]
            detail = (
                f"{summary['invariant_groups']}/{summary['groups']} invariant groups; "
                f"{summary['boundary_cases']} boundary probes; "
                f"p95 {summary['latency']['p95_ms']} ms"
            )
        elif name == "semantic_catalog" and result:
            summary = result["summary"]
            detail = (
                f"{summary['semantically_certified_dataset_count']}/"
                f"{summary['registered_dataset_count']} certified datasets"
            )
        elif name == "independent_factual" and result:
            summary = result["summary"]
            detail = f"{summary['total']} independently referenced cases"
        elif name == "same_query_repeatability" and result:
            summary = result["summary"]
            detail = f"{summary['stable_questions']}/{summary['questions']} stable questions"
        elif name.startswith("conversation_") and result:
            turns = [
                turn
                for conversation in result["conversations"].values()
                for turn in conversation["turns"]
            ]
            detail = f"{sum(turn['passed'] for turn in turns)}/{len(turns)} turns"
        elif name == "complex_reasoning" and result:
            summary = result["summary"]
            detail = f"{summary['passed']}/{summary['tasks']} tasks at >=9/10"
        if component.get("error"):
            detail = f"error: {component['error']}"
        lines.append(
            f"| {name.replace('_', ' ').title()} | "
            f"{'PASS' if component['passed'] else 'FAIL'} | "
            f"{component['duration_seconds']}s | {detail} |"
        )
    warnings = identity.get("provider_warnings") or []
    if warnings:
        lines.extend(["", "## Provider Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)
    if report["summary"]["failed_components"]:
        lines.extend(["", "## Blocking Failures", ""])
        lines.extend(f"- {name}" for name in report["summary"]["failed_components"])
    return "\n".join(lines) + "\n"


def write_report(
    report: dict[str, Any], output_dir: Path = DEFAULT_REPORT_DIR
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "latest.json"
    markdown_path = output_dir / "latest.md"
    json_path.write_text(json.dumps(report, indent=2, default=str) + "\n")
    markdown_path.write_text(report_to_markdown(report))
    return json_path, markdown_path


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except Exception:
        pass
    parser = argparse.ArgumentParser(description="Run unified premium demo/release gates.")
    parser.add_argument("--profile", choices=tuple(PROFILES), default="demo")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--json", action="store_true", help="print the complete JSON report")
    parser.add_argument(
        "--no-write", action="store_true", help="do not write latest.json/latest.md"
    )
    args = parser.parse_args()
    report = run_release(args.profile)
    paths = None if args.no_write else write_report(report, args.output_dir)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(report_to_markdown(report), end="")
        if paths:
            print(f"Artifacts: {paths[0]} and {paths[1]}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
