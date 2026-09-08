"""Summarize non-blocking LLM drift telemetry.

The report never reads prompt or response text. It compares hashes and provider
metadata for identical request contracts so sampling variance, prompt changes,
and provider-side model changes remain distinguishable.

Usage: python -m app.observability.drift_report [--json]
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any

from app.observability.logging import LLM_LOG_PATH


def _load(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _percentile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * fraction))
    return ordered[index]


def build_report(path: Path = LLM_LOG_PATH) -> dict[str, Any]:
    rows = _load(path)
    contracts: dict[str, list[dict[str, Any]]] = defaultdict(list)
    purposes: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        fingerprint = str(row.get("request_fingerprint") or "")
        if fingerprint:
            contracts[fingerprint].append(row)
        purposes[str(row.get("purpose") or "unspecified")].append(row)

    repeated = {key: group for key, group in contracts.items() if len(group) > 1}
    output_variants = {
        key: sorted({str(row.get("output_fingerprint") or "") for row in group})
        for key, group in repeated.items()
        if len({str(row.get("output_fingerprint") or "") for row in group}) > 1
    }
    backend_variants = {
        key: {
            "configured_models": sorted({str(row.get("configured_model") or "") for row in group}),
            "response_models": sorted({str(row.get("response_model") or "") for row in group}),
            "system_fingerprints": sorted(
                {str(row.get("system_fingerprint") or "") for row in group}
            ),
        }
        for key, group in repeated.items()
        if len(
            {
                (
                    str(row.get("response_model") or ""),
                    str(row.get("system_fingerprint") or ""),
                )
                for row in group
            }
        )
        > 1
    }

    by_purpose: dict[str, Any] = {}
    for purpose, group in sorted(purposes.items()):
        latencies = [
            int(row["latency_ms"])
            for row in group
            if isinstance(row.get("latency_ms"), (int, float))
        ]
        totals = [
            int((row.get("usage") or {}).get("total_tokens") or 0)
            for row in group
            if isinstance(row.get("usage"), dict)
        ]
        by_purpose[purpose] = {
            "calls": len(group),
            "configured_models": sorted({str(row.get("configured_model") or "") for row in group}),
            "response_models": sorted({str(row.get("response_model") or "") for row in group}),
            "system_fingerprints": sorted(
                {
                    str(row.get("system_fingerprint"))
                    for row in group
                    if row.get("system_fingerprint")
                }
            ),
            "latency_ms_p50": round(median(latencies)) if latencies else None,
            "latency_ms_p95": _percentile(latencies, 0.95),
            "total_tokens": sum(totals),
        }

    return {
        "summary": {
            "calls": len(rows),
            "unique_request_contracts": len(contracts),
            "repeated_request_contracts": len(repeated),
            "contracts_with_output_variance": len(output_variants),
            "contracts_with_backend_drift": len(backend_variants),
        },
        "by_purpose": by_purpose,
        "output_variants": output_variants,
        "backend_variants": backend_variants,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--path", type=Path, default=LLM_LOG_PATH)
    args = parser.parse_args()
    report = build_report(args.path)
    if args.json:
        print(json.dumps(report, indent=2))
        return 0
    summary = report["summary"]
    print("LLM drift report")
    print(
        f"  calls: {summary['calls']} | contracts: {summary['unique_request_contracts']} | "
        f"repeated: {summary['repeated_request_contracts']}"
    )
    print(
        f"  output variance: {summary['contracts_with_output_variance']} | "
        f"backend drift: {summary['contracts_with_backend_drift']}"
    )
    for purpose, item in report["by_purpose"].items():
        print(
            f"  {purpose}: calls={item['calls']} p50={item['latency_ms_p50']}ms "
            f"p95={item['latency_ms_p95']}ms tokens={item['total_tokens']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
