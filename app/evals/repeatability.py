"""Live gate: identical questions must produce identical evidence.

Natural-language phrasing may vary, but the contract, resolution, and returned
rows must not.  This directly detects the production failure mode where the
same query silently changed tables, years, filters, or computation.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from typing import Any

from app.core.pipeline import answer_question

QUESTIONS = [
    "how many employees does contract_static_state dataset have?",
    "how many grant dollars did Maryland receive",
    "top 10 counties in Maryland by grants",
    "compare Maryland vs Virginia on grants",
    "subcontract inflow to Maryland",
    "What is the correlation between federal grant dollars and poverty rate across states?",
    "Which county in government finance is poorest in terms of assets minus liabilities?",
    "Which state has the highest negative difference in total assets and total liabilities?",
    "Which county in New York has the largest liabilities minus assets?",
    "How much did Prince George County borrow in 2023?",
    "How many unique counties are in the ACS county dataset in 2023?",
    "Provide correlation of median household income with all education variables.",
    "For the state of Colorado, correlate all loaded education measures with median household income and the loaded income bands.",
    "Rank the Wyoming counties by Black and Asian population.",
    "Express Sub-contract Out as a percentage of Federal Contracts for all counties of Nevada in 2024.",
    "epartment of defence biggest deals by state",
    "which congressional district receives the most subaward inflow",
]


_DIMENSION_KEYS = {
    "state",
    "county",
    "cd_118",
    "fips",
    "state_fips",
    "county_fips",
    "agency",
    "agency_name",
    "agency_code",
    "rcpt_state",
    "rcpt_state_name",
    "subawardee_state",
    "subawardee_state_name",
    "rcpt_cty",
    "rcpt_cty_name",
    "subawardee_cty",
    "subawardee_cty_name",
    "rcpt_cd_name",
    "subawardee_cd_name",
    "prime_awardee_stcd118",
    "subawardee_stcd118",
    "year",
    "act_dt_fis_yr",
}


def _normalized_number(value: int | float) -> int | float | str:
    number = float(value)
    if not math.isfinite(number):
        return str(number)
    if number.is_integer() and abs(number) < 2**53:
        return int(number)
    # Provider-generated SQL can change floating aggregation at machine epsilon
    # (for example 0.10029186109466055 vs ...048). Twelve significant digits
    # preserves analytical evidence while ignoring non-user-visible noise.
    return float(f"{number:.12g}")


def _normalized_rows(
    rows: list[dict[str, Any]],
    *,
    required_dimensions: set[str] | None = None,
    include_other_values: bool = True,
) -> str:
    # SQL is required to stabilize ranking ties, but sorting here makes the
    # signature insensitive to JSON key order and harmless DB serialization.
    normalized = []
    for row in rows:
        dimensions: dict[str, Any] = {}
        numeric_values: list[int | float | str] = []
        sample_values: set[int | float | str] = set()
        other_values: list[str] = []
        for key, value in row.items():
            normalized_key = re.sub(r"[^a-z0-9_]+", "_", str(key).casefold()).strip("_")
            if normalized_key in _DIMENSION_KEYS:
                if required_dimensions is None or normalized_key in required_dimensions:
                    dimensions[normalized_key] = value
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                normalized_value = _normalized_number(value)
                if re.search(
                    r"(?:^|_)(?:sample(?:_size)?|observations?|n_pairs)(?:_|$)", normalized_key
                ):
                    # A matrix may return one shared complete-case count or the
                    # same paired count beside every coefficient. Those shapes
                    # carry identical evidence. Unequal pair counts remain
                    # distinct and therefore still fail the repeatability gate.
                    sample_values.add(normalized_value)
                else:
                    numeric_values.append(normalized_value)
            elif value is not None and include_other_values:
                other_values.append(str(value))
        normalized.append(
            {
                "dimensions": dict(sorted(dimensions.items())),
                "numeric_values": sorted(numeric_values, key=str),
                "sample_values": sorted(sample_values, key=str),
                "other_values": sorted(other_values),
            }
        )
    return json.dumps(normalized, sort_keys=True, default=str, separators=(",", ":"))


def _normalized_formula(
    formula: Any,
    *,
    statistic: Any,
) -> dict[str, Any]:
    """Keep calculation meaning while discarding generated display wording."""

    raw = formula if isinstance(formula, dict) else {}
    operator = str(raw.get("operator") or "none").casefold()
    if operator in {"none", "identity"} and str(statistic or "").casefold() != "derived":
        # Base-metric selection is already represented by `metrics`. Providers
        # legitimately alternate between no formula and an explicit identity;
        # operands/output labels on either form do not change the calculation.
        return {"operator": "identity"}
    return {
        "operator": operator,
        "operands": [str(value) for value in (raw.get("operands") or [])],
        "scale": _normalized_number(raw.get("scale", 1.0)),
    }


def _normalized_predicate(predicate: Any) -> dict[str, Any]:
    raw = predicate if isinstance(predicate, dict) else {}
    operator = str(raw.get("operator") or "none").casefold()
    if operator == "none":
        return {"operator": "none"}
    return {
        "operator": operator,
        "operands": [str(value) for value in (raw.get("operands") or [])],
    }


def _normalized_period(value: Any) -> Any:
    """Normalize provider JSON typing without erasing period meaning."""

    if isinstance(value, dict):
        return {str(key): _normalized_period(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [_normalized_period(item) for item in value]
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(_normalized_number(value))
    if isinstance(value, str):
        return value.strip()
    return value


def _signature(result: dict[str, Any]) -> dict[str, Any]:
    contract = result.get("contract") or {}
    analysis = (result.get("resultPackage") or {}).get("analysis_contract") or {}
    scope = analysis.get("result_scope")
    operation = analysis.get("operation") or contract.get("operation")
    output_dimensions = {
        re.sub(r"[^a-z0-9_]+", "_", str(value).casefold()).strip("_")
        for value in (analysis.get("output_dimensions") or [])
    }
    dimension_sensitive = scope in {"top_n", "full", "grouped"} or operation in {
        "ranking",
        "breakdown",
        "comparison",
        "trend",
        "distribution",
    }
    # A fixed-focus lookup/aggregate may or may not repeat the already-grounded
    # label beside its one numeric result. That projection choice is cosmetic.
    # Ranked/comparative/grouped labels remain part of the evidence signature.
    signature_dimensions = output_dimensions if dimension_sensitive else set()
    required_dimensions = None if dimension_sensitive else signature_dimensions
    metrics = analysis.get("metric_columns") or (
        [contract.get("metric")] if contract.get("metric") else []
    )
    statistic = analysis.get("statistic")
    return {
        "resolution": result.get("resolution"),
        "tables": analysis.get("tables") or contract.get("tables"),
        "metrics": metrics,
        "operation": operation,
        "statistic": statistic,
        "formula": _normalized_formula(analysis.get("formula"), statistic=statistic),
        "predicate": _normalized_predicate(analysis.get("predicate")),
        "observation_grain": analysis.get("observation_grain"),
        "result_scope": scope,
        "result_unit": analysis.get("result_unit"),
        "year": _normalized_period(analysis.get("effective_period", contract.get("year"))),
        "period_by_table": _normalized_period(analysis.get("period_by_table")),
        "sort_direction": analysis.get("sort_direction", contract.get("sort_direction")),
        "sort_columns": analysis.get("sort_columns") or [],
        "top_k": analysis.get("top_k", contract.get("top_k")),
        "output_dimensions": sorted(signature_dimensions),
        "rows": _normalized_rows(
            result.get("data") or [],
            required_dimensions=required_dimensions,
            include_other_values=dimension_sensitive,
        ),
    }


def _diagnostic(result: dict[str, Any], signature: dict[str, Any]) -> dict[str, Any]:
    stages = (result.get("pipelineTrace") or {}).get("stages") or []
    return {
        "signature": signature,
        "sql": result.get("sql"),
        "answer_preview": str(result.get("answer") or "")[:800],
        "quality": result.get("quality"),
        "sql_generation": [
            stage.get("data") for stage in stages if stage.get("name") == "stage4_sql_generation"
        ],
    }


def run(repeats: int = 2) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    answered_runs = 0
    for question in QUESTIONS:
        runs = [answer_question(question) for _ in range(repeats)]
        signatures = [_signature(result) for result in runs]
        stable = all(signature == signatures[0] for signature in signatures[1:])
        resolutions = [result.get("resolution") for result in runs]
        answered_runs += sum(value == "answered" for value in resolutions)
        reasons: list[str] = []
        if not stable:
            reasons.append("semantic evidence changed across identical requests")
        if any(value != "answered" for value in resolutions):
            reasons.append("one or more runs did not answer")
        if reasons:
            failures.append(
                {
                    "question": question,
                    "reasons": reasons,
                    "runs": [
                        _diagnostic(result, signature)
                        for result, signature in zip(runs, signatures, strict=True)
                    ],
                }
            )
    return {
        "passed": not failures,
        "summary": {
            "questions": len(QUESTIONS),
            "repeats": repeats,
            "total_runs": len(QUESTIONS) * repeats,
            "stable_questions": len(QUESTIONS) - len(failures),
            "answered_runs": answered_runs,
        },
        "failures": failures,
    }


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except Exception:
        pass
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if not 2 <= args.repeats <= 20:
        parser.error("--repeats must be between 2 and 20")
    report = run(args.repeats)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        summary = report["summary"]
        print("Repeatability evaluation")
        print(f"  questions:        {summary['questions']}")
        print(f"  repetitions:      {summary['repeats']}")
        print(f"  answered runs:    {summary['answered_runs']}/{summary['total_runs']}")
        print(f"  stable questions: {summary['stable_questions']}/{summary['questions']}")
        print(f"  GATE: {'PASS' if report['passed'] else 'FAIL'}")
        for failure in report["failures"]:
            print(f"  ✗ {failure['question']}: {', '.join(failure['reasons'])}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
