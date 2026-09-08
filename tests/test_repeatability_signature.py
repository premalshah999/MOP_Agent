from __future__ import annotations

from app.evals.repeatability import _normalized_rows, _signature


def test_repeatability_signature_ignores_aliases_and_machine_epsilon() -> None:
    left = [{"state": "NEVADA", "sub_out_pct": 0.10029186109466055}]
    right = [{"state": "NEVADA", "sub_contract_out_pct": 0.10029186109466048}]
    assert _normalized_rows(left) == _normalized_rows(right)


def test_repeatability_signature_preserves_value_sign() -> None:
    positive = [{"county": "nassau", "gap": 7205284000}]
    negative = [{"county": "nassau", "Net_Position": -7205284000}]
    assert _normalized_rows(positive) != _normalized_rows(negative)


def test_repeatability_signature_deduplicates_identical_sample_counts() -> None:
    shared = [{"hs_corr": 0.5, "bachelor_corr": 0.7, "sample_size": 52}]
    repeated = [
        {
            "hs_corr": 0.5,
            "bachelor_corr": 0.7,
            "hs_sample_size": 52,
            "bachelor_sample_size": 52,
        }
    ]
    assert _normalized_rows(shared) == _normalized_rows(repeated)


def test_repeatability_signature_preserves_different_pair_counts() -> None:
    shared = [{"hs_corr": 0.5, "bachelor_corr": 0.7, "sample_size": 52}]
    pairwise = [
        {
            "hs_corr": 0.5,
            "bachelor_corr": 0.7,
            "hs_sample_size": 52,
            "bachelor_sample_size": 51,
        }
    ]
    assert _normalized_rows(shared) != _normalized_rows(pairwise)


def test_scalar_signature_ignores_an_optional_focus_label() -> None:
    base = {
        "resolution": "answered",
        "contract": {"tables": ["state_flow"], "operation": "aggregate"},
        "resultPackage": {
            "analysis_contract": {
                "tables": ["state_flow"],
                "metric_columns": ["subaward_amount_year"],
                "operation": "aggregate",
                "statistic": "sum",
                "result_scope": "single",
                "output_dimensions": [],
            }
        },
    }
    unlabeled = {**base, "data": [{"total": 26962782666.9}]}
    labeled = {
        **base,
        "data": [{"destination": "Maryland", "total": 26962782666.9}],
    }
    assert _signature(unlabeled) == _signature(labeled)


def test_signature_ignores_base_metric_formula_display_variants() -> None:
    base = {
        "resolution": "answered",
        "contract": {"tables": ["contract_state"], "operation": "aggregate"},
        "data": [{"total": 1970717}],
    }
    omitted = {
        **base,
        "resultPackage": {
            "analysis_contract": {
                "tables": ["contract_state"],
                "metric_columns": ["Employees"],
                "operation": "aggregate",
                "statistic": "sum",
                "result_scope": "single",
                "formula": {"operator": "none", "operands": []},
            }
        },
    }
    explicit = {
        **base,
        "resultPackage": {
            "analysis_contract": {
                "tables": ["contract_state"],
                "metric_columns": ["Employees"],
                "operation": "aggregate",
                "statistic": "sum",
                "result_scope": "single",
                "formula": {
                    "operator": "identity",
                    "operands": ["Employees"],
                    "scale": 1.0,
                    "output_label": "Total Employees",
                },
            }
        },
    }

    assert _signature(omitted) == _signature(explicit)


def test_signature_preserves_derived_operand_order() -> None:
    def result(operands: list[str]) -> dict:
        return {
            "resolution": "answered",
            "contract": {"tables": ["gov_state"], "operation": "comparison"},
            "data": [{"state": "Alabama", "gap": -10}],
            "resultPackage": {
                "analysis_contract": {
                    "tables": ["gov_state"],
                    "metric_columns": ["Total_Assets", "Total_Liabilities"],
                    "operation": "comparison",
                    "statistic": "derived",
                    "result_scope": "single",
                    "output_dimensions": ["state"],
                    "formula": {
                        "operator": "subtract",
                        "operands": operands,
                        "scale": 1.0,
                        "output_label": "gap",
                    },
                }
            },
        }

    assert _signature(result(["Total_Assets", "Total_Liabilities"])) != _signature(
        result(["Total_Liabilities", "Total_Assets"])
    )


def test_signature_normalizes_equivalent_period_json_types() -> None:
    base = {
        "resolution": "answered",
        "contract": {"tables": ["contract_state"], "operation": "aggregate"},
        "data": [{"value": 10}],
    }

    def result(period: int | str) -> dict:
        return {
            **base,
            "resultPackage": {
                "analysis_contract": {
                    "tables": ["contract_state"],
                    "metric_columns": ["Employees"],
                    "operation": "aggregate",
                    "statistic": "sum",
                    "result_scope": "single",
                    "effective_period": period,
                    "period_by_table": {"contract_state": period},
                }
            },
        }

    assert _signature(result(2024)) == _signature(result("2024"))
