from app.evals.reasoning_eval import _presentation_evidence_aligned
from app.evals.reference import GoldenCase
from app.evals.run_evals import _check_expectation


def test_county_flow_full_label_satisfies_same_side_county_requirement() -> None:
    case = GoldenCase(
        id="flow-label-equivalence",
        question="outflow from Montgomery County, Maryland",
        intent="ANALYTICAL",
        must_columns=["rcpt_cty_name", "subaward_amount"],
    )
    result = {
        "sql": (
            "SELECT SUM(subaward_amount) FROM mart_county_flow "
            "WHERE rcpt_full_name = 'Montgomery County, Maryland'"
        ),
        "data": [{"total": 1.0}],
        "answer": "$1",
        "resultPackage": {"analysis_contract": {"metric_columns": ["subaward_amount"]}},
    }

    assert _check_expectation(case, result) == (True, "ok")


def test_county_flow_opposite_side_does_not_satisfy_required_dimension() -> None:
    case = GoldenCase(
        id="flow-direction-not-equivalent",
        question="outflow from Montgomery County, Maryland",
        intent="ANALYTICAL",
        must_columns=["rcpt_cty_name", "subaward_amount"],
    )
    result = {
        "sql": (
            "SELECT SUM(subaward_amount) FROM mart_county_flow "
            "WHERE subawardee_full_name = 'Montgomery County, Maryland'"
        ),
        "data": [{"total": 1.0}],
        "answer": "$1",
        "resultPackage": {"analysis_contract": {"metric_columns": ["subaward_amount"]}},
    }

    passed, reason = _check_expectation(case, result)
    assert passed is False
    assert "rcpt_cty_name" in reason


def test_reasoning_eval_rejects_answer_data_from_different_evidence() -> None:
    result = {
        "resolution": "answered",
        "sql": "SELECT correct",
        "data": [{"district": "AL-05", "rate": 3350.76}],
        "resultPackage": {
            "primary_evidence_id": "E1",
            "cited_tool_results": [
                {
                    "evidence_id": "E1",
                    "name": "run_sql",
                    "result": {
                        "sql": "SELECT alaska_sample",
                        "rows": [{"district": "AK-00", "rate": 200}],
                    },
                }
            ],
        },
    }

    assert _presentation_evidence_aligned(result) is False


def test_reasoning_eval_accepts_matching_primary_evidence() -> None:
    rows = [{"district": "AL-05", "rate": 3350.76}]
    result = {
        "resolution": "answered",
        "sql": "SELECT ranked_rates",
        "data": rows,
        "resultPackage": {
            "primary_evidence_id": "E1",
            "cited_tool_results": [
                {
                    "evidence_id": "E1",
                    "name": "run_sql",
                    "result": {"sql": "SELECT ranked_rates", "rows": rows},
                }
            ],
        },
    }

    assert _presentation_evidence_aligned(result) is True
