"""Re-export of the shared reference engine (single source of truth).

The implementation lives in app.evals.reference so the pytest suite and the
`run_evals` CI command stay byte-for-byte consistent.
"""

from __future__ import annotations

from app.evals.reference import (
    GoldenCase,
    cases_by_intent,
    load_golden,
    reference_scalar,
    run_reference_sql,
)

__all__ = [
    "GoldenCase",
    "cases_by_intent",
    "load_golden",
    "reference_scalar",
    "run_reference_sql",
]
