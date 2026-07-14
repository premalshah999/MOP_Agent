"""Stage 3 — Retrieval / grounding pack completeness.

The grounding pack must contain, for the routed tables: the column schema, the
critical-warning that applies, and the correctly-cased resolved filter values.
This stage is deterministic (no LLM) so it runs without a key.
"""

from __future__ import annotations

import pytest

from tests.ground_truth import cases_by_intent

grounding_mod = pytest.importorskip("app.core.grounding", reason="Stage 3 not implemented yet")


def _pack_text(question: str, tables: list[str]) -> str:
    pack = grounding_mod.build_grounding(question, tables)
    return pack if isinstance(pack, str) else pack.get("text", str(pack))


def test_grounding_includes_schema_and_must_columns() -> None:
    missing: list[str] = []
    for case in cases_by_intent("ANALYTICAL"):
        text = _pack_text(case.question, case.tables)
        for table in case.tables:
            if table not in text:
                missing.append(f"{case.id}: table {table} absent from grounding")
        for col in case.must_columns:
            if col not in text:
                missing.append(f"{case.id}: column {col!r} absent from grounding")
    assert not missing, "\n".join(missing)


def test_grounding_surfaces_critical_warnings() -> None:
    """Trap cases must carry their guard rail into the grounding pack."""
    checks = {
        "g03": "year",        # gov: no year filter
        "g10": "per",         # per-capita / no year
        "g18": "UPPERCASE",   # spending_state_agency state casing
        "g22": "year",        # gov single snapshot
        "g23": "'2024'",      # contract year is a string
    }
    by_id = {c.id: c for c in cases_by_intent("ANALYTICAL")}
    for cid, needle in checks.items():
        case = by_id[cid]
        text = _pack_text(case.question, case.tables).lower()
        assert needle.lower() in text, f"{cid}: expected critical-warning hint {needle!r}"


def test_grounding_resolves_state_casing() -> None:
    """When the question names an entity, the exact stored value must appear."""
    by_id = {c.id: c for c in cases_by_intent("ANALYTICAL")}
    text = _pack_text(by_id["g01"].question, by_id["g01"].tables)
    assert "MARYLAND" in text, "contract_county uses UPPERCASE state values"
    text2 = _pack_text(by_id["g18"].question, by_id["g18"].tables)
    assert "Department of Defense" in text2, "DoD must resolve to canonical agency"
