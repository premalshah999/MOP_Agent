"""Stage 3 — Retrieval / grounding pack completeness.

The grounding pack must contain, for the routed tables: the column schema, the
critical-warning that applies, and the correctly-cased resolved filter values.
This stage is deterministic (no LLM) so it runs without a key.
"""

from __future__ import annotations

from app.core.grounding import build_grounding
from tests.ground_truth import cases_by_intent


def _pack_text(question: str, tables: list[str]) -> str:
    pack = build_grounding(question, tables)
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
        "g03": "year",  # gov: no year filter
        "g10": "per",  # per-capita / no year
        "g18": "UPPERCASE",  # spending_state_agency state casing
        "g22": "year",  # gov single snapshot
        "g23": "'2024'",  # contract year is a string
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


def test_grounding_obeys_planned_filter_dimensions() -> None:
    pack = build_grounding(
        "Which New York county has the largest liabilities minus assets?",
        ["gov_county"],
        filter_columns=["state"],
    )
    assert pack["resolved"]["gov_county"]["state"]["value"] == "new york"
    assert "county" not in pack["resolved"]["gov_county"]


def test_grounding_resolves_named_state_through_congressional_prefix() -> None:
    pack = build_grounding(
        "Maryland congressional districts by free cash flow",
        ["gov_congress"],
        filter_columns=[],
    )
    districts = pack["resolved"]["gov_congress"]["cd_118"]["values"]
    assert districts
    assert all(str(district).startswith("MD-") for district in districts)
    assert "RESOLVED FILTER VALUES" in pack["text"]


def test_grounding_recovers_exact_entity_in_non_output_dimension() -> None:
    pack = build_grounding(
        "epartment of defence biggest deals by state",
        ["spending_state_agency"],
        filter_columns=[],
        output_dimensions=["state"],
    )

    assert pack["resolved"]["spending_state_agency"]["agency"]["value"] == ("Department of Defense")


def test_grounding_does_not_turn_output_dimension_into_filter() -> None:
    pack = build_grounding(
        "Show Department of Defense among agencies by contracts",
        ["spending_state_agency"],
        filter_columns=[],
        output_dimensions=["agency"],
    )

    assert pack["resolved"] == {}


def test_flow_grounding_recovers_only_the_planned_direction() -> None:
    pack = build_grounding(
        "total subaward dollars flowing out of Montgomery County, Maryland in 2023",
        ["county_flow"],
        filter_columns=[],
        output_dimensions=[],
        flow_direction="outflow",
    )

    columns = set(pack["resolved"]["county_flow"])
    assert columns
    assert all(column.startswith("rcpt_") for column in columns)
