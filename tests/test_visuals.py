"""Deterministic checks for the visualization layer (no LLM)."""

from __future__ import annotations

from app.core.visuals import build_visuals


def _valid_spec(spec: dict) -> bool:
    return (
        isinstance(spec, dict)
        and "$schema" in spec
        and "vega-lite" in spec["$schema"]
        and isinstance(spec.get("data", {}).get("values"), list)
        and "mark" in spec
        and "encoding" in spec
    )


def test_ranking_emits_horizontal_bar_and_state_map() -> None:
    rows = [{"state": s, "Grants": v} for s, v in
            [("maryland", 9), ("virginia", 7), ("texas", 5), ("ohio", 3)]]
    v = build_visuals("top states by grants", {"tables": ["contract_state"], "geography_level": "state"}, {}, rows)
    assert v["charts"] and _valid_spec(v["charts"][0]["spec"])
    enc = v["charts"][0]["spec"]["encoding"]
    assert enc["y"]["field"] == "label" and enc["x"]["field"] == "value"
    assert v["map_intent"]["enabled"] and v["map_intent"]["level"] == "state"
    assert v["map_intent"]["metric"] == "Grants"


def test_comparison_emits_colored_bar_and_comparison_map() -> None:
    rows = [{"state": "maryland", "Grants": 9}, {"state": "virginia", "Grants": 7}]
    v = build_visuals("compare Maryland vs Virginia on grants",
                      {"tables": ["contract_state"], "geography_level": "state"}, {}, rows)
    assert _valid_spec(v["chart"])
    assert v["chart"]["encoding"]["color"]["field"] == "label"
    assert v["map_intent"]["mapType"] == "atlas-comparison"
    assert set(v["map_intent"]["comparisonIds"]) == {"maryland", "virginia"}


def test_trend_emits_line() -> None:
    rows = [{"Year": y, "financial_literacy": v} for y, v in
            [(2015, 0.5), (2018, 0.55), (2021, 0.6)]]
    v = build_visuals("trend of financial literacy by year",
                      {"tables": ["finra_state"], "geography_level": "state"}, {}, rows)
    assert v["chart"]["mark"]["type"] == "line"


def test_distribution_emits_histogram() -> None:
    rows = [{"state": f"s{i}", "Debt_Ratio": i / 10} for i in range(20)]
    v = build_visuals("what is the distribution of debt ratio",
                      {"tables": ["gov_state"], "geography_level": "state"}, {}, rows)
    assert v["chart"]["encoding"]["x"].get("bin")


def test_county_rows_with_focus_state_map() -> None:
    rows = [{"county": c, "state": "maryland", "Total_Assets": v}
            for c, v in [("montgomery", 9), ("howard", 7), ("frederick", 5)]]
    resolved = {"gov_county": {"state": {"value": "maryland", "score": 1.0}}}
    v = build_visuals("top counties in Maryland by assets",
                      {"tables": ["gov_county"], "geography_level": "county"}, resolved, rows)
    assert v["map_intent"]["enabled"] and v["map_intent"]["level"] == "county"
    assert v["map_intent"]["mapType"] == "single-state-ranked-subregions"
    assert v["map_intent"]["state"] == "Maryland"


def test_congress_level_detected() -> None:
    rows = [{"cd_118": d, "Free_Cash_Flow": v} for d, v in [("MD-05", 9), ("MD-08", 4)]]
    v = build_visuals("Maryland districts by free cash flow",
                      {"tables": ["gov_congress"], "geography_level": "congress"}, {}, rows)
    assert v["map_intent"]["level"] == "congress"


def test_non_geo_disables_map_but_keeps_chart() -> None:
    rows = [{"agency": "Department of Defense", "Contracts": 9},
            {"agency": "Department of Energy", "Contracts": 5}]
    v = build_visuals("contracts by agency", {"tables": ["spending_state_agency"], "geography_level": "none"}, {}, rows)
    assert v["chart"] is not None
    assert v["map_intent"]["enabled"] is False


def test_single_row_and_empty_no_chart() -> None:
    assert build_visuals("debt ratio for texas", {"tables": ["gov_state"]}, {}, [{"state": "texas", "Debt_Ratio": 0.5}])["chart"] is None
    assert build_visuals("x", {"tables": []}, {}, [])["chart"] is None
    assert build_visuals("x", {"tables": []}, {}, [])["map_intent"]["enabled"] is False
