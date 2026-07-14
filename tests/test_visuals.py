"""Deterministic checks for the visualization layer (no LLM)."""

from __future__ import annotations

from app.core.visuals import build_visuals


def _valid_spec(spec: dict) -> bool:
    # Accepts single-mark OR layered specs.
    return (
        isinstance(spec, dict)
        and "$schema" in spec
        and "vega-lite" in spec["$schema"]
        and isinstance(spec.get("data", {}).get("values"), list)
        and ("mark" in spec or "layer" in spec)
        and "encoding" in spec
    )


def test_ranking_emits_lollipop_and_state_map() -> None:
    rows = [{"state": s, "Grants": v} for s, v in
            [("maryland", 9), ("virginia", 7), ("texas", 5), ("ohio", 3)]]
    v = build_visuals("top states by grants", {"tables": ["contract_state"], "geography_level": "state"}, {}, rows)
    assert v["charts"] and _valid_spec(v["charts"][0]["spec"])
    spec = v["charts"][0]["spec"]
    enc = spec["encoding"]
    assert enc["y"]["field"] == "label" and enc["x"]["field"] == "value"
    # ranking = layered bars: bar layer (with hover param) + text value labels,
    # data pre-sorted descending because Vega-Lite drops sorts on layered specs
    assert "layer" in spec and any(l["mark"]["type"] == "bar" for l in spec["layer"])
    assert any(l["mark"]["type"] == "text" for l in spec["layer"])
    values = [d["value"] for d in spec["data"]["values"]]
    assert values == sorted(values, reverse=True)
    assert v["map_intent"]["enabled"] and v["map_intent"]["level"] == "state"
    assert v["map_intent"]["metric"] == "Grants"


def test_mid_n_comparison_emits_horizontal_bars_and_comparison_map() -> None:
    # 4-8 entities -> horizontal bars (smaller N goes to dot plot, see test above).
    rows = [
        {"state": "maryland", "Grants": 9},
        {"state": "virginia", "Grants": 7},
        {"state": "texas", "Grants": 5},
        {"state": "ohio", "Grants": 3},
    ]
    v = build_visuals("compare Maryland Virginia Texas Ohio on grants",
                      {"tables": ["contract_state"], "geography_level": "state"}, {}, rows)
    assert _valid_spec(v["chart"])
    assert v["chart"]["mark"]["type"] == "bar"
    assert v["chart"]["encoding"]["y"]["field"] == "label" and v["chart"]["encoding"]["x"]["field"] == "value"
    assert v["map_intent"]["mapType"] == "atlas-comparison"


def test_trend_emits_area_line() -> None:
    rows = [{"Year": y, "financial_literacy": v} for y, v in
            [(2015, 0.5), (2018, 0.55), (2021, 0.6)]]
    v = build_visuals("trend of financial literacy by year",
                      {"tables": ["finra_state"], "geography_level": "state"}, {}, rows)
    spec = v["chart"]
    assert "layer" in spec
    marks = {l["mark"]["type"] for l in spec["layer"]}
    assert "line" in marks and "area" in marks


def test_small_n_comparison_emits_dot_plot() -> None:
    rows = [{"state": "maryland", "Grants": 9}, {"state": "virginia", "Grants": 7}]
    v = build_visuals("compare Maryland vs Virginia on grants",
                      {"tables": ["contract_state"], "geography_level": "state"}, {}, rows)
    assert v["chart"]["mark"]["type"] == "circle"
    assert v["chart"]["encoding"]["y"]["field"] == "label"


def test_diverging_bars_when_values_span_zero() -> None:
    rows = [{"cd_118": f"MD-0{i}", "Free_Cash_Flow": v} for i, v in
            enumerate([5, -3, 8, -2, 1], start=1)]
    v = build_visuals("Maryland districts by free cash flow",
                      {"tables": ["gov_congress"], "geography_level": "congress"}, {}, rows)
    enc = v["chart"]["encoding"]
    assert "color" in enc and "condition" in enc["color"]
    assert enc["color"]["condition"]["test"] == "datum.value < 0"


def test_heatmap_for_agency_state_breakdown() -> None:
    agencies = ["Defense", "Energy", "Health", "Justice"]
    states = ["Maryland", "Virginia", "Texas", "California"]
    rows = [{"agency": a, "state": s, "Grants": (i + j) * 1_000_000}
            for i, a in enumerate(agencies) for j, s in enumerate(states)]
    v = build_visuals("grants by agency and state",
                      {"tables": ["spending_state_agency"], "geography_level": "state"}, {}, rows)
    assert v["chart"]["mark"]["type"] == "rect"
    assert v["chart"]["encoding"]["color"]["field"] == "value"


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
