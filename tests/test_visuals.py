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
    assert "layer" in spec and any(layer["mark"]["type"] == "bar" for layer in spec["layer"])
    assert any(layer["mark"]["type"] == "text" for layer in spec["layer"])
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
    marks = {layer["mark"]["type"] for layer in spec["layer"]}
    assert "line" in marks and "area" in marks


def test_multi_entity_trend_keeps_series_separate() -> None:
    rows = [
        {"state": state, "Year": year, "financial_literacy": value}
        for state, values in (("Maryland", [0.5, 0.6]), ("Virginia", [0.4, 0.55]))
        for year, value in zip((2018, 2021), values)
    ]
    visual = build_visuals(
        "compare Maryland and Virginia financial literacy trends",
        {"tables": ["finra_state"], "columns": ["financial_literacy"], "geography_level": "state"},
        {},
        rows,
    )
    spec = visual["chart"]
    assert spec["encoding"]["color"]["field"] == "series"
    assert {item["series"] for item in spec["data"]["values"]} == {"Maryland", "Virginia"}
    assert not any(layer["mark"]["type"] == "area" for layer in spec["layer"])


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
    assert len(v["chart"]["data"]["values"]) == 20


def test_distribution_uses_every_returned_row() -> None:
    rows = [{"state": f"s{i}", "Debt_Ratio": i / 100} for i in range(52)]
    visual = build_visuals(
        "show the distribution of state debt ratios",
        {"tables": ["gov_state"], "columns": ["Debt_Ratio"], "geography_level": "state"},
        {},
        rows,
    )
    assert visual["chart"]["encoding"]["x"].get("bin")
    assert len(visual["chart"]["data"]["values"]) == 52


def test_large_ranking_stays_ranking_and_reports_visual_limit() -> None:
    rows = [{"state": f"s{i}", "Debt_Ratio": i / 100} for i in range(52)]
    visual = build_visuals(
        "rank states by debt ratio",
        {
            "tables": ["gov_state"],
            "columns": ["Debt_Ratio"],
            "geography_level": "state",
            "sort_direction": "desc",
        },
        {},
        rows,
    )
    block = visual["charts"][0]
    assert "layer" in block["spec"]
    assert len(block["spec"]["data"]["values"]) == 20
    assert block["subtitle"] == "Showing 20 of 52 returned rows"


def test_bottom_ranking_is_sorted_ascending_in_chart_and_map() -> None:
    rows = [{"state": state, "Debt_Ratio": value} for state, value in
            [("Maryland", 0.7), ("Virginia", 0.5), ("Texas", 0.6)]]
    visual = build_visuals(
        "bottom 3 states by debt ratio",
        {
            "tables": ["gov_state"],
            "columns": ["Debt_Ratio"],
            "geography_level": "state",
            "sort_direction": "asc",
        },
        {},
        rows,
    )
    assert [item["value"] for item in visual["chart"]["data"]["values"]] == [0.5, 0.6, 0.7]
    assert visual["map_intent"]["sortDirection"] == "asc"


def test_cross_dataset_two_metric_result_emits_scatterplot() -> None:
    rows = [
        {"state": "Maryland", "Below poverty": 9.1, "Debt_Ratio": 0.78},
        {"state": "Virginia", "Below poverty": 9.9, "Debt_Ratio": 0.62},
        {"state": "Texas", "Below poverty": 13.7, "Debt_Ratio": 0.55},
    ]
    visual = build_visuals(
        "which states have high poverty and high debt ratios",
        {
            "tables": ["acs_state", "gov_state"],
            "columns": ["Below poverty", "Debt_Ratio"],
            "geography_level": "state",
        },
        {},
        rows,
    )
    spec = visual["chart"]
    assert spec["mark"]["type"] == "circle"
    assert spec["encoding"]["x"]["field"] == "x"
    assert spec["encoding"]["y"]["field"] == "y"


def test_same_unit_multi_metric_comparison_emits_grouped_bars() -> None:
    rows = [
        {"state": "Maryland", "Grants": 9, "Contracts": 7},
        {"state": "Virginia", "Grants": 8, "Contracts": 6},
    ]
    visual = build_visuals(
        "compare Maryland and Virginia on grants and contracts",
        {
            "tables": ["contract_state"],
            "columns": ["Grants", "Contracts"],
            "geography_level": "state",
        },
        {},
        rows,
    )
    spec = visual["chart"]
    assert spec["mark"]["type"] == "bar"
    assert spec["encoding"]["yOffset"]["field"] == "metric"
    assert len(spec["data"]["values"]) == 4


def test_catalog_units_flow_into_chart_and_map() -> None:
    rows = [{"state": "Maryland", "Below poverty": 9.1}, {"state": "Virginia", "Below poverty": 9.9}]
    visual = build_visuals(
        "compare Maryland and Virginia poverty rates",
        {
            "tables": ["acs_state"],
            "columns": ["Below poverty"],
            "geography_level": "state",
        },
        {},
        rows,
    )
    assert visual["chart"]["encoding"]["x"]["axis"]["labelExpr"] == "datum.label + '%'"
    assert visual["map_intent"]["unit"] == "percent"


def test_duplicate_geographies_disable_misleading_choropleth() -> None:
    rows = [
        {"agency": "Defense", "state": "Maryland", "Grants": 9},
        {"agency": "Energy", "state": "Maryland", "Grants": 5},
    ]
    visual = build_visuals(
        "grants by agency and state",
        {"tables": ["spending_state_agency"], "columns": ["Grants"], "geography_level": "state"},
        {},
        rows,
    )
    assert visual["map_intent"]["enabled"] is False
    assert "same geography" in visual["map_intent"]["reason"]


def test_national_counties_without_state_keys_disable_map() -> None:
    rows = [
        {"county": "Washington", "Grants": 9},
        {"county": "Franklin", "Grants": 5},
    ]
    visual = build_visuals(
        "top counties nationally by grants",
        {"tables": ["contract_county"], "columns": ["Grants"], "geography_level": "county"},
        {},
        rows,
    )
    assert visual["map_intent"]["enabled"] is False
    assert "state boundary key" in visual["map_intent"]["reason"]


def test_focused_inflow_map_uses_origins_and_names_focus() -> None:
    rows = [
        {"rcpt_state_name": "Virginia", "subaward_amount": 9},
        {"rcpt_state_name": "Pennsylvania", "subaward_amount": 5},
    ]
    visual = build_visuals(
        "Which states send the most subawards into Maryland?",
        {
            "tables": ["state_flow"],
            "columns": ["subaward_amount"],
            "geography_level": "state",
            "flow_direction": "inflow",
            "sort_direction": "desc",
        },
        {"state_flow": {"state": {"value": "Maryland"}}},
        rows,
    )
    intent = visual["map_intent"]
    assert intent["geoSide"] == "source"
    assert intent["state"] == "Maryland"
    assert intent["metricLabel"] == "Subaward amount"
    assert "origins" in intent["subtitle"].lower()


def test_focused_outflow_map_uses_destinations() -> None:
    rows = [
        {"subawardee_state_name": "Virginia", "subaward_amount": 9},
        {"subawardee_state_name": "Pennsylvania", "subaward_amount": 5},
    ]
    visual = build_visuals(
        "Where do Maryland prime recipients send subawards?",
        {
            "tables": ["state_flow"],
            "columns": ["subaward_amount"],
            "geography_level": "state",
            "flow_direction": "outflow",
        },
        {"state_flow": {"state": {"value": "Maryland"}}},
        rows,
    )
    assert visual["map_intent"]["geoSide"] == "destination"
    assert "destinations" in visual["map_intent"]["subtitle"].lower()


def test_flow_map_tolerates_safe_model_chosen_geo_alias() -> None:
    visual = build_visuals(
        "Which states send the most subawards into Maryland?",
        {
            "tables": ["state_flow"],
            "columns": ["subaward_amount_year"],
            "geography_level": "state",
            "flow_direction": "inflow",
            "sort_direction": "desc",
        },
        {"state_flow": {"state": {"value": "Maryland"}}},
        [{"sending_state": "Virginia", "total_outflow": 8_129_717_777.39}],
    )
    intent = visual["map_intent"]
    assert intent["enabled"] is True
    assert intent["geoSide"] == "source"
    assert intent["metricLabel"] == "Subaward amount"


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
