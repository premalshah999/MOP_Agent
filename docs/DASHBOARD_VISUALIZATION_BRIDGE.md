# Dashboard visualization bridge

The chatbot and the Maryland Opportunity dashboards remain separate UI surfaces, but they now share a small, explicit visualization contract.

## Data flow

1. The backend executes and verifies a query.
2. `mapIntent`, `contract`, and the returned rows describe the visualization without relying on generated answer prose.
3. The chatbot renders a choropleth or a focused fund-flow map from those rows.
4. `dashboardBridge.ts` maps the contract's source tables to the matching dashboard and carries supported filters in the URL.
5. The dashboard reads those filters once during initialization, validates them against its loaded options, and falls back to its normal defaults when a value is unavailable.

## Supported dashboard filters

- Atlas: geography level, variable, year, and agency.
- Fund flow: geography level, state, direction, agency, industry/NAICS, period, and flow range.
- Spending breakdown: metric, year, and state.

The production dashboard base defaults to `https://mop.rhsmith.umd.edu`. A different integration host can be supplied at frontend build time with `VITE_MOP_SITE_URL`.

## Map semantics

- Standard results use up to five data-derived quantile bands. Tied values are
  never split across bands, so fewer bands may be shown when cut points repeat.
- Mixed positive and negative values use equal-width, zero-centered diverging
  bands. The legend prints every threshold rather than resembling a continuous
  gradient.
- Focused inflows use blue curves toward the selected state.
- Focused outflows use red curves away from the selected state.
- All locations and arc endpoints come from the shipped geographic boundaries; no coordinates are inferred by an LLM.
- State and county FIPS identifiers are accepted in addition to canonical names.
- Unshaded areas mean "not returned," not zero. Boundary join misses, missing
  numeric values, and truncated query results are disclosed in the map itself.
- Ranks are computed over the returned evidence, retain ties, and are labeled as
  returned-result ranks rather than national ranks.
- The map is disabled upstream only when rows cannot be joined uniquely to
  boundaries; partial-but-valid results remain visible with an explicit warning.

This layer is intentionally dataset-family aware but question agnostic. New questions supported by the same table contracts inherit the visualization behavior without new phrase matching or per-question code.
