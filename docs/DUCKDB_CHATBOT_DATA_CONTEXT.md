# DuckDB Chatbot Data Context

This document is the canonical reference for chatbot behavior over the Maryland Opportunity Project datasets.

It explains:

- what each dataset is
- what each metric means
- how the site currently uses each table
- how descriptive statistics are calculated in the app
- and which business rules the chatbot should follow so answers match the dashboard behavior

## 1. Recommended Chatbot Scope

Primary chatbot tables:

### Atlas / map datasets

- `backend/data/atlas/processed/census/acs_state.xlsx`
- `backend/data/atlas/processed/census/acs_county.xlsx`
- `backend/data/atlas/processed/census/acs_congress.xlsx`
- `backend/data/atlas/processed/gov_spending/gov_state.xlsx`
- `backend/data/atlas/processed/gov_spending/gov_county.xlsx`
- `backend/data/atlas/processed/gov_spending/gov_congress.xlsx`
- `backend/data/atlas/processed/Finra/finra_state.xlsx`
- `backend/data/atlas/processed/Finra/finra_county.xlsx`
- `backend/data/atlas/processed/Finra/finra_congress.xlsx`
- `backend/data/atlas/processed/contract_static/contract_state.xlsx`
- `backend/data/atlas/processed/contract_static/contract_county.xlsx`
- `backend/data/atlas/processed/contract_static/contract_congress.xlsx`
- `backend/data/atlas/processed/contract_agency/contract_state.xlsx`
- `backend/data/atlas/processed/contract_agency/contract_county.xlsx`
- `backend/data/atlas/processed/contract_agency/contract_congress.xlsx`
- `backend/data/atlas/processed/spending_breakdown/spending_state.xlsx`

### Supporting dashboard-specific tables

- `backend/data/spending_state_agency.xlsx`
  - Used by the Federal Spending Breakdown state drilldown charts.
  - Structurally the same concept as the state-level `contract_agency` data, but kept as a separate operational file for the breakdown dashboard.

### Fund flow tables

- `data/state_flow.xlsx`
- `data/county_flow.xlsx`
- `data/congress_flow.xlsx`

## 2. Global Conventions

### Geography levels

- `state`
- `county`
- `congress`

### Key ID fields

- `state_fips`
- `fips` or `county_fips`
- `cd_118`

### Year fields

The app normalizes both `year` and `Year` to string form.

Examples:

- `2024` = single reported year
- `2020-2024` = multi-year summary period
- `Fiscal Year 2023` = fiscal-finance label

For DuckDB, do not assume all years are numeric. Keep a text version available.

### State-name casing

State names are not fully standardized:

- census: lowercase
- gov_spending: lowercase
- finra: title case
- contract_static / contract_agency: uppercase
- spending_breakdown: lowercase

Recommended helper field:

- `state_norm = upper(trim(state))`

### Metric families

- Dollar totals: `Contracts`, `Grants`, `Resident Wage`, `Revenue`, `Expenses`
- Counts: `Employees`, `Federal Residents`, `# of household`, `Total population`
- Rates / percentages / shares
- Per-capita or per-1000 normalized metrics
- Ratios: `Current_Ratio`, `Debt_Ratio`
- Index scores: `financial_constraint`, `financial_literacy`, `alternative_financing`

The chatbot should not mix these types unless the user explicitly asks for that.

## 3. How the App Calculates Descriptive Statistics

For the main atlas-style dashboards:

### Step 1: filter

Filter by:

- dataset
- geography level
- year
- and, for `contract_agency`, agency

### Step 2: choose the metric

The app works on one numeric column at a time for the map.

### Step 3: build displayed values

For each geography, the app creates one displayed `value`.

- Regular atlas datasets: selected metric value
- `contract_agency`: filter by agency, re-aggregate by geography, then compute stats

### Step 4: summary statistics

The backend calculates:

- `count`
- `min`
- `max`
- `mean`
- `median`

Implementation details:

- nulls / NaNs are dropped
- no weighting
- no winsorization
- no log transformation

### Step 5: quintile thresholds

The atlas dashboards compute quintiles from sorted non-null values using:

- Q1 threshold = value at index `int(0.2 * (n - 1))`
- Q2 threshold = value at index `int(0.4 * (n - 1))`
- Q3 threshold = value at index `int(0.6 * (n - 1))`
- Q4 threshold = value at index `int(0.8 * (n - 1))`

This is a simple rank-based threshold rule, not percentile interpolation.

### Step 6: ranking

The app builds:

- top 10 locations
- bottom 10 locations
- per-location quintile, rank, and percentile

These are based on the displayed filtered values only.

## 4. Dataset-by-Dataset Reference

### 4A. Census (ACS Demographics)

Files:

- `backend/data/atlas/processed/census/acs_state.xlsx`
- `backend/data/atlas/processed/census/acs_county.xlsx`
- `backend/data/atlas/processed/census/acs_congress.xlsx`

Coverage:

- state: 728 rows
- county: 45,090 rows
- congress: 6,122 rows
- years: `2010` through `2023`

Grain:

- one row per geography per year

Core metrics:

- `Total population`
- `# of household`
- `Median household income`
- `Age 18-65`
- `White`, `Black`, `Asian`, `Hispanic`
- `Education >= High School`, `Education >= Bachelor's`, `Education >= Graduate`
- `Income >$50K`, `Income >$100K`, `Income >$200K`
- `Below poverty`
- `Owner occupied`, `Renter occupied`

Chatbot guidance:

- Treat as population and household profile data, not spending data.
- `Total population` is a count.
- `Median household income` is dollars.
- Most other ACS fields are shares/percentages.

### 4B. Government Finances

Files:

- `backend/data/atlas/processed/gov_spending/gov_state.xlsx`
- `backend/data/atlas/processed/gov_spending/gov_county.xlsx`
- `backend/data/atlas/processed/gov_spending/gov_congress.xlsx`

Coverage:

- state: 50 rows
- county: 3,057 rows
- congress: 428 rows
- year label: `Fiscal Year 2023`

Core metric groups:

- Balance sheet / obligations
- Operating flow
- Normalized or ratio metrics
- Population

Chatbot guidance:

- This dataset is not federal spending.
- Do not describe `Revenue` or `Expenses` here as federal obligations.
- Ratios and per-capita columns should not be added together.

### 4C. FINRA Financial Literacy

Files:

- `backend/data/atlas/processed/Finra/finra_state.xlsx`
- `backend/data/atlas/processed/Finra/finra_county.xlsx`
- `backend/data/atlas/processed/Finra/finra_congress.xlsx`

Coverage:

- state: 255 rows, years `2009`, `2012`, `2015`, `2018`, `2021`
- county: 2,071 rows, year `2021`
- congress: 436 rows, year `2021`

Core metrics:

- `financial_constraint`
- `alternative_financing`
- `financial_literacy`
- `satisfied`
- `risk_averse`

Chatbot guidance:

- These are survey-derived measures, not administrative totals.
- `financial_constraint`, `alternative_financing`, and `financial_literacy` are normalized indices.
- `satisfied` and `risk_averse` are shares/proportions, not counts.

### 4D. Federal Spending

Files:

- `backend/data/atlas/processed/contract_static/contract_state.xlsx`
- `backend/data/atlas/processed/contract_static/contract_county.xlsx`
- `backend/data/atlas/processed/contract_static/contract_congress.xlsx`

Coverage:

- state: 102 rows
- county: 6,268 rows
- congress: 868 rows
- periods: `2020-2024`, `2024`

Core metrics:

- Dollar channels: `Contracts`, `Grants`, `Resident Wage`, `Direct Payments`, `Employees Wage`
- Count channels: `Federal Residents`, `Employees`
- Normalized channels: `... Per 1000`

Chatbot guidance:

- Treat this as a channel-by-channel federal funding dataset.
- The dashboard does not automatically sum channels into one total.
- If the user asks for contracts, use `Contracts`.
- If the user asks for grants, use `Grants`.
- If the user asks for direct payments, use `Direct Payments`.

### 4E. Federal Spending by Agency

Files:

- `backend/data/atlas/processed/contract_agency/contract_state.xlsx`
- `backend/data/atlas/processed/contract_agency/contract_county.xlsx`
- `backend/data/atlas/processed/contract_agency/contract_congress.xlsx`

Coverage:

- state: 2,142 rows
- county: 92,240 rows
- congress: 18,229 rows
- periods: `2020-2024`, `2024`
- agencies: 21 unique agencies

Grain:

- one row per geography per period per agency

Chatbot guidance:

- This is the correct dataset for questions like:
  - "Which agencies are largest in Maryland?"
  - "Which counties receive the most NIH-related grants?"
  - "Where is Department of Defense contract exposure highest?"

### Canonical agency-ranking rule

If the user asks:

> Which agencies account for the most spending in Maryland?

The default total should be:

- `Contracts + Grants + Resident Wage`

and should exclude by default:

- `Direct Payments`
- `Federal Residents`
- `Employees`
- `Employees Wage`

These should only be included when the user explicitly asks for them.

### 4F. Federal Spending Breakdown

Files:

- `backend/data/atlas/processed/spending_breakdown/spending_state.xlsx`
- supporting detail table: `backend/data/spending_state_agency.xlsx`

Important dashboard rule:

- state detail / bar-chart layer ranks agencies on `Contracts + Grants + Resident Wage`
- jobs chart ranks agencies on `Employees`

Canonical definition of "spending" at the agency level:

- `Contracts + Grants + Resident Wage`

Not included by default:

- `Direct Payments`
- `Federal Residents`
- `Employees`
- `Employees Wage`

### 4G. Fund Flow

Files:

- `data/state_flow.xlsx`
- `data/county_flow.xlsx`
- `data/congress_flow.xlsx`

Chatbot guidance:

- Use flow data for directional subcontract questions:
  - inflow
  - outflow
  - subcontract movement
  - origin / destination
- Do not merge flow data into federal spending totals unless the user explicitly asks about subcontract flows.

## 5. Canonical Chatbot Business Rules

### Rule 1: "Spending" is ambiguous

Decide whether the user means:

- one displayed spending channel
- an agency-composition total
- or subcontract flow

### Rule 2: Agency ranking default

For questions like:

- "Which agencies account for the most spending in Maryland?"
- "Top agencies by spending"

default to:

- `Contracts + Grants + Resident Wage`

Do not default to:

- `Direct Payments`
- `Employees`
- `Federal Residents`
- `Employees Wage`

unless explicitly requested.

### Rule 3: Counts are not dollars

Never add these into a dollar total:

- `Federal Residents`
- `Employees`
- `# of household`
- `Total population`

### Rule 4: Per-1000 and per-capita metrics are already normalized

Do not sum or compare them as if they were raw totals.

### Rule 5: Atlas statistics are geography-comparison statistics

If the dashboard says mean, median, quintile, or percentile, it means comparison across selected geographies.

### Rule 6: Preserve text geography IDs

Do not cast away formatting for:

- `cd_118`
- FIPS with leading zeros

### Rule 7: Treat `2020-2024` as its own period label

Do not treat it as interchangeable with single-year `2024`.

### Rule 8: Use flow data only for directional subcontract questions

If the user asks about inflow, outflow, origin, or destination, use the flow tables.

## 6. Suggested DuckDB Normalization Layer

Recommended helper fields:

- `state_norm = upper(trim(state))`
- `year_norm = trim(cast(year_or_Year as varchar))`
- `county_fips_norm = lpad(cast(fips_or_county_fips as varchar), 5, '0')`
- `state_fips_norm = lpad(cast(state_fips as varchar), 2, '0')`
- `cd_norm = upper(trim(cd_118))`

Recommended normalized view names:

- `vw_census_state`
- `vw_census_county`
- `vw_census_congress`
- `vw_gov_fin_state`
- `vw_gov_fin_county`
- `vw_gov_fin_congress`
- `vw_finra_state`
- `vw_finra_county`
- `vw_finra_congress`
- `vw_federal_spending_state`
- `vw_federal_spending_county`
- `vw_federal_spending_congress`
- `vw_federal_spending_agency_state`
- `vw_federal_spending_agency_county`
- `vw_federal_spending_agency_congress`
- `vw_federal_spending_breakdown_state`
- `vw_spending_state_agency`
- `vw_flow_state`
- `vw_flow_county`
- `vw_flow_congress`

For the agency-ranking use case, create a helper view with:

- `spending_total = Contracts + Grants + Resident Wage`

## 7. Known Data Caveats

### Connecticut county boundary issue

Known validation warning:

- `contract_agency:county missing_in_boundaries:4`
- sample IDs: `09130`, `09150`, `09160`, `09180`

These rows can still exist in DuckDB and still be queried as data records.

### Congressional boundaries are year-sensitive

The app uses different congressional boundary files depending on year:

- `2010-2011`
- `2012-2021`
- `2022+`

Be careful when comparing districts across redistricting eras.

### State-level fund flow has different temporal behavior

The state flow table does not behave like county/congress flow with explicit year filtering in the app.

## 8. Quick Answer Recipes

### Which agencies account for the most spending in Maryland?

Use:

- state-level agency table
- default year if unspecified
- `Contracts + Grants + Resident Wage`

Rank descending.

### Which states receive the most federal contracts?

Use:

- `contract_static` state table
- metric: `Contracts`

### Which counties have the highest financial literacy?

Use:

- `finra_county`
- metric: `financial_literacy`

### Which counties are most fiscally stressed?

Use:

- `gov_spending_county`

Define the metric explicitly, for example:

- highest `Debt_Ratio`
- most negative `Free_Cash_Flow`
- lowest `Current_Ratio`

### Which states send the most subcontract dollars into Maryland?

Use:

- flow tables
- Maryland
- inflow direction
- flow amount

## 9. Validation Snapshot

From `python backend/scripts/validate_data.py --warn-only`:

- all major atlas tables validate with `missing_id = 0` and `duplicates = 0`
- one known warning remains:
  - `contract_agency:county missing_in_boundaries:4`

That means the processed tables are otherwise in a good state for chatbot ingestion.
