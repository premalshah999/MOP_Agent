# DuckDB Chatbot Data Context

This document is the canonical reference for anyone building a chatbot or direct-query layer on top of the Maryland Opportunity Project data.

It explains:

- what each dataset is
- what each metric means
- how the website currently uses each table
- how descriptive statistics are calculated in the app
- and which business rules the chatbot should follow so answers match the dashboard behavior

## Confidence Labels

Because the chatbot needs to be reliable, this guide distinguishes between different levels of certainty:

- Verified in code: explicitly implemented in the backend or frontend code
- Verified in files: directly observable from the processed tables currently in the repo
- Documented semantic meaning: described in project metadata or docs, but not recomputed in runtime code
- Not fully traceable from this repo alone: likely true, but the full raw-to-processed ETL is not present locally

This matters most for normalized fields such as `Per 1000` and some source-derived finance metrics.

## 1. Recommended Chatbot Scope

If you are loading data into DuckDB, treat these as the primary tables:

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

If the chatbot is focused on the same facts the site shows, these are the tables it should know first.

## 2. Global Conventions

### Geography levels

Most datasets come in one or more of these grains:

- `state`
- `county`
- `congress`

### Key ID fields

- `state_fips`: state FIPS code
- `fips` or `county_fips`: county FIPS code
- `cd_118`: congressional district identifier stored as text like `MD-05`

### Year fields

The app normalizes both `year` and `Year` to string form.

Important examples:

- `2024` = a single reported year
- `2020-2024` = a multi-year summary period used in the federal spending datasets
- `Fiscal Year 2023` = fiscal-finance label used in the government finances dataset

For DuckDB, do not assume all years are numeric. Keep a text version available.

### What this repo does and does not contain

This repo contains:

- the processed dashboard-ready tables
- the backend logic that reads, filters, aggregates, ranks, and summarizes them
- metadata descriptions for many variables
- and repair / validation notes for known data-quality issues

This repo does not contain the full raw-source ETL pipeline for every dataset.

That means:

- we can document how the app uses the data
- we can document formulas that are explicitly described in metadata
- we can document the current file schema and current dashboard semantics
- but we cannot honestly claim every source-side transformation from raw federal, ACS, FINRA, or Reason records unless that formula is visible in this repo

### State-name casing

State names are not fully standardized across files:

- `census`: lowercase state names like `alabama`
- `gov_spending`: lowercase state names
- `finra`: title case state names like `Alabama`
- `contract_static` / `contract_agency`: uppercase state names like `ALABAMA`
- `spending_breakdown`: lowercase state names

For chatbot work, create a normalized helper field such as:

- `state_norm = upper(trim(state))`

### Metric families

Across the project, metrics usually fall into one of these types:

- Dollar totals: `Contracts`, `Grants`, `Resident Wage`, `Revenue`, `Expenses`
- Counts: `Employees`, `Federal Residents`, `# of household`, `Total population`
- Rates / percentages / shares: ACS demographic shares, `satisfied`, `risk_averse`
- Per-capita or per-1,000 normalizations: columns ending in `_per_capita` or `Per 1000`
- Ratios: `Current_Ratio`, `Debt_Ratio`
- Index scores: `financial_constraint`, `financial_literacy`, `alternative_financing`

The chatbot should not mix these types unless the user explicitly asks for that.

### Normalization caution: do not over-assume `Per 1000`

For chatbot purposes:

- treat `Per 1000` and `_per_capita` columns as standalone published metrics
- do not automatically recompute them from raw totals unless you have separately audited the upstream ETL

Reason:

- the app uses these stored normalized columns directly
- some file families are semantically documented as per-1,000 style metrics
- but the exact upstream denominator and scaling convention is not fully derivable for every processed dataset from this repo alone

So the safest rule is:

- if the user asks for the same metric as the dashboard, use the stored normalized field
- if the user asks for a custom normalization, compute a new metric and label it clearly as chatbot-derived

## 3. How the App Calculates Descriptive Statistics

For the main atlas-style dashboards (`census`, `gov_spending`, `contract_static`, `contract_agency`, `spending_breakdown` state map), the backend computes descriptive statistics the same way:

### Step 1: filter

The app filters by:

- dataset
- geography level
- year
- and, for `contract_agency`, agency

### Step 2: choose the metric

The app works on one numeric column at a time for the map.

Important exception:

- the Federal Spending Breakdown state detail chart uses a composite agency spending measure based on multiple columns, not one map column

### Step 3: build the displayed values

For each geography, the app creates one displayed `value`.

- For regular atlas datasets, that is just the selected metric value.
- For `contract_agency`, the data is first filtered by agency and then re-aggregated by geography before stats are computed.
- For the spending breakdown agency chart, agencies are ranked on a composite total built in the app logic.

### Step 4: summary statistics

The backend calculates these on the filtered displayed values:

- `count`
- `min`
- `max`
- `mean`
- `median`

Implementation details:

- nulls / NaNs are dropped
- no weighting is applied
- no winsorization is applied
- no log transformation is applied

So the mean is a plain arithmetic mean across displayed geographies, not a population-weighted mean.

### Step 5: quintile thresholds

The atlas dashboards compute quintiles from the sorted non-null values using the backend rule:

- Q1 threshold = value at index `int(0.2 * (n - 1))`
- Q2 threshold = value at index `int(0.4 * (n - 1))`
- Q3 threshold = value at index `int(0.6 * (n - 1))`
- Q4 threshold = value at index `int(0.8 * (n - 1))`

This is a simple rank-based threshold rule.

It is not percentile interpolation in the statistical-library sense.

### Step 6: ranking

The app then builds:

- top 10 locations
- bottom 10 locations
- per-location quintile, rank, and percentile

These are based on the displayed filtered values only.

## 4. Dataset-by-Dataset Reference

### 4A0. Verified calculation catalog

This section lists the metric formulas and behaviors we can state with confidence from the current repo.

#### Verified in backend code

- Atlas summary statistics:
  - `count`, `min`, `max`, `mean`, `median`
- Atlas quintile thresholds:
  - 20%, 40%, 60%, 80% rank-index thresholds
- Agency dataset map behavior:
  - filter by agency
  - aggregate selected metric back to geography
- Census fallback derived variables:
  - if `Income >$50K`, `Income >$100K`, or `Income >$200K` are missing, the backend can derive them as:
    - `# of household - Income <$50K`
    - `# of household - Income <$100K`
    - `# of household - Income <$200K`

#### Verified in app behavior

- Federal Spending Breakdown agency spending chart:
  - `spending_total = Contracts + Grants + Resident Wage`
- Federal Spending Breakdown jobs chart:
  - rank agencies by `Employees`

#### Documented semantic formulas from repo metadata / docs

- `Non-Current_Liabilities = Total_Liabilities - Current_Liabilities`
- `Net_Position = Total_Assets - Total_Liabilities`
- `Debt_Ratio = Total_Liabilities / Total_Assets`
- `Current_Ratio = Current_Assets / Current_Liabilities`
- `Free_Cash_Flow = Revenue - (Expenses + Current_Liabilities)`
- `*_per_capita = metric / POPULATION`

These formulas are safe to describe as documented project definitions.

#### Not fully traceable from this repo alone

- raw-source construction of ACS percentage fields
- raw-source construction of FINRA normalized indices
- raw-source construction of federal spending `Per 1000` values for every processed file family
- exact upstream joins, exclusions, and harmonization logic that produced the final processed files

### 4A. Census (ACS Demographics)

#### Files

- `backend/data/atlas/processed/census/acs_state.xlsx`
- `backend/data/atlas/processed/census/acs_county.xlsx`
- `backend/data/atlas/processed/census/acs_congress.xlsx`

#### Source

- American Community Survey (ACS) 5-year estimates

#### Coverage

- `state`: 728 rows
- `county`: 45,090 rows
- `congress`: 6,122 rows
- years: `2010` through `2023`

#### Grain

- one row per geography per year

#### Core metrics

- `Total population` -> absolute count
- `# of household` -> absolute count
- `Median household income` -> dollar amount
- `Age 18-65` -> percentage/share
- `White`, `Black`, `Asian`, `Hispanic` -> percentage/share
- `Education >= High School`, `Education >= Bachelor's`, `Education >= Graduate` -> percentage/share
- `Income >$50K`, `Income >$100K`, `Income >$200K` -> threshold-style household measures
- `Below poverty` -> percentage/share
- `Owner occupied`, `Renter occupied` -> percentage/share

#### Column dictionary

- `state` -> state name
- `county` -> county name where present
- `fips` -> county FIPS where present
- `cd_118` -> congressional district ID where present
- `Year` -> ACS year label
- `Total population` -> absolute count
- `Age 18-65` -> working-age share / percentage
- `White`, `Black`, `Asian`, `Hispanic` -> demographic share / percentage
- `Education >= High School`, `Education >= Bachelor's`, `Education >= Graduate` -> attainment share / percentage
- `# of household` -> household count
- `Income >$50K`, `Income >$100K`, `Income >$200K` -> household-income threshold measures used by the app
- `Median household income` -> dollar median
- `Below poverty` -> poverty share / percentage
- `Owner occupied`, `Renter occupied` -> housing-tenure share / percentage

#### Calculation notes

- Verified in code: if certain high-income columns are absent, the backend can derive them from household totals and lower-income complements.
- Not fully traceable from this repo alone: the exact raw ACS extraction and denominator logic for each share field before processing.

#### How the website uses it

- Used for demographic comparison across states, counties, and congressional districts.
- Map, summary stats, quintiles, rank, and percentile all operate on one selected ACS metric at a time.

#### Chatbot guidance

- Treat this dataset as population and household profile data, not spending data.
- Be careful about units:
  - `Total population` is a count
  - `Median household income` is dollars
  - most other ACS fields here are shares/percentages

#### Good question types

- "Which counties have the highest median household income?"
- "Which congressional districts have the largest Hispanic population share?"
- "How does Maryland compare to other states on bachelor's degree attainment?"

### 4B. Government Finances

#### Files

- `backend/data/atlas/processed/gov_spending/gov_state.xlsx`
- `backend/data/atlas/processed/gov_spending/gov_county.xlsx`
- `backend/data/atlas/processed/gov_spending/gov_congress.xlsx`

#### Source

- Reason Foundation local government finance compilation

#### Coverage

- `state`: 50 rows
- `county`: 3,057 rows
- `congress`: 428 rows
- year label: `Fiscal Year 2023`

#### Grain

- one row per geography

#### Core metric groups

##### Balance sheet / obligations

- `Total_Assets`
- `Current_Assets`
- `Total_Liabilities`
- `Current_Liabilities`
- `Non-Current_Liabilities`
- `Net_Position`
- `Net_Pension_Liability`
- `Net_OPEB_Liability`
- `Bonds,_Loans_&_Notes`
- `Compensated_Absences`

##### Operating flow

- `Revenue`
- `Expenses`
- `Free_Cash_Flow`

##### Normalized or ratio metrics

- columns ending in `_per_capita`
- `Current_Ratio`
- `Debt_Ratio`

##### Population

- `POPULATION`

#### Column dictionary

- `Total_Assets` -> total government assets
- `Current_Assets` -> assets expected to be liquid / usable within one year
- `Total_Liabilities` -> total obligations
- `Current_Liabilities` -> short-term obligations
- `Non-Current_Liabilities` -> long-term obligations
- `Net_Position` -> assets minus liabilities
- `Net_Pension_Liability` -> unfunded pension obligation
- `Net_OPEB_Liability` -> unfunded other post-employment benefits
- `Bonds,_Loans_&_Notes` -> debt instruments
- `Compensated_Absences` -> accrued leave liabilities
- `Revenue` -> total revenue
- `Expenses` -> total expenses
- `Free_Cash_Flow` -> residual flow metric described by project docs
- `POPULATION` -> population used for normalization
- `Current_Ratio` -> liquidity ratio
- `Debt_Ratio` -> debt-to-assets ratio
- all `*_per_capita` columns -> normalized by population

#### Calculation notes

- Documented semantic formulas:
  - `Non-Current_Liabilities = Total_Liabilities - Current_Liabilities`
  - `Net_Position = Total_Assets - Total_Liabilities`
  - `Debt_Ratio = Total_Liabilities / Total_Assets`
  - `Current_Ratio = Current_Assets / Current_Liabilities`
  - `Free_Cash_Flow = Revenue - (Expenses + Current_Liabilities)`
  - `*_per_capita = metric / POPULATION`
- Not fully traceable from this repo alone:
  - the complete upstream Reason Foundation extraction logic
  - how state and congress aggregates were originally materialized before arriving in these files

#### How the website uses it

- Used as a fiscal-condition comparison layer across geographies.
- The dashboard treats every selected metric as a geography-level comparison variable.

#### Chatbot guidance

- This dataset is not federal spending. It is local-government financial condition data.
- Do not describe `Revenue` or `Expenses` here as federal obligations.
- Ratios and per-capita columns should not be added together.
- If the user asks "Which counties are fiscally strongest?", the chatbot should define what metric it is using.

#### Good question types

- "Which counties have the highest debt ratio?"
- "Which states have the largest net pension liability per capita?"
- "Where is free cash flow most negative?"

### 4C. FINRA Financial Literacy

#### Files

- `backend/data/atlas/processed/Finra/finra_state.xlsx`
- `backend/data/atlas/processed/Finra/finra_county.xlsx`
- `backend/data/atlas/processed/Finra/finra_congress.xlsx`

#### Source

- FINRA National Financial Capability Study

#### Coverage

- `state`: 255 rows, years `2009`, `2012`, `2015`, `2018`, `2021`
- `county`: 2,071 rows, year `2021`
- `congress`: 436 rows, year `2021`

#### Grain

- one row per geography per survey wave

#### Core metrics

- `financial_constraint` -> normalized index
- `alternative_financing` -> normalized index
- `financial_literacy` -> normalized index
- `satisfied` -> share from 0 to 1
- `risk_averse` -> share from 0 to 1

#### Column dictionary

- `financial_constraint` -> normalized financial-stress / liquidity-constraint index
- `alternative_financing` -> normalized reliance-on-alternative-finance index
- `financial_literacy` -> normalized objective financial-knowledge index
- `satisfied` -> share with high financial satisfaction
- `risk_averse` -> share with low willingness to take investment risk

#### Calculation notes

- Documented semantic meaning exists for all five variables.
- Not fully traceable from this repo alone:
  - the raw survey recoding
  - index-construction weights
  - and the normalization pipeline used upstream

For chatbot use, these fields should be described as published survey-derived scores/shares, not recomputed measures.

#### How the website uses it

- Used to compare relative financial vulnerability and literacy across geographies.
- The app maps one selected index/share at a time.

#### Chatbot guidance

- These are survey-derived measures, not administrative financial totals.
- `financial_constraint`, `alternative_financing`, and `financial_literacy` should be described as scores or normalized indices.
- `satisfied` and `risk_averse` are shares/proportions, not counts.

#### Good question types

- "Which states have the lowest financial literacy?"
- "Which counties show the highest financial constraint?"
- "How does Maryland compare to peer states on risk aversion?"

### 4D. Federal Spending

#### Files

- `backend/data/atlas/processed/contract_static/contract_state.xlsx`
- `backend/data/atlas/processed/contract_static/contract_county.xlsx`
- `backend/data/atlas/processed/contract_static/contract_congress.xlsx`

#### Source

- USAspending-derived processed totals

#### Coverage

- `state`: 102 rows
- `county`: 6,268 rows
- `congress`: 868 rows
- periods: `2020-2024`, `2024`

#### Grain

- one row per geography per period

#### Core metrics

##### Dollar channels

- `Contracts`
- `Grants`
- `Resident Wage`
- `Direct Payments`
- `Employees Wage`

##### Count channels

- `Federal Residents`
- `Employees`

##### Normalized channels

- `Contracts Per 1000`
- `Grants Per 1000`
- `Resident Wage Per 1000`
- `Direct Payments Per 1000`
- `Federal Residents Per 1000`
- `Employees Per 1000`
- `Employees Wage Per 1000`

#### Column dictionary

- `Contracts` -> contract obligations
- `Grants` -> grant obligations
- `Resident Wage` -> wages associated with federal resident workers
- `Direct Payments` -> direct federal payments
- `Federal Residents` -> resident federal worker count
- `Employees` -> employee count
- `Employees Wage` -> employee wage total
- `... Per 1000` columns -> published normalized versions of the corresponding metric

#### Calculation notes

- Verified in files: there are two period labels in the current processed tables:
  - `2020-2024`
  - `2024`
- Verified in app behavior: the dashboard treats each metric independently and does not auto-sum categories.
- Documented semantic meaning: metadata describes the normalized columns as published relative-exposure metrics.
- Not fully traceable from this repo alone:
  - exact upstream denominator and scaling convention for every `Per 1000` field

For chatbot answers, the safest rule is:

- use raw totals when the user asks for totals
- use the stored normalized field when the user asks for "per 1,000" or relative exposure

#### How the website uses it

- This is the main Federal Spending map dataset.
- Users choose one channel at a time.
- The dashboard does not automatically sum channels into one total.

#### Chatbot guidance

- Treat this as a channel-by-channel federal funding dataset.
- If the user asks for "contracts", use `Contracts`.
- If the user asks for "grants", use `Grants`.
- If the user asks for "direct payments", use `Direct Payments`.
- If the user asks for "federal workforce exposure", use `Federal Residents`, `Employees`, or wage measures depending on wording.

#### Important caution

The `Per 1000` fields in the processed files are already stored as ready-to-use values.

For chatbot answers, prefer:

- using the stored column if you want to mirror the dashboard exactly
- recomputing your own normalized value only if you are intentionally defining a new metric

Do not silently mix stored `Per 1000` values with recomputed totals.

### 4E. Federal Spending by Agency

#### Files

- `backend/data/atlas/processed/contract_agency/contract_state.xlsx`
- `backend/data/atlas/processed/contract_agency/contract_county.xlsx`
- `backend/data/atlas/processed/contract_agency/contract_congress.xlsx`

#### Source

- USAspending-derived processed totals with agency breakdown

#### Coverage

- `state`: 2,142 rows
- `county`: 92,240 rows
- `congress`: 18,229 rows
- periods: `2020-2024`, `2024`
- agencies: 21 unique agencies

#### Grain

- one row per geography per period per agency

#### Core metrics

Same channel family as `contract_static`, but split by `agency`:

- `Contracts`
- `Grants`
- `Resident Wage`
- `Direct Payments`
- `Federal Residents`
- and their `Per 1000` variants

State level also includes:

- `Employees`
- `Employees Wage`
- and their `Per 1000` variants

#### Column dictionary

- `agency` -> awarding agency name
- `Contracts`, `Grants`, `Resident Wage`, `Direct Payments` -> dollar channels
- `Federal Residents`, `Employees` -> count channels
- `Employees Wage` -> employee payroll dollar channel where present
- `... Per 1000` columns -> published normalized versions

#### Calculation notes

- Verified in backend code:
  - agency filtering is exact string matching after normalization
  - once filtered, the selected metric is re-aggregated to geography before ranking/statistics
- Verified in files:
  - 21 agencies currently exist in this dataset family
  - two period labels currently exist: `2020-2024` and `2024`
- Not fully traceable from this repo alone:
  - exact upstream derivation of all normalized `Per 1000` columns

#### How the website uses it

- The user selects geography level, year, metric, and optionally one agency.
- The backend:
  1. filters by selected agency
  2. aggregates back to geography
  3. then computes statistics and rankings

#### Chatbot guidance

- This is the correct dataset for questions like:
  - "Which agencies are largest in Maryland?"
  - "Which counties receive the most NIH-related grants?"
  - "Where is Department of Defense contract exposure highest?"

#### Canonical agency-ranking rule

If the user asks:

> "Which agencies account for the most spending in Maryland?"

the chatbot should not rank agencies on a single metric column unless the user names one.

Instead, the default spending total should be:

- `Contracts + Grants + Resident Wage`

and should exclude:

- `Direct Payments`
- `Federal Residents`
- `Employees`
- `Employees Wage`

unless the user explicitly asks for them.

Why this rule exists:

- It matches the current Federal Spending Breakdown agency-composition chart logic in the site, where the stacked spending view is based on contracts, grants, and `Resident Wage`.

#### Example chatbot interpretation

Question:

> "Which agencies account for the most spending in Maryland in 2024?"

Recommended logic:

1. Use the state-level agency table
2. Filter:
   - `state_norm = 'MARYLAND'`
   - `year = '2024'`
3. Compute:
   - `spending_total = Contracts + Grants + Resident Wage`
4. Rank descending

If the user instead asks:

> "Which agencies account for the most direct payments in Maryland?"

then rank by `Direct Payments` only.

### 4F. Federal Spending Breakdown

#### Files

- `backend/data/atlas/processed/spending_breakdown/spending_state.xlsx`
- supporting detail table: `backend/data/spending_state_agency.xlsx`

#### Source

- USAspending-derived state totals plus state-agency detail table

#### Coverage

- state only
- 102 rows in `spending_state.xlsx`
- periods: `2020-2024`, `2024`

#### Grain

- state-year totals in `spending_state.xlsx`
- state-year-agency detail in `spending_state_agency.xlsx`

#### Core metrics

- `Contracts`
- `Grants`
- `Resident Wage`
- `Direct Payments`
- `Federal Residents`
- `Employees`
- `Employees Wage`
- plus `Per 1000` variants

#### Column dictionary

- the state-level map file contains the same main federal-spending channels:
  - `Contracts`
  - `Grants`
  - `Resident Wage`
  - `Direct Payments`
  - `Federal Residents`
  - `Employees`
  - `Employees Wage`
  - normalized `Per 1000` versions
- the detail file `spending_state_agency.xlsx` contains agency-by-state-by-period rows used for stacked bars

#### Calculation notes

- Verified in app behavior:
  - spending composition = `Contracts + Grants + Resident Wage`
  - jobs chart = `Employees`
- Verified in files:
  - this dataset family has state-only totals for `2020-2024` and `2024`
- Not fully traceable from this repo alone:
  - exact upstream derivation of the normalized `Per 1000` fields

#### How the website uses it

This dashboard has two layers:

1. State map layer
   - uses `spending_breakdown/spending_state.xlsx`
   - one selected metric at a time
2. State detail / bar-chart layer
   - uses `backend/data/spending_state_agency.xlsx`
   - the spending chart ranks agencies on `Contracts + Grants + Resident Wage`
   - the jobs chart ranks agencies on `Employees`

#### Chatbot guidance

This dashboard is the best conceptual reference when users ask:

- "Who are the top agencies in Maryland?"
- "What makes up federal spending in Maryland?"
- "Which agencies dominate contracts and grants?"

#### Canonical definition of spending in this dashboard

Default agency-level spending composition:

- `Contracts + Grants + Resident Wage`

Not included in that default composite:

- `Direct Payments`
- `Federal Residents`
- `Employees`
- `Employees Wage`

So the chatbot should mirror that unless the user explicitly asks for a broader or different definition.

#### Important distinction

- `Resident Wage` is the wage component used in the dashboard's spending composition.
- `Employees Wage` is a separate payroll metric and should not be substituted automatically.

### 4G. Fund Flow

#### Files

- `data/state_flow.xlsx`
- `data/county_flow.xlsx`
- `data/congress_flow.xlsx`

#### Source

- subcontract flow extracts used by the fund-flow dashboard

#### Coverage

- `state_flow.xlsx`: 27,436 rows
- `county_flow.xlsx`: 157,113 rows
- `congress_flow.xlsx`: 115,827 rows

#### Grain

Raw-ish flow records, then grouped by:

- origin geography
- destination geography
- agency context
- industry context where available

#### Core columns

##### State flow

- `rcpt_state_name`
- `subawardee_state_name`
- `subaward_amount_year`
- `agency_name`
- `naics_2digit_code`
- `naics_2digit_title`

##### County flow

- `rcpt_cty`, `subawardee_cty`
- `rcpt_cty_name`, `subawardee_cty_name`
- `rcpt_state`, `subawardee_state`
- `act_dt_fis_yr`
- `subaward_amount`
- `agency_name`
- origin / destination coordinates

##### Congress flow

- `prime_awardee_stcd118`
- `subawardee_stcd118`
- `rcpt_cd_name`, `subawardee_cd_name`
- `act_dt_fis_yr`
- `subaward_amount`
- `agency_name`
- `naics_2digit_code`, `naics_2digit_title`
- origin / destination coordinates

#### Calculation notes

- Verified in backend code:
  - raw rows are normalized into a common schema
  - filters are applied for agency, state, direction, industry, and year range
  - internal flows are counted, then removed from the displayed arc set
  - displayed flows are grouped by origin-destination pair and summed
- Verified in backend code:
  - fixed default amount thresholds are:
    - `<= $1M`
    - `$1M-$10M`
    - `$10M-$100M`
    - `$100M-$1B`
    - `> $1B`
- Verified in files:
  - state / county / congress flow tables exist and are large raw-ish extracts
- Not fully traceable from this repo alone:
  - full upstream construction pipeline for coordinates and harmonized geography names

#### How the website uses it

The backend:

1. loads raw flow rows
2. normalizes origin/destination names and coordinates
3. filters by agency, state, direction, industry, and year range
4. computes totals and internal-flow stats
5. removes internal flows from the map
6. groups remaining flows into origin-destination arcs

#### Important fund-flow rules

- Internal flows = same origin and destination location
  - These are counted in summary stats but removed from map arcs.
- State-level flow table has no explicit year filter in the app.
- County and congress flow tables use `act_dt_fis_yr`.
- If no agency filter is chosen, the map arc label shows the top contributing agency for that grouped arc.

#### Flow descriptive statistics

The flow dashboard summary is different from the atlas dashboards.

It reports:

- total amount
- total flows
- displayed flows
- displayed amount
- average flow
- locations involved
- internal flow count
- internal flow amount

Flow quintiles are based on flow amounts, not geography-level metrics.

When a flow bucket is active, thresholds are recomputed from filtered displayed amounts. Otherwise the dashboard uses fixed thresholds:

- Q1: `<= $1M`
- Q2: `$1M - $10M`
- Q3: `$10M - $100M`
- Q4: `$100M - $1B`
- Q5: `> $1B`

#### Chatbot guidance

This dataset answers directional questions:

- "Which states send the most subcontract dollars into Maryland?"
- "Which counties are the biggest outflow origins?"
- "Which agencies dominate Maryland inflows?"
- "What industries carry the biggest subcontract flows?"

This dataset should not be merged into the federal spending totals unless the user is explicitly asking about subcontract flows.

## 5. Canonical Chatbot Business Rules

These rules should be hard-coded into prompts, retrieval logic, or semantic SQL templates.

### Rule 1: Spending is ambiguous

When the user says "spending", the chatbot should first decide whether they mean:

- a single displayed spending channel (`Contracts`, `Grants`, `Direct Payments`, etc.)
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

If the user asks for "largest total", use raw totals.
If the user asks for "highest relative burden/exposure", use the normalized fields.

### Rule 4A: Never silently reinterpret normalized federal-spending columns

For `contract_static`, `contract_agency`, and `spending_breakdown`:

- if the user asks for `Contracts Per 1000`, use that exact stored field
- do not silently substitute your own formula using population unless you explicitly say you are creating a chatbot-derived metric

### Rule 5: Atlas statistics are geography-comparison statistics

If the dashboard says:

- mean
- median
- quintile
- percentile

that means comparison across selected geographies, not comparison across people or households.

### Rule 6: Preserve text geography IDs

Do not cast these to integers in a way that loses formatting:

- `cd_118`
- FIPS with leading zeros

### Rule 7: Treat `2020-2024` as its own period label

Do not treat `2020-2024` as the same thing as a single-year `2024`.

It is a separate summary period and should be described that way.

### Rule 8: Use flow data only for directional subcontract questions

If the user asks about:

- inflow
- outflow
- subcontract movement
- origin/destination

use the flow tables.

If the user asks about:

- contracts
- grants
- wages
- direct payments

use the federal spending tables.

## 6. Suggested DuckDB Normalization Layer

Before the chatbot queries the tables, create normalized views with:

- `state_norm = upper(trim(state))`
- `year_norm = trim(cast(year_or_Year as varchar))`
- `county_fips_norm = lpad(cast(fips_or_county_fips as varchar), 5, '0')`
- `state_fips_norm = lpad(cast(state_fips as varchar), 2, '0')`
- `cd_norm = upper(trim(cd_118))`

Recommended view names:

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

This will make the chatbot much more reliable.

## 7. Known Data Caveats

### Connecticut county boundary issue

Validation currently reports:

- `contract_agency:county missing_in_boundaries:4`
- sample IDs: `09130`, `09150`, `09160`, `09180`

These are Connecticut planning-region related IDs that do not exist in the legacy county boundary file used for that dataset's county map rendering.

Effect:

- the rows exist in the data
- but those specific IDs are not available for map boundary matching in the current legacy-county setup

For chatbot use, the rows can still exist in DuckDB and can still be queried as data records.

### Congressional boundaries are year-sensitive

The app uses different congressional boundary files depending on year:

- `2010-2011` -> older districts
- `2012-2021` -> mid-period districts
- `2022+` -> current districts

For chatbot answers, be careful when comparing congressional districts across redistricting eras.

### State-level fund flow has different temporal behavior

The state flow table does not behave like county/congress flow tables with explicit year filtering in the app.

If the chatbot needs year-specific flow answers, county or congress tables are more reliable starting points.

## 8. Quick Answer Recipes

### Q: Which agencies account for the most spending in Maryland?

Use:

- `contract_agency` state table or `spending_state_agency.xlsx`

Filter:

- Maryland
- selected year

Compute:

- `Contracts + Grants + Resident Wage`

Rank:

- descending

### Q: Which states receive the most federal contracts?

Use:

- `contract_static` state table

Metric:

- `Contracts`

### Q: Which counties have the highest financial literacy?

Use:

- `finra_county`

Metric:

- `financial_literacy`

### Q: Which counties are most fiscally stressed?

Use:

- `gov_spending_county`

But define the metric explicitly, for example:

- highest `Debt_Ratio`
- most negative `Free_Cash_Flow`
- lowest `Current_Ratio`

### Q: Which states send the most subcontract dollars into Maryland?

Use:

- fund flow tables

Filter:

- Maryland
- inflow direction

Measure:

- flow `amount`

## 9. Validation Snapshot

Current documented validation status:

- all major atlas tables validate with:
  - `missing_id = 0`
  - `duplicates = 0`
- one known warning remains:
  - `contract_agency:county missing_in_boundaries:4`

That means the processed tables are otherwise in a good state for chatbot ingestion.

## 10. Honest Scope Statement

Do we now have enough context to build a good chatbot?

Yes.

Do we now have enough context to claim a full raw-to-final lineage audit for every field?

Not completely.

The current repo is enough to support:

- high-quality DuckDB querying
- dashboard-consistent answer logic
- defensible metric descriptions
- and explicit business rules for ambiguous questions

The current repo is not enough to prove every upstream ETL detail for every source system without additional source-building scripts or notebooks.
