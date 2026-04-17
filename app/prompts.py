"""Centralized prompts, few-shot examples, and definitions for the MOP agent."""

from __future__ import annotations

# ---------------------------------------------------------------------------
# SQL generation system prompt — bakes ALL critical rules from metadata.json
# ---------------------------------------------------------------------------
SQL_SYSTEM_PROMPT = """\
You are a SQL expert for a U.S. economic data research dashboard backed by DuckDB.
Write correct, analytical SQL. Return ONLY valid DuckDB SQL — no markdown, no backticks, no explanation.

Think step-by-step before writing SQL:
1. Which table(s) do I need? (see TABLE DECISION GUIDE)
2. What year type does each table use? (see YEAR RULES)
3. Do I need cross-table joins? (see JOIN RULES)
4. Do any columns need quoting? (see QUOTING RULE)

## TABLE DECISION GUIDE

### Federal spending / contracts
- "How much federal money goes to state X?" → contract_state (totals by state)
- "Compare contracts vs grants across states" → contract_state or spending_state
- "Spending by agency/department" → spending_state_agency
- "County-level federal spending" → contract_county. Add WHERE county_fips > 1000 (excludes state-aggregate rows)
- "Congressional district federal spending" → contract_congress
- `spending_state` exists only at the state level and uses `Year` (capital Y), not `year`.

### Government finance (debt, revenue, pension, OPEB)
- gov_state / gov_county / gov_congress — single-year FY2023 only. NEVER add a year filter.

### Demographics (population, poverty, income, education, race)
- acs_state / acs_county / acs_congress — Year is INTEGER, 2010–2023.

### Financial literacy (FINRA survey)
- finra_state (years: 2009, 2012, 2015, 2018, 2021), finra_county / finra_congress (2021 only)

### Fund flows (subaward money between geographies)
- "Flow between states" → state_flow (NO year column — aggregate totals only)
- "Flow trends over time" → county_flow or congress_flow (have act_dt_fis_yr INTEGER 2020-2024)
- state_flow grain: one row per agency per NAICS industry per state-pair. Always SUM(subaward_amount_year) and GROUP BY.
- state_flow has agency_name column — use it for "which department/agency" drill-downs.
- county_flow / congress_flow grain: per agency per NAICS per district-pair per fiscal year.

## YEAR RULES (wrong type or wrong column name = 0 rows)

| Tables | Column | Type | Default | Example |
|--------|--------|------|---------|---------|
| acs_* | Year | INTEGER | 2023 | WHERE Year = 2023 |
| finra_state | Year | INTEGER | 2021 | WHERE Year = 2021 |
| finra_county, finra_congress | Year | INTEGER | 2021 | Single year — do NOT filter |
| contract_* | year | VARCHAR | '2024' | WHERE year = '2024' |
| spending_state | Year | VARCHAR | '2024' | WHERE Year = '2024' |
| spending_state_agency | year | VARCHAR | '2024' | WHERE year = '2024' |
| gov_* | Year | VARCHAR | — | NEVER filter (single year FY2023) |
| county_flow, congress_flow | act_dt_fis_yr | INTEGER | 2024 | WHERE act_dt_fis_yr = 2024 |
| state_flow | — | — | — | NO year column exists |

CRITICAL: contract/spending year also has '2020-2024' aggregate rows. Use year = '2024' for single-year, year = '2020-2024' for 5-year totals.

## STATE NAME CASING (wrong case = 0 rows on joins)

| Casing | Tables |
|--------|--------|
| lowercase | acs_*, gov_*, spending_state, finra_county |
| UPPERCASE | contract_*, spending_state_agency |
| Title Case | finra_state, state_flow, county_flow, congress_flow |

RULE: Always use LOWER(a.state) = LOWER(b.state) for ANY cross-table state join.
For single-table filters: match the table's casing or use LOWER().

## COLUMN QUOTING RULE

RULE: If a column name contains a space, comma, ampersand, hash, angle bracket, or hyphen, wrap it in double quotes.
Common examples: "Below poverty", "Median household income", "Contracts Per 1000", "Grants Per 1000",
"Direct Payments", "Resident Wage", "Federal Residents", "# of household",
"Income >$50K", "Income >$100K", "Income >$200K",
"Education >= High School", "Education >= Bachelor's", "Education >= Graduate",
"Bonds,_Loans_&_Notes", "Non-Current_Liabilities", "Non-Current_Liabilities_per_capita"
Columns without special chars (Contracts, Grants, Debt_Ratio, Revenue_per_capita, financial_literacy) do NOT need quotes.

## JOIN RULES

| Level | Key | Notes |
|-------|-----|-------|
| State | LOWER(a.state) = LOWER(b.state) | Always use LOWER() |
| County | a.fips = b.fips | INTEGER, reliable |
| Congress | a.cd_118 = b.cd_118 | VARCHAR like 'MD-08' |

### congress_flow TRAP
- prime_awardee_stcd118 is INTEGER (not 'MD-08' format) — NEVER join on it.
- Use rcpt_cd_name (VARCHAR like 'Maryland CD-08') for district identification.
- Use rcpt_state / subawardee_state for state filtering (Title Case).

### state_flow columns
- Use rcpt_state_name and subawardee_state_name (Title Case). NEVER use rcpt_st_cd or subawardee_st_cd.
- subaward_amount_year can be NEGATIVE (adjustments). Use SUM() and note if negatives exist.

### Cross-dataset year alignment
When joining tables with different year types, add explicit year filters on BOTH sides:
```sql
-- ACS (int) + contract (varchar) cross-join
FROM acs_state a JOIN contract_state c ON LOWER(a.state) = LOWER(c.state)
WHERE a.Year = 2023 AND c.year = '2024'
```

## DUCKDB SYNTAX

- UNION/INTERSECT/EXCEPT: all branches MUST have the same column count and compatible types.
- Window functions: NEVER put in GROUP BY. Use a CTE:
```sql
WITH ranked AS (
  SELECT state, metric,
         RANK() OVER (ORDER BY metric DESC) AS rnk,
         AVG(metric) OVER () AS national_avg
  FROM table_name
)
SELECT * FROM ranked WHERE rnk <= 10;
```
- Median: quantile_cont(col, 0.5). Percentile: quantile_cont(col, 0.75).
- Correlation: corr(x, y). Always include COUNT(*) AS sample_size with correlations.
- Use LIMIT (not TOP). Default LIMIT 15 for rankings.
- Use ROUND(col, 2) for dollar amounts and ratios in final output.

## ANALYTICAL GUIDELINES

- Ranking without explicit N: LIMIT 15.
- Single-entity question: include national AVG, MEDIAN, RANK via window functions in a CTE.
- Relationship/correlation: include corr(), COUNT(*), and means of both metrics.
- Comparison: use CTEs to compute each side, then JOIN.
- The contract_* and spending_state tables are channel-based federal spending tables. Do not silently invent a total by adding every dollar-like column.
- For generic federal spending questions where a default composite is needed, use spending_total = Contracts + Grants + "Resident Wage".
- Exclude "Direct Payments", "Federal Residents", Employees, and "Employees Wage" unless the user explicitly asks for them.
- NEVER add count columns such as "Federal Residents" or Employees into dollar totals.
- NEVER add any "Per 1000" columns into totals.
- Treat stored "Per 1000" and `_per_capita` fields as published dashboard metrics. Use them directly when the user asks for them.
- Do not recompute "Per 1000" or `_per_capita` fields from raw totals unless the user explicitly asks for a chatbot-derived custom normalization.
- For agency-ranking questions like "Which agencies account for the most spending in Maryland?":
  - Default to spending_total = Contracts + Grants + "Resident Wage".
  - Exclude "Direct Payments", "Federal Residents", Employees, and "Employees Wage" unless the user explicitly asks for them.
- Treat '2020-2024' as its own aggregate period label, not as interchangeable with single-year '2024'.
- Prefer per-capita or per-1000 metrics for fair cross-entity comparisons.
- Default years when unspecified: acs=2023, contract/spending='2024', finra=2021, gov=no filter, flow=no filter (or act_dt_fis_yr=2024 for county/congress flow).

## OUTPUT FORMAT

Return your response as JSON with two fields:
{{"reasoning": "1-2 sentences: which tables, key joins, year handling", "sql": "YOUR SQL HERE"}}

The sql field must contain valid DuckDB SQL starting with SELECT or WITH.
If the data is not available: {{"reasoning": "not available", "sql": "SELECT 'DATA_NOT_AVAILABLE' AS message"}}

{schema_context}

{examples}"""


# ---------------------------------------------------------------------------
# Few-shot examples — organized by category, selected at runtime
# ---------------------------------------------------------------------------
_EXAMPLES_GOV = [
    (
        "Which state has the highest total liabilities per capita?",
        '{"reasoning": "Ranking gov_state by liabilities per capita, no year filter needed", "sql": "SELECT state, ROUND(Total_Liabilities_per_capita, 2) AS liabilities_pc FROM gov_state ORDER BY Total_Liabilities_per_capita DESC LIMIT 15;"}',
    ),
    (
        "Show Maryland revenue in the government finance data",
        '{"reasoning": "Single-entity query on gov_state — add national context via CTE with window functions", "sql": "WITH ranked AS (SELECT state, Revenue_per_capita, AVG(Revenue_per_capita) OVER() AS national_avg, quantile_cont(Revenue_per_capita, 0.5) OVER() AS national_median, RANK() OVER(ORDER BY Revenue_per_capita DESC) AS rev_rank, COUNT(*) OVER() AS total_states FROM gov_state) SELECT * FROM ranked WHERE LOWER(state) = \'maryland\';"}',
    ),
    (
        "Which counties in Virginia have a current ratio below 1?",
        '{"reasoning": "Filter gov_county by state and Current_Ratio threshold, no year filter", "sql": "SELECT county, state, ROUND(Current_Ratio, 3) AS current_ratio FROM gov_county WHERE LOWER(state) = \'virginia\' AND Current_Ratio < 1 ORDER BY Current_Ratio LIMIT 25;"}',
    ),
    (
        "Top 10 states by pension liability per capita",
        '{"reasoning": "CTE with window for ranking + national average context", "sql": "WITH ranked AS (SELECT state, Net_Pension_Liability_per_capita, RANK() OVER(ORDER BY Net_Pension_Liability_per_capita DESC) AS rnk, AVG(Net_Pension_Liability_per_capita) OVER() AS national_avg FROM gov_state) SELECT state, ROUND(Net_Pension_Liability_per_capita, 2) AS pension_pc, rnk, ROUND(national_avg, 2) AS national_avg FROM ranked WHERE rnk <= 10;"}',
    ),
]

_EXAMPLES_ACS = [
    (
        "Top 10 states by poverty rate in 2023",
        '{"reasoning": "ACS state table, Year is INTEGER, quote column with space", "sql": "SELECT state, \\"Below poverty\\" AS poverty_rate FROM acs_state WHERE Year = 2023 ORDER BY \\"Below poverty\\" DESC LIMIT 10;"}',
    ),
    (
        "What is the median household income in Maryland counties?",
        '{"reasoning": "ACS county filtered by state, Year=2023 integer", "sql": "SELECT county, state, \\"Median household income\\" FROM acs_county WHERE LOWER(state) = \'maryland\' AND Year = 2023 ORDER BY \\"Median household income\\" DESC;"}',
    ),
    (
        "How has poverty changed in Texas from 2018 to 2023?",
        '{"reasoning": "Time-series from ACS state with integer years, show trend", "sql": "SELECT Year, \\"Below poverty\\" AS poverty_rate, \\"Total population\\" FROM acs_state WHERE LOWER(state) = \'texas\' AND Year BETWEEN 2018 AND 2023 ORDER BY Year;"}',
    ),
]

_EXAMPLES_CONTRACT = [
    (
        "Which congressional districts receive the most federal grant funding in 2024?",
        '{"reasoning": "contract_congress with varchar year, ranking by Grants", "sql": "SELECT cd_118, Grants FROM contract_congress WHERE year = \'2024\' ORDER BY Grants DESC LIMIT 10;"}',
    ),
    (
        "How much federal money goes to Maryland?",
        '{"reasoning": "User asks about federal spending broadly. Use the dashboard-aligned spending_total = Contracts + Grants + \\"Resident Wage\\" and keep the component columns visible.", "sql": "SELECT state, Contracts, Grants, \\"Resident Wage\\", ROUND(Contracts + Grants + \\"Resident Wage\\", 2) AS spending_total FROM contract_state WHERE LOWER(state) = \'maryland\' AND year = \'2024\';"}',
    ),
    (
        "Counties in Texas with highest grants per capita",
        '{"reasoning": "contract_county, varchar year, exclude state-aggregate rows with county_fips>1000", "sql": "SELECT county, state, \\"Grants Per 1000\\" FROM contract_county WHERE LOWER(state) = \'texas\' AND year = \'2024\' AND county_fips > 1000 ORDER BY \\"Grants Per 1000\\" DESC LIMIT 15;"}',
    ),
]

_EXAMPLES_AGENCY = [
    (
        "How much did the Department of Defense spend in Maryland in 2024?",
        '{"reasoning": "spending_state_agency for agency detail, state is UPPERCASE, year is varchar. Default spending composition on the dashboard is Contracts + Grants + Resident Wage.", "sql": "SELECT agency, state, Contracts, Grants, \\"Resident Wage\\", ROUND(Contracts + Grants + \\"Resident Wage\\", 2) AS spending_total FROM spending_state_agency WHERE LOWER(state) = \'maryland\' AND agency = \'Department of Defense\' AND year = \'2024\';"}',
    ),
    (
        "Federal spending in California by agency in 2024",
        '{"reasoning": "All agencies in one state from spending_state_agency. For default agency spending composition, use Contracts + Grants + Resident Wage.", "sql": "SELECT agency, Contracts, Grants, \\"Resident Wage\\", ROUND(Contracts + Grants + \\"Resident Wage\\", 2) AS spending_total FROM spending_state_agency WHERE LOWER(state) = \'california\' AND year = \'2024\' ORDER BY spending_total DESC;"}',
    ),
    (
        "Which agency spends the most on grants nationwide?",
        '{"reasoning": "Aggregate spending_state_agency across all states by agency", "sql": "SELECT agency, SUM(Grants) AS total_grants, SUM(Contracts) AS total_contracts FROM spending_state_agency WHERE year = \'2024\' GROUP BY agency ORDER BY total_grants DESC LIMIT 15;"}',
    ),
]

_EXAMPLES_FINRA = [
    (
        "Top 10 states by financial literacy score in 2021",
        '{"reasoning": "finra_state, Year is integer, available years: 2009,2012,2015,2018,2021", "sql": "SELECT state, financial_literacy FROM finra_state WHERE Year = 2021 ORDER BY financial_literacy DESC LIMIT 10;"}',
    ),
    (
        "How has financial literacy changed over time nationally?",
        '{"reasoning": "finra_state has data for 2009,2012,2015,2018,2021 — aggregate by year", "sql": "SELECT Year, AVG(financial_literacy) AS avg_literacy, AVG(financial_constraint) AS avg_constraint FROM finra_state GROUP BY Year ORDER BY Year;"}',
    ),
]

_EXAMPLES_FLOW = [
    (
        "What is the total subaward flow from Maryland to other states?",
        '{"reasoning": "state_flow for state-to-state flows, no year column. Filter rcpt_state_name (Title Case). Must GROUP BY because grain is per-agency-per-industry.", "sql": "SELECT subawardee_state_name, ROUND(SUM(subaward_amount_year), 2) AS total_flow FROM state_flow WHERE LOWER(rcpt_state_name) = \'maryland\' GROUP BY subawardee_state_name ORDER BY total_flow DESC LIMIT 10;"}',
    ),
    (
        "Which federal agency drives the most subaward flow from Virginia to Virginia?",
        '{"reasoning": "state_flow has agency_name — drill down on a state-pair by agency", "sql": "SELECT agency_name, ROUND(SUM(subaward_amount_year), 2) AS total_flow FROM state_flow WHERE LOWER(rcpt_state_name) = \'virginia\' AND LOWER(subawardee_state_name) = \'virginia\' GROUP BY agency_name ORDER BY total_flow DESC LIMIT 10;"}',
    ),
    (
        "Subaward flow trends from Maryland by fiscal year",
        '{"reasoning": "county_flow has act_dt_fis_yr for time trends, rcpt_state is Title Case", "sql": "SELECT act_dt_fis_yr AS fiscal_year, ROUND(SUM(subaward_amount), 2) AS total_outbound FROM county_flow WHERE LOWER(rcpt_state) = \'maryland\' GROUP BY act_dt_fis_yr ORDER BY fiscal_year;"}',
    ),
    (
        "Fund flows to Maryland congressional districts in 2024",
        '{"reasoning": "congress_flow — use rcpt_cd_name (NOT prime_awardee_stcd118 which is integer). Filter by rcpt_state.", "sql": "SELECT rcpt_cd_name, ROUND(SUM(subaward_amount), 2) AS total_inbound FROM congress_flow WHERE LOWER(rcpt_state) = \'maryland\' AND act_dt_fis_yr = 2024 GROUP BY rcpt_cd_name ORDER BY total_inbound DESC;"}',
    ),
]

_EXAMPLES_CROSS = [
    (
        "Do states with higher financial literacy scores tend to have lower government debt ratios?",
        '{"reasoning": "Cross-dataset: finra_state (int Year=2021) + gov_state (no year filter). Join on LOWER(state). Include correlation + sample_size.", "sql": "SELECT corr(f.financial_literacy, g.Debt_Ratio) AS literacy_debt_corr, COUNT(*) AS sample_size, ROUND(AVG(f.financial_literacy), 3) AS avg_literacy, ROUND(AVG(g.Debt_Ratio), 3) AS avg_debt_ratio FROM finra_state f JOIN gov_state g ON LOWER(f.state) = LOWER(g.state) WHERE f.Year = 2021;"}',
    ),
    (
        "Which states with high poverty rates receive the most federal contracts per capita?",
        '{"reasoning": "Cross-dataset: acs_state (Year=2023 int) + contract_state (year=\'2024\' varchar). CTEs for each side, join on LOWER(state). Year filters on BOTH sides.", "sql": "WITH demo AS (SELECT state, \\"Below poverty\\" AS poverty_rate FROM acs_state WHERE Year = 2023), fed AS (SELECT state, \\"Contracts Per 1000\\" AS contracts_per_1000 FROM contract_state WHERE year = \'2024\') SELECT d.state, d.poverty_rate, f.contracts_per_1000 FROM demo d JOIN fed f ON LOWER(d.state) = LOWER(f.state) ORDER BY d.poverty_rate DESC LIMIT 20;"}',
    ),
    (
        "Correlation between poverty and federal grants at the county level",
        '{"reasoning": "Cross-dataset county join: acs_county (fips, Year=2023 int) + contract_county (fips, year=\'2024\' varchar). Year filters on BOTH sides.", "sql": "SELECT corr(a.\\"Below poverty\\", c.\\"Grants Per 1000\\") AS poverty_grants_corr, COUNT(*) AS sample_size, ROUND(AVG(a.\\"Below poverty\\"), 2) AS avg_poverty, ROUND(AVG(c.\\"Grants Per 1000\\"), 2) AS avg_grants FROM acs_county a JOIN contract_county c ON a.fips = c.fips WHERE a.Year = 2023 AND c.year = \'2024\';"}',
    ),
    (
        "States with highest debt but lowest federal funding",
        '{"reasoning": "Cross-dataset: gov_state (no year) + contract_state (year=\'2024\'). Use dashboard-aligned spending_total = Contracts + Grants + \\"Resident Wage\\".", "sql": "WITH debt AS (SELECT state, Debt_Ratio, RANK() OVER(ORDER BY Debt_Ratio DESC) AS debt_rank FROM gov_state), funding AS (SELECT state, Contracts + Grants + \\"Resident Wage\\" AS spending_total FROM contract_state WHERE year = \'2024\') SELECT d.state, ROUND(d.Debt_Ratio, 3) AS debt_ratio, d.debt_rank, ROUND(f.spending_total, 0) AS spending_total FROM debt d JOIN funding f ON LOWER(d.state) = LOWER(f.state) WHERE d.debt_rank <= 15 ORDER BY f.spending_total ASC;"}',
    ),
]

# Map table prefixes → relevant example sets
_EXAMPLE_GROUPS = {
    "gov": _EXAMPLES_GOV,
    "acs": _EXAMPLES_ACS,
    "contract": _EXAMPLES_CONTRACT,
    "spending": _EXAMPLES_AGENCY,
    "finra": _EXAMPLES_FINRA,
    "flow": _EXAMPLES_FLOW,
    "state_flow": _EXAMPLES_FLOW,
    "county_flow": _EXAMPLES_FLOW,
    "congress_flow": _EXAMPLES_FLOW,
}


def get_relevant_examples(table_names: list[str], max_examples: int = 10) -> str:
    """Select few-shot examples relevant to the routed tables."""
    seen: set[str] = set()
    selected: list[tuple[str, str]] = []

    # Always include cross-dataset examples (most failure-prone)
    for ex in _EXAMPLES_CROSS:
        selected.append(ex)
        seen.add(ex[0])

    # Add examples for each routed table's family
    for table in table_names:
        for prefix, examples in _EXAMPLE_GROUPS.items():
            if table.startswith(prefix):
                for ex in examples:
                    if ex[0] not in seen and len(selected) < max_examples:
                        selected.append(ex)
                        seen.add(ex[0])

    # Fill remaining slots with gov/acs basics
    for ex in _EXAMPLES_GOV + _EXAMPLES_ACS:
        if ex[0] not in seen and len(selected) < max_examples:
            selected.append(ex)
            seen.add(ex[0])

    lines = ["EXAMPLES (respond in the same JSON format):"]
    for question, response_json in selected[:max_examples]:
        lines.append(f"\nQ: {question}")
        lines.append(f"A: {response_json}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# SQL repair prompt
# ---------------------------------------------------------------------------
SQL_REPAIR_SYSTEM = """\
You repair broken DuckDB SQL for a government data research dashboard.
Return ONLY the corrected SQL — no markdown, no explanation, no JSON wrapper.

Rules:
- Output SQL only, starting with WITH or SELECT.
- Keep the same analytical intent as the user question.
- Ensure syntax is valid DuckDB (not PostgreSQL).
- For UNION/INTERSECT/EXCEPT, all branches must have the same column count and compatible types.
- Do not GROUP BY window expressions; use a CTE for windows.
- Use quantile_cont() not PERCENTILE_CONT() for percentiles.
- Quote columns that have spaces or special characters with double quotes.
- Contract/spending year is VARCHAR: year = '2024' not year = 2024.
- ACS/finra Year is INTEGER: Year = 2023 not Year = '2023'.
- Gov tables have no year column — never add a year filter.
- state_flow has no year column — never filter by year.
- congress_flow: use rcpt_cd_name (varchar), not prime_awardee_stcd118 (integer).
- Always use LOWER() for cross-table state joins.
- If query returned 0 rows, check: year type mismatch, state casing, missing LOWER()."""


def build_repair_prompt(question: str, failed_sql: str, error: str, schema_ctx: str) -> str:
    return (
        f"{schema_ctx}\n\n"
        f"User question:\n{question}\n\n"
        f"Broken SQL:\n{failed_sql}\n\n"
        f"Execution error:\n{error}\n\n"
        "Return corrected SQL only."
    )


# ---------------------------------------------------------------------------
# Formatter prompt
# ---------------------------------------------------------------------------
FORMATTER_SYSTEM = """\
You are a quantitative research analyst writing a polished, evidence-grounded response for a serious public-policy data product.

CRITICAL — Anti-hallucination rules:
- Every number you cite MUST appear in the evidence or data preview below. Do not invent, estimate, or round numbers.
- If the user suggests an answer that contradicts the data (e.g., "isn't it X?"), TRUST THE DATA, not the user's suggestion. Politely state what the data actually shows. Never agree with a user claim that the evidence does not support.
- If the data does not contain information to answer the question, say so. Do not fabricate rows, categories, or values that are not in the evidence.
- Do not add entities, industries, states, or values that do not appear in the query results.
- Treat the grounded draft as the canonical factual scaffold. You may improve structure and explanation, but you must not change the factual content unless the evidence explicitly requires it.

Formatting rules:
- Lead with the direct answer in 1-2 bold sentences.
- Expand the answer into a genuinely useful analyst-style response, not a stub.
- Prefer a consistent answer contract when the evidence supports it:
  1. direct answer
  2. *Definition* if the metric is ambiguous, normalized, relative, or composite
  3. *Key findings* as 3-6 bullet points
  4. *Breakdown* or *Leader profile* when components are available
  5. *Context* paragraph with spread, averages, medians, ranks, nearby peers, or sample size
  6. *Interpretation* that explains what the numbers mean descriptively
  7. *Scope* or *Caveat* only if supported by the evidence
- Include key quantitative context: ranks, averages, medians, spreads, peers, sample sizes, and leading/trailing entities when available.
- Use short paragraphs and flat bullet points. No markdown headings or tables.
- 260-700 words unless the question is extremely simple or the evidence is genuinely sparse.
- No causal claims unless the data directly supports them.
- If the result is a single row, interpret it in context.
- If the result is a ranking, highlight the top entries, the spread, and any notable outliers.
- If the result includes helper columns such as rank, average, median, leader, or trailing entity, use them explicitly.
- Bold the lead finding. Use italics sparingly for labels like _Caveat_ or _Context_.
- If this is a drill-down query that breaks down a previous aggregate, explicitly note the relationship
  (e.g., "The $32.59B total was spread across multiple agencies, with DoD accounting for $20.55B (63%)").
  Never present a sub-component as if it were the total from a prior query."""


def build_formatter_prompt(question: str, evidence_text: str, preview: str, sql: str | None, grounded_draft: str) -> str:
    return (
        f'Question: "{question}"\n\n'
        f"SQL used:\n{sql or 'N/A'}\n\n"
        f"Grounded draft answer (preserve its factual content unless the evidence below requires a correction):\n{grounded_draft}\n\n"
        f"Evidence summary:\n{evidence_text}\n\n"
        f"Data preview (first rows):\n{preview}\n\n"
        "REMINDER: Your answer must be based ONLY on the evidence summary and data preview above. "
        "If the question implies a particular answer (e.g. 'isn't it X?'), verify against the data. "
        "If the data contradicts the user's expectation, say what the data actually shows. "
        "Do not omit important scope, definition, interpretation, or caveat notes from the grounded draft if they remain supported by the evidence. "
        "Expand the grounded draft into a fuller analyst answer with richer key findings, but do not change the factual scaffold."
    )


# ---------------------------------------------------------------------------
# Conceptual answer prompt
# ---------------------------------------------------------------------------
CONCEPTUAL_SYSTEM = """\
You are a U.S. government finance and economic data expert.
Answer questions about definitions, metric meanings, fiscal concepts, and dataset descriptions.
Use 3-6 clear sentences with concrete examples when possible.
If the concept relates to specific tables in the dashboard, mention which dataset contains relevant data.
Be explicit about confidence:
- "verified in code" for runtime behaviors implemented in this repo
- "verified in files" for properties directly observable in the processed tables / schema
- "documented semantic meaning" for formulas or definitions described in project metadata or docs
- "not fully traceable from this repo alone" when the upstream ETL is not present locally
Do not claim a raw-to-final derivation unless it is explicitly visible in code or metadata."""


# ---------------------------------------------------------------------------
# Definitions knowledge base — zero LLM cost for common terms
# ---------------------------------------------------------------------------
DEFINITIONS: dict[str, str] = {
    "debt ratio": (
        "The debt ratio measures total liabilities as a proportion of total assets. "
        "A ratio above 1.0 means liabilities exceed assets. "
        "Available in gov_state, gov_county, gov_congress as Debt_Ratio."
    ),
    "current ratio": (
        "The current ratio is current assets divided by current liabilities. "
        "Values below 1.0 suggest potential short-term liquidity stress. "
        "Available in gov_state, gov_county, gov_congress as Current_Ratio."
    ),
    "free cash flow": (
        "Free cash flow is revenue minus expenses, representing the surplus (or deficit) "
        "available after operating costs. Available as Free_Cash_Flow and Free_Cash_Flow_per_capita in gov tables."
    ),
    "net pension liability": (
        "Net pension liability is the difference between the total pension obligation and the plan's assets. "
        "Higher values indicate larger unfunded pension burdens. "
        "Available as Net_Pension_Liability and Net_Pension_Liability_per_capita in gov tables."
    ),
    "opeb": (
        "OPEB stands for Other Post-Employment Benefits — retiree healthcare and similar non-pension benefits. "
        "Net_OPEB_Liability measures the unfunded portion. Available in gov_state, gov_county, gov_congress."
    ),
    "net position": (
        "Net position is total assets minus total liabilities — the government's net worth. "
        "Negative net position means liabilities exceed assets. Available as Net_Position in gov tables."
    ),
    "financial literacy": (
        "The FINRA financial literacy score measures respondents' ability to answer questions about "
        "interest rates, inflation, bonds, mortgages, and diversification. Higher = more literate. "
        "Available in finra_state (2009-2021), finra_county, finra_congress (2021 only)."
    ),
    "financial constraint": (
        "FINRA financial constraint measures the share of respondents who report difficulty making ends meet. "
        "Higher = more financially stressed. Available in finra_state, finra_county, finra_congress."
    ),
    "acs": (
        "The American Community Survey (ACS) is an annual Census Bureau survey covering demographics, "
        "income, education, poverty, and housing. Available at state, county, and congressional district levels "
        "for 2010-2023."
    ),
    "fips": (
        "FIPS (Federal Information Processing Standards) codes are unique numeric identifiers for U.S. counties. "
        "Used as the primary join key for county-level cross-dataset queries."
    ),
    "cd_118": (
        "cd_118 identifies the 118th Congressional District in format 'STATE-NUMBER' (e.g., 'MD-08'). "
        "Used as the join key for all congress-level tables."
    ),
    "per capita": (
        "Per capita metrics normalize a total by the population of the geographic unit, enabling fair comparison "
        "across entities of different sizes. In this project, _per_capita and 'Per 1000' columns should usually "
        "be treated as stored dashboard metrics rather than silently recomputed from raw totals unless a custom "
        "chatbot-derived normalization is requested."
    ),
    "subaward": (
        "A subaward is a secondary award made by a prime federal award recipient to another entity. "
        "Fund flow tables track these subaward flows between geographies."
    ),
    "contracts": (
        "Federal contracts are agreements where the government purchases goods or services. "
        "Available in contract_state, contract_county, contract_congress with year as VARCHAR."
    ),
    "federal spending": (
        "In the dashboard's default spending composition, federal spending means Contracts + Grants + Resident Wage. "
        "Direct Payments, Federal Residents, Employees, and Employees Wage should only be included when the user explicitly asks for them."
    ),
    "grants": (
        "Federal grants are financial assistance awards to state/local governments or organizations. "
        "Available alongside contracts in the contract_* and spending_* tables."
    ),
    "direct payments": (
        "Federal direct payments include Social Security, Medicare, veterans' benefits, and other payments "
        "made directly to individuals. Available as 'Direct Payments' in contract_* and spending_* tables."
    ),
    "risk averse": (
        "FINRA risk aversion measures the share of respondents unwilling to take any financial risk. "
        "Higher = more risk-averse. Available in finra_state, finra_county, finra_congress."
    ),
}


def lookup_definition(question: str) -> str | None:
    """Return a static definition if the question matches a known term."""
    q = question.lower()
    for term, definition in DEFINITIONS.items():
        if term in q:
            return definition
    return None
