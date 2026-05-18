# Rebuild Handoff

Date: 2026-04-29

This document is for the next Codex session that will rebuild `mop-agent` from scratch.

It explains:

- what the current project contains
- where the data and metadata live
- what the user actually wants
- what we changed in the current system
- what improved
- what failed
- and what the replacement architecture should look like

This is intentionally blunt. The goal is not to defend the current app. The goal is to give the next build a clean start.

## 1. Product Goal

The user does not want a clever NL-to-SQL demo.

The user wants a production-grade analytical assistant for a small set of curated public-policy datasets:

- accurate
- grounded in the real data
- strong on follow-ups
- strong on ambiguous phrasing
- strong on rankings, comparisons, trends, and scoped drill-downs
- able to refuse only when truly out of scope
- able to clarify when a query is underspecified
- able to explain methodology transparently
- able to attach maps and charts where they are actually useful

The desired end state is:

1. User asks messy real-world question.
2. System understands the request in context.
3. System maps the request to supported semantics.
4. System executes trustworthy calculations.
5. System returns a detailed, structured answer with evidence.
6. System offers a good map or chart only when appropriate.

The user explicitly does not want:

- gimmicks
- shallow LLM-first guessing
- random fallback answers
- follow-up leakage from prior questions
- overengineered UI masking weak reasoning

## 2. Current Repository Structure

Main repo:

```text
mop-agent/
├── app/                      # FastAPI backend, legacy agent, v2 governed engine
├── data/
│   ├── uploads/              # Raw Excel source files
│   ├── parquet/              # Converted Parquet runtime tables
│   ├── boundaries/           # GeoJSON for maps
│   └── schema/               # manifest.json + metadata.json
├── frontend/                 # React + Vite UI
├── scripts/                  # conversion, benchmarks, eval
├── tests/                    # unit + HTTP + smoke tests
├── docs/                     # architecture and redesign notes
├── deploy/                   # nginx/setup/redeploy scripts
└── reports/                  # benchmark and eval artifacts
```

Parent dataset directory:

```text
datasets/
├── dashboards/               # Original normalized dashboard-oriented folders
├── docs/processed_data_notes.md
├── metadata/
│   ├── datasets_manifest.json
│   └── dashboard_registry.json
├── geospatial/boundaries/
├── reference/
│   ├── variable_dictionary_full.xlsx
│   └── data_source_url.pdf
├── metadata.json             # large semantic metadata file
└── mop-agent/
```

## 3. Where the Data Is

### Inside `mop-agent`

Raw uploaded files:

- [acs_state.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/acs_state.xlsx)
- [acs_county.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/acs_county.xlsx)
- [acs_congress.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/acs_congress.xlsx)
- [gov_state.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/gov_state.xlsx)
- [gov_county.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/gov_county.xlsx)
- [gov_congress.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/gov_congress.xlsx)
- [finra_state.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/finra_state.xlsx)
- [finra_county.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/finra_county.xlsx)
- [finra_congress.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/finra_congress.xlsx)
- [contract_state.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/contract_state.xlsx)
- [contract_county.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/contract_county.xlsx)
- [contract_congress.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/contract_congress.xlsx)
- [spending_state.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/spending_state.xlsx)
- [spending_state_agency.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/spending_state_agency.xlsx)
- [state_flow.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/state_flow.xlsx)
- [county_flow.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/county_flow.xlsx)
- [congress_flow.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads/congress_flow.xlsx)

Runtime Parquet tables:

- [data/parquet](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/parquet)

Geospatial boundaries:

- [states.geojson](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/boundaries/states.geojson)
- [counties.geojson](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/boundaries/counties.geojson)
- [congress.geojson](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/boundaries/congress.geojson)

### Parent-level supporting assets

These should not be ignored in the rebuild:

- [datasets_manifest.json](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/metadata/datasets_manifest.json)
- [dashboard_registry.json](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/metadata/dashboard_registry.json)
- [processed_data_notes.md](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/docs/processed_data_notes.md)
- [variable_dictionary_full.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/reference/variable_dictionary_full.xlsx)
- [data_source_url.pdf](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/reference/data_source_url.pdf)
- [metadata.json](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/metadata.json)

## 4. Where the Metadata Is

Primary runtime metadata inside the app:

- [manifest.json](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/schema/manifest.json)
- [metadata.json](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/schema/metadata.json)

Important distinction:

- `manifest.json` is the runtime registration source.
- `metadata.json` is the semantic knowledge source.

That distinction matters because they are not aligned.

### Runtime-loaded tables

Current manifest has 17 runtime tables:

- `acs_state`
- `acs_county`
- `acs_congress`
- `gov_state`
- `gov_county`
- `gov_congress`
- `finra_state`
- `finra_county`
- `finra_congress`
- `contract_state`
- `contract_county`
- `contract_congress`
- `spending_state`
- `spending_state_agency`
- `state_flow`
- `county_flow`
- `congress_flow`

### Documented-but-not-loaded tables

Current semantic metadata documents 20 tables, including:

- `contract_state_agency`
- `contract_county_agency`
- `contract_cd_agency`

Those agency tables are documented but not currently present in the runtime manifest.

This is one of the biggest structural sources of confusion. The app sometimes reasons as if those tables exist operationally, but they are not actually loaded.

## 5. Dataset Families

The current app effectively works with these families:

1. ACS / Census
2. Government Finances
3. FINRA
4. Federal Spending
5. Federal Spending by Agency
6. Federal Spending Breakdown
7. Fund Flow

The most important semantic facts already captured in metadata:

- state casing is inconsistent across families
- some columns need SQL quoting
- contract/spending year is string-based, not integer-only
- government finance is only `Fiscal Year 2023`
- FINRA state has multiple years, county/congress only `2021`
- flow tables have special join/key caveats
- published `Per 1000` and `_per_capita` fields should be treated as first-class metrics, not casually recomputed

## 6. Current Application Architecture

### Backend

Entrypoint:

- [main.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/main.py)

Primary legacy path:

- [agent.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/agent.py)
- [classifier.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/classifier.py)
- [planner.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/planner.py)
- [router.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/router.py)
- [formatter.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/formatter.py)

Governed v2 path:

- [engine.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/v2/engine.py)
- [parser.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/v2/parser.py)
- [resolver.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/v2/resolver.py)
- [contract.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/v2/contract.py)
- [governance.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/v2/governance.py)
- [writer.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/v2/writer.py)

Semantic helpers:

- [query_frame.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/query_frame.py)
- [semantic_registry.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/semantic_registry.py)
- [metadata_answerer.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/metadata_answerer.py)

### Frontend

Root:

- [App.tsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/frontend/src/App.tsx)

Core pieces:

- [ChatArea.tsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/frontend/src/components/ChatArea.tsx)
- [Message.tsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/frontend/src/components/Message.tsx)
- [DatasetLibrary.tsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/frontend/src/components/DatasetLibrary.tsx)
- [ChatbotMapButton.tsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/frontend/src/components/ChatbotMapButton.tsx)
- [ChatbotMapModal.tsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/frontend/src/components/ChatbotMapModal.tsx)
- [ChatbotMapRenderer.tsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/frontend/src/components/ChatbotMapRenderer.tsx)
- [VegaChart.tsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/frontend/src/components/VegaChart.tsx)

## 7. What We Tried in the Current App

The current project went through multiple iterations:

1. Basic NL-to-SQL agent over DuckDB.
2. Added prompt engineering and repair loops.
3. Added metadata answering and schema rules.
4. Added map intents and map overlays.
5. Added a governed `v2` engine for covered query shapes.
6. Added benchmarks and a 500-question eval harness.
7. Added more follow-up logic, ambiguity handling, and composite-score handling.

This work was not worthless. A lot of useful assets exist now. But the system accreted behavior faster than it simplified.

## 8. What Went Right

### A. The data packaging is usable

The repo already has a workable local data stack:

- Excel source files
- converted Parquet tables
- manifest-driven DuckDB registration
- metadata-rich schema documentation

That is a solid foundation.

### B. The benchmark harness is valuable

Useful scripts and outputs:

- [benchmark_deepseek_direct_500.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/scripts/benchmark_deepseek_direct_500.py)
- [report_500_20260425T014121Z.md](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/reports/deepseek_500_eval/report_500_20260425T014121Z.md)

The latest 500-question report shows:

- 100% success rate
- 93.9 average score
- but still many `short`, `missing_sql`, and ranking coverage issues

That eval framework should be kept and strengthened.

### C. The v2 governed engine is directionally correct

The best idea in the current app is not the legacy NL-to-SQL path. It is the governed path that tries to:

- parse intent
- validate support
- compile known operators
- execute deterministic SQL
- narrate from trusted facts

That direction is correct.

### D. Tests exist

Key test file:

- [test_unit_backend.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/tests/test_unit_backend.py)

The test suite is not enough yet, but it gives a starting regression surface for:

- follow-ups
- ranking logic
- flow questions
- metadata answers
- composite score handling

## 9. What Went Wrong

### A. There are multiple competing brains

This is the main problem.

The app currently has:

- legacy free-form classification/planning/SQL generation
- query-frame heuristics
- semantic registry hints
- governed v2 parsing
- governed v2 resolution
- metadata-answer side paths

The result is not one system. It is several half-systems with inconsistent authority.

### B. Follow-up inheritance is too aggressive

Observed failure:

- User asks: `top 10 counties by funding`
- Then asks: `top counties with maximum crime`
- System reuses prior funding context and answers with federal spending

That is not a surface bug. That is an architectural flaw.

The current follow-up model over-inherits context before re-grounding the new question against the supported metric catalog.

In a rebuild, follow-up rules should be:

1. Try to parse the new utterance independently.
2. Only inherit missing slots.
3. Never inherit metric family if the new utterance introduces a conflicting metric concept.
4. If the new concept is unsupported, say so explicitly.

### C. Unsupported concepts are not handled cleanly enough

Example:

- `crime` is not a supported metric in the loaded datasets.

The correct behavior is:

- explicit unsupported answer
- optional nearest supported alternatives

The current behavior sometimes drifts into a prior metric or a generic “funding” fallback.

### D. SQL is still acting as a semantic crutch

Too much correctness is discovered after query generation.

The system should know before SQL:

- whether a metric exists
- whether the geography level is valid
- whether the year is valid
- whether the combination is legal
- whether a comparison spans incompatible units

### E. Metadata and runtime are out of sync

This is dangerous.

The semantic layer documents tables that runtime does not load. That means the planner can think in terms of tables that do not exist operationally.

### F. Answer writing is still too shallow

Even when the data result is right, answers often feel thin.

Typical problems:

- too short
- missing scope
- not enough narrative structure
- weak benchmark context
- insufficient explanation of assumptions

### G. Maps and charts were bolted on instead of being tied to the execution plan

The right visual should be a consequence of the resolved query contract.

Instead, map behavior has often been secondary and brittle.

### H. UI work outran reasoning quality

There was too much time spent on:

- sidebars
- dataset panels
- aesthetic churn

before the central reasoning loop was fully trustworthy.

The rebuild should invert that priority.

## 10. Concrete Failure Examples That Must Inform the Rebuild

### Example 1: Unsupported metric drift

Question:

- `top counties with maximum crime`

Current bad behavior:

- answers with federal spending rankings

Correct behavior:

- say crime is not in the current loaded datasets
- offer nearest supported families: poverty, financial constraint, direct payments, contracts, grants, etc.

### Example 2: Ambiguous money semantics

Question:

- `How much federal money goes to Maryland?`

Current system often assumes geography-level spending totals.
But user sometimes means fund flow.

Correct behavior:

- resolve ambiguity explicitly between:
  - total federal spending received in Maryland
  - subcontract flow into Maryland
  - specific channel such as grants or contracts

### Example 3: Rank semantics

The user reported rank-direction failures where the model treated bigger numeric rank labels as better.

Correct behavior:

- rank is ordinal
- smaller rank number is better unless explicitly phrased otherwise

This should be deterministic and enforced in the result semantics layer, not improvised in answer writing.

### Example 4: Follow-up compare failure

Question:

- `grants in maryland`
- then `compare Maryland vs Virginia`

Current bad behavior:

- sometimes repeats Maryland-only answer instead of carrying forward the metric and extending entities

Correct behavior:

- inherit metric and period
- expand entity scope
- execute governed compare operator

## 11. What the User Wants the Rebuild To Achieve

The next Codex instance should assume these requirements are firm:

### Reasoning requirements

- strong general reasoning
- good handling of messy natural language
- good handling of thousands of phrasings
- robust ambiguity management
- correct follow-up inheritance
- trustworthy calculations
- clear support boundaries

### Answer quality requirements

- detailed
- well-structured
- explicit assumptions
- explicit scope
- explicit methodology when relevant
- explicit alternatives when unsupported

### Data correctness requirements

- no silent metric substitution
- no unsupported year guessing
- no family leakage
- no unit mixing without an explicit rule
- no fake causal claims

### UX requirements

- sleek, minimal, professional
- aligned with the Maryland Opportunity dashboard aesthetic
- map and chart support when useful
- no irrelevant map forcing
- no clutter hiding weak answers

## 12. Files and Assets Worth Reusing

Keep and reuse:

- [data/uploads](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/uploads)
- [data/parquet](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/parquet)
- [data/schema/manifest.json](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/schema/manifest.json)
- [data/schema/metadata.json](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/data/schema/metadata.json)
- [metadata/datasets_manifest.json](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/metadata/datasets_manifest.json)
- [metadata/dashboard_registry.json](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/metadata/dashboard_registry.json)
- [reference/variable_dictionary_full.xlsx](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/reference/variable_dictionary_full.xlsx)
- [docs/processed_data_notes.md](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/docs/processed_data_notes.md)
- [tests/test_unit_backend.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/tests/test_unit_backend.py)
- [reports/deepseek_500_eval](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/reports/deepseek_500_eval)

Treat as reference, not as architecture to preserve:

- [agent.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/agent.py)
- [planner.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/planner.py)
- [classifier.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/classifier.py)

Treat as source material for semantics, not as final design:

- [query_frame.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/query_frame.py)
- [semantic_registry.py](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/semantic_registry.py)
- [app/v2](/Users/premalparagbhaishah/Desktop/ChatAgent/datasets/mop-agent/app/v2)

## 13. Recommended Rebuild Architecture

Build one authoritative pipeline:

1. `intent parser`
2. `semantic resolver`
3. `query contract compiler`
4. `operator executor`
5. `result package builder`
6. `narrator`
7. `visual recommender`

### A. Intent parser

Input:

- raw question
- normalized history state

Output:

- typed intent JSON

The LLM can help here, but output must be constrained.

### B. Semantic resolver

This should be deterministic and authoritative.

It should answer:

- which family
- which metric
- which geography level
- which year
- which operation
- whether the request is supported
- which slots are missing
- which alternatives are nearest if unsupported

### C. Query contract compiler

Compile the resolved request into a narrow internal contract such as:

- `lookup`
- `ranking`
- `compare`
- `trend`
- `cross_metric_compare`
- `flow_pair_total`
- `flow_share`
- `agency_breakout`
- `metadata_answer`

No free-form SQL generation should happen here.

### D. Operator executor

Each supported contract should have a deterministic SQL builder or dataframe executor.

This layer should own:

- SQL text
- joins
- grouping
- sorting
- rank semantics
- benchmark context

### E. Result package

Return structured facts, not prose first.

Example package fields:

- `status`
- `contract_type`
- `family`
- `metric`
- `unit`
- `scope`
- `assumptions`
- `sql`
- `rows`
- `statistics`
- `ranking_context`
- `methodology_notes`
- `map_intent`
- `chart_intent`

### F. Narrator

Use the LLM here.

The LLM should receive only the trusted result package and write:

- short answer
- key findings
- context
- interpretation
- caveats
- suggested follow-ups

### G. Visual recommender

Map/chart selection must derive from the query contract and result shape, not from loose post-hoc heuristics.

## 14. Strong Recommendation About LLM Usage

Use the LLM for:

- messy-language interpretation
- clarification writing
- answer narration
- methodology explanation
- offline eval and critique

Do not use the LLM as source of truth for:

- metric availability
- year legality
- geography legality
- rank direction
- unit safety
- joins
- SQL generation for common supported operators

## 15. Rebuild Rules

The replacement system should enforce these rules early:

1. Unsupported metric names must not inherit prior supported metrics.
2. Follow-ups inherit only missing slots, never conflicting semantics.
3. Every answer must identify family, geography, year, metric, and assumption.
4. Every ranking answer must include returned rank logic and result scope.
5. Every ambiguous “money/funding/spending” query must resolve the intended family.
6. Every unsupported request must offer nearest supported alternatives.
7. Every chart/map must be tied to the resolved contract.
8. Metadata answers should be first-class, not second-class fallback prose.

## 16. Suggested Rebuild Sequence

### Phase 1: semantic core

- build a single semantic registry
- align runtime tables and documented tables
- define supported metric aliases and units
- define supported operator contracts

### Phase 2: deterministic executor

- implement lookup
- implement ranking
- implement compare
- implement trend
- implement core flow operators
- implement metadata availability operators

### Phase 3: narrator and response package

- structured response schema
- LLM-backed answer writing from trusted facts
- better follow-up suggestions

### Phase 4: visual layer

- map intent from contract
- chart intent from contract
- only add visuals for supported result shapes

### Phase 5: eval hardening

- preserve current 500-question benchmark
- add failure cases from real user transcripts
- add unsupported-metric tests
- add ambiguity tests
- add follow-up conflict tests

## 17. Minimum Success Criteria For The Rebuild

The rebuild is not done until:

1. Unsupported metrics like `crime` fail safely and clearly.
2. Follow-up compare questions reliably inherit the right metric.
3. Ambiguous “money/funding” questions are resolved instead of guessed.
4. Ranking semantics are deterministic.
5. Answer quality is detailed and structured by default.
6. Visuals appear only when the contract supports them.
7. The benchmark improves without gaming the metric.

## 18. Final Honest Summary

The current app is not empty effort.

It already contains:

- the data
- the schema
- the benchmark harness
- many useful business rules
- and a partially correct governed-engine direction

But it is still too fragmented to trust fully.

The next rebuild should not keep patching behavior across multiple agent paths.
It should consolidate around one semantic contract and one execution model.

If a future Codex session has to choose between:

- preserving current code shape
- or preserving current data and semantic assets

it should preserve the data and semantic assets, and be willing to replace most of the current orchestration code.
