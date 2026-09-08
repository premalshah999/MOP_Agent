# Maryland Opportunity Analytics Assistant

A natural-language assistant over a fixed catalog of US public-policy datasets
(Census ACS demographics, state/local government finance, federal
contracts/grants/spending incl. by agency, FINRA financial-health indices, and
federal subaward flows) at state, county, and congressional-district level.

The application is an LLM-grounded text-to-SQL assistant. Every analytical
answer uses the curated catalog (`data/schema/metadata.json`) and live DuckDB
value lookups, then passes structural, semantic, and evidence checks:

```text
Chat API
  -> conversation context     (resolve follow-ups without losing the current question)
  -> semantic planner         (intent, datasets, variables, operation, geography, period)
  -> plan verifier            (recover false absences and reject impossible plans)
  -> typed analysis plan      (metric, entity, period, direction, limit, result shape)
  -> grounding                (schema, warnings, and resolved live filter values)
  -> SQL generation           (DuckDB SQL + self-repair loop)
            -> structural + semantic validators -> DuckDB executor
  -> grounded response        (strictly from returned rows)
  -> blocking faithfulness    (repair/recheck; otherwise render validated rows only)
```

Non-analytical messages never touch the database: META/UNANSWERABLE get a
grounded explanation, CLARIFY asks one question back, OUT_OF_SCOPE is declined.

## LLM provider (required)

Set `LLM_PROVIDER` to `deepseek`, `gemini`, or `openai`, then set that
provider's dedicated key and model variables. The production environment is
currently expected to select DeepSeek explicitly; Gemini remains an available
cutover switch. Without a working provider the app still boots for diagnostics,
but analytical questions report that the analysis service is unavailable. The
client also supports recorded fixtures (`LLM_MODE=fixture`) and an injectable
stub for fully offline tests.

The analytical contract and validators are provider-neutral. Every provider or
model change must pass the full release gates before deployment. Reasoning mode
also preserves Gemini thought signatures between tool calls.

Model-written prose is never streamed before verification. If two verification
attempts cannot establish that the prose matches the evidence, the assistant
does not guess or silently refuse a valid query: it displays only the already
validated result rows directly, without adding an unverified interpretation.

## Backend layout

```text
app/
  api/         auth, datasets, feedback, maps, and thread endpoints
  core/        conversation, planner, plan verifier, analysis plan, grounding,
               query engine, response writer, pipeline, reasoning, visuals
  duckdb/      manifest-driven view registration and query execution
  evals/       golden, held-out, conversation, reasoning, and repeatability gates
  llm/         provider-neutral client (live + fixture + stub modes)
  quality/     blocking answer-faithfulness checks
  schemas/     validated response models
  semantic/    registry (metadata.json catalog), value_resolver (live DuckDB)
  sql/         structural + semantic validators (read-only, allow-listed)
  storage/     SQLite persistence
  main.py      FastAPI composition and frontend serving
```

## Install & run

```bash
pip install -r requirements-dev.txt
cp .env.example .env          # select LLM_PROVIDER and set its API key
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000

cd frontend && npm install && npm run dev
```

Backend `http://127.0.0.1:8000` · Frontend `http://127.0.0.1:5173`

## Test & evaluate

```bash
pytest -q                          # per-stage suites (live tests skip w/o key)
python -m app.evals.run_evals --suite both  # golden + held-out live gate
python -m app.evals.repeatability           # identical-query evidence gate
python -m app.evals.conversation_eval --mode normal
python -m app.evals.conversation_eval --mode reasoning
python -m app.evals.reasoning_eval            # complex multi-step quality gate
python -m app.semantic.audit --format markdown
cd frontend && npm run check
```

Release gates require 100% intent, routing, generation, and evidence
faithfulness across the golden + held-out corpus. The reasoning gate requires
every task to score at least 9/10, and critical queries must remain semantically
identical across five independent generations. This is a zero-silent-error
standard: an unverified result must never be presented as a verified answer.

## Production build

```bash
cp deploy/.env.production.example .env   # set JWT_SECRET, provider key, hosts
docker compose build && docker compose up -d
curl http://127.0.0.1:8000/health/deep
```

The image runs FastAPI and serves the built React app from `frontend/dist`.
Committed runtime assets are the curated Parquet tables, schema metadata, map
boundaries, and raw uploads; generated state stays in the `mop_agent_runtime`
volume. `deploy/redeploy.sh` creates a protected backup before each deployment
and restores the prior image if deep health checks fail.

## Behaviour examples

- `top 10 counties in maryland by grants` → `contract_county`, UPPERCASE state
  normalised, `year = '2024'`, ranked table answer.
- `what is the debt ratio for Texas` → `gov_state`, **no** year filter (single
  FY2023 snapshot).
- `Maryland congressional districts by free cash flow` → `gov_congress` (cash
  flow is government finance, **not** the subaward-flow tables).
- `epartment of defence biggest deals by state` → `spending_state_agency`,
  agency resolved to `Department of Defense`, "deals" → contracts.
- `How much federal money goes to Maryland?` → asks which channel (clarify).
- `top counties with the maximum crime rate` → declined (not in catalog) with
  the nearest available metrics.

## Design rule

Correctness comes from the catalog and the grounding pack, not from one-off code
branches. Improve `metadata.json`, the resolved-value layer, and the stage
prompts first; keep the SQL validator strict and the faithfulness judge honest.
The reviewed SQL examples in `app/evals/analyst_queries.yaml` are offline test
material only and never bypass the production planner or query engine.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Development and release checks](docs/DEVELOPMENT.md)
- [Operations](docs/OPERATIONS.md)
- [Gemini cutover](docs/GEMINI_CUTOVER.md)
