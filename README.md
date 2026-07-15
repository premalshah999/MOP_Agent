# Maryland Opportunity Analytics Assistant — LLM-grounded text-to-SQL

A natural-language assistant over a fixed catalog of US public-policy datasets
(Census ACS demographics, state/local government finance, federal
contracts/grants/spending incl. by agency, FINRA financial-health indices, and
federal subaward flows) at state, county, and congressional-district level.

The pipeline is an **LLM-grounded text-to-SQL** system. Every analytical answer
is grounded in the curated catalog (`data/schema/metadata.json`) and in live
DuckDB value lookups, then verified:

```text
Chat API
  -> Stage 1  intent        (ANALYTICAL | CLARIFY | UNANSWERABLE | META | OUT_OF_SCOPE)
  -> Stage 2  routing        (pick the exact catalog table(s) — the critical gate)
  -> analysis contract       (operation, metric, entity, period, direction, limit)
  -> Stage 3  retrieval      (schema + critical warnings + live resolved filter values)
  -> Stage 4  SQL generation (DuckDB SQL + self-repair loop)
            -> structural + semantic validators -> DuckDB executor
  -> Stage 4  grounded answer (strictly from returned rows)
  -> blocking verification    (repair/recheck; otherwise render validated rows only)
```

Non-analytical messages never touch the database: META/UNANSWERABLE get a
grounded explanation, CLARIFY asks one question back, OUT_OF_SCOPE is declined.

## LLM provider (required)

DeepSeek (OpenAI-compatible) is the current local provider. Set
`DEEPSEEK_API_KEY` in `.env`. Without a working provider the
app still boots for diagnostics, but analytical questions report that the
analysis service is unavailable. The client also supports recorded fixtures
(`LLM_MODE=fixture`) and an injectable stub for fully offline tests.

The analytical contract and validators are provider-neutral. Gemini can use
its OpenAI-compatible endpoint through environment configuration, but every
provider or model change must pass the full release gates before deployment.

Model-written prose is never streamed before verification. If two verification
attempts cannot establish that the prose matches the evidence, the assistant
does not guess or silently refuse a valid query: it displays only the already
validated result rows directly, without adding an unverified interpretation.

## Backend layout

```text
app/
  core/        intent, router, typed analysis contract, grounding, sql_writer,
               answer_writer, meta_answer, orchestrator
  llm/         DeepSeek client (live + fixture + stub modes)
  semantic/    registry (metadata.json catalog), value_resolver (live DuckDB)
  sql/         structural + semantic validators (read-only, allow-listed)
  duckdb/      manifest-driven view registration
  evals/       golden + held-out references, repeatability, faithfulness
  api/ storage/ observability/ schemas/ main.py
```

## Install & run

```bash
pip install -r requirements.txt
cp .env.example .env          # set DEEPSEEK_API_KEY
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000

cd frontend && npm install && npm run dev
```

Backend `http://127.0.0.1:8000` · Frontend `http://127.0.0.1:5173`

## Test & evaluate

```bash
pytest -q                          # per-stage suites (live tests skip w/o key)
python -m app.evals.run_evals --suite both  # golden + held-out live gate
python -m app.evals.repeatability           # identical-query evidence gate
python -m app.evals.conversation_eval        # multi-turn gate across every family
python -m app.semantic.audit --format markdown
cd frontend && npm run typecheck && npm run build
```

Per-stage gates (golden set, graded against an independent reference DuckDB):
intent ≥ 90%, routing ≥ 90%, generation ≥ 85%, faithfulness ≥ 90%.

## Production build

```bash
cp deploy/.env.production.example .env   # set JWT_SECRET, DEEPSEEK_API_KEY, hosts
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
