# MOP Controlled Analytics Assistant

This is a clean rebuild of the MOP assistant around a controlled multi-mode analytics architecture.

The backend is not a generic LLM-to-DuckDB chatbot. It first routes the user message into the right assistant workflow, then uses the controlled SQL pipeline only when the workflow actually needs SQL:

```text
Chat API
  -> conversation state manager
  -> assistant router
  -> workflow selector
  -> help / dataset discovery / metric definition / analytics workflow
  -> intent classifier
  -> semantic retrieval
  -> query planner
  -> plan validator
  -> SQL generator
  -> SQL validator
  -> DuckDB executor
  -> result verifier
  -> grounded answer generator
```

## What Is Preserved

- `data/uploads/` raw Excel sources
- `data/parquet/` runtime Parquet tables
- `data/boundaries/` map boundary assets
- `data/schema/manifest.json`
- `data/schema/metadata.json`
- `reports/` benchmark/evaluation artifacts
- `docs/` rebuild notes and handoff material
- `frontend/` visual/design source

## New Backend Layout

```text
app/
  api/                 # FastAPI-facing auth, dataset, thread helpers
  core/                # Router, conversation state, orchestrator, planner, verifier, answer writer
  duckdb/              # Curated DuckDB view registration and execution
  evals/               # Golden questions and regression runner
  observability/       # Structured JSONL pipeline logs
  schemas/             # Pydantic contracts between stages
  semantic/            # Metadata registry, retrieval, plan validation
  sql/                 # SQL generation, validation, execution adapter
  storage/             # SQLite auth/thread/message storage
  main.py              # FastAPI application
```

## Install

```bash
pip install -r requirements.txt

cd frontend
npm install
cd ..
```

## Run Backend

```bash
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

## Run Frontend

```bash
cd frontend
npm run dev
```

Backend: `http://127.0.0.1:8000`

Frontend: `http://127.0.0.1:5173`

## Test

```bash
pytest tests/test_database.py tests/test_unit_backend.py tests/test_http_surface.py tests/test_production_smoke.py -q
python -m app.evals.run_evals
cd frontend && npm run typecheck
```

## Production Build

```bash
cp deploy/.env.production.example .env
# edit JWT_SECRET, ALLOWED_ORIGINS, and TRUSTED_HOSTS
docker compose build
docker compose up -d
curl http://127.0.0.1:8000/health
```

The Docker image runs FastAPI and serves the built React app from `frontend/dist`. The committed runtime assets are the curated Parquet tables, schema metadata, map boundaries, and raw uploads for dataset downloads. Generated runtime state stays in the `mop_agent_runtime` Docker volume.

## Semantic Audit

Run the coverage audit whenever data, metadata, or metric definitions change:

```bash
python -m app.semantic.audit --format markdown
```

The audit compares the runtime manifest, semantic metadata, and in-memory registry. It reports documented-but-not-loaded tables, loaded-but-undocumented columns, metric/dimension coverage, semantic variant groups, and registry quality warnings. This is the first robustness gate: new behavior should come from better metadata coverage, not one-off planner branches.

## Current Capability

The controlled slice now supports:

- assistant help / identity answers
- dataset discovery
- metric definitions
- rankings
- direct lookups
- comparisons
- trends
- agency breakouts
- fund-flow inflow/outflow rankings
- metadata/availability answers
- unsupported metric refusal with alternatives
- ambiguity clarification for broad federal-money questions
- follow-up inheritance for missing slots only
- metric variant switching from registry metadata, such as amount/count vs share/ratio vs per-capita/per-1,000

The assistant router can run in two modes:

- `ASSISTANT_ROUTER_MODE=local`: offline semantic router used by tests and local development.
- `ASSISTANT_ROUTER_MODE=llm`: optional LLM router through `DEEPSEEK_API_KEY` and the OpenAI-compatible `ASSISTANT_ROUTER_BASE_URL`, with local fallback if the model call is unavailable.

```text
top 10 counties in maryland with maximum funding
```

is resolved as:

- dataset: `contract_county`
- metric: `total_federal_funding`
- filter: `state = Maryland`
- default period: `2024`
- executor: validated SQL over `mart_contract_county`

The response includes:

- final answer
- SQL
- rows
- chart intent/spec where useful
- map intent where the contract supports it
- `resultPackage`
- `contract`
- `pipelineTrace`
- quality warnings/confidence

Important regression examples:

```text
top counties with maximum crime
```

returns unsupported with nearest supported alternatives.

```text
How much federal money goes to Maryland?
```

asks for clarification instead of silently choosing a funding family.

```text
the first one
```

after that clarification resolves back to Maryland and returns the scoped total funding value, not a national ranking.

```text
grants in maryland
compare Maryland vs Virginia
```

inherits grants and the period into the follow-up comparison.

## Design Rule

Future work should extend the semantic registry and deterministic contracts first. UI polish and answer styling come only after the resolver, validator, executor, and evaluator are strong.
