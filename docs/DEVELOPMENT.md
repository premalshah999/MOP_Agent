# Development and Release Checks

## Local setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Run the frontend separately:

```bash
cd frontend
npm ci
npm run dev
```

## Required checks

Run the offline gates for every change:

```bash
python -m compileall -q app tests
ruff check app tests
ruff format --check app tests
pytest -q
python -m app.semantic.audit --format markdown
cd frontend && npm run check
git diff --check
```

When a configured LLM provider is available, also run:

```bash
python -m app.evals.premium_eval --profile demo
python -m app.evals.run_evals --suite both
python -m app.evals.repeatability --repeats 5
python -m app.evals.conversation_eval --mode normal
python -m app.evals.conversation_eval --mode reasoning
python -m app.evals.reasoning_eval
```

The premium command is the unified demo-readiness report. The individual
commands remain useful for focused diagnosis; see `docs/PREMIUM_EVALUATION.md`.

Provider billing or quota failures are infrastructure failures, not acceptable
evaluation results. Record them separately and rerun the live gates after the
provider is restored.

## Change policy

For a new dataset or variable:

1. Update the source workbook and generated Parquet/manifest as needed.
2. Add complete semantic metadata: meaning, unit, grain, period, aliases,
   valid joins, and cautions.
3. Run `python -m app.semantic.audit` until the catalog is clean.
4. Add representative questions to golden or held-out evaluation coverage.
5. Test follow-ups and close paraphrases, not only the reported wording.

For an answer-quality defect, fix the most general responsible layer: metadata,
value resolution, planning prompt, typed plan, SQL validator, or evidence gate.
Do not add phrase matching, fixed answers, or a parallel classifier. Reviewed
question/SQL examples belong in `app/evals/analyst_queries.yaml` and cannot be
imported from production modules.

Before adding deterministic logic, write down the invariant without referring
to a particular question. If it cannot be stated entirely in terms of schema,
types, explicit typed-plan fields, database safety, or returned evidence, it
belongs in the model prompt/evaluation loop instead. Never repair a typed plan
by parsing the user's prose again downstream.

## Naming

Modules are named for their current responsibility, not a historical stage:
`planner`, `analysis_plan`, `query_engine`, `response_writer`, `pipeline`, and
`faithfulness`. Preserve public API field names and telemetry stage identifiers
unless a versioned migration is planned.

## Pre-deployment checklist

- The working tree contains only intentional changes.
- Backend, frontend, and semantic audit gates pass.
- Live golden, held-out, repeatability, and conversation gates pass against the
  exact provider/model selected for production.
- Every runtime dataset is semantically certified and represented by at least
  one independent-reference golden or held-out case.
- The strict live gate is 100% across intent, routing, generation, and
  faithfulness; reasoning tasks each score at least 9/10.
- `.env` contains no placeholders and selects the provider explicitly.
- A current backup exists and the rollback path has been tested.
- `/health` and `/health/deep` pass before and after deployment.
