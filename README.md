# MOP Research Agent

Production-oriented chat agent for Maryland Opportunities Platform datasets.

- Backend: FastAPI + DuckDB + DeepSeek (OpenAI-compatible API)
- Frontend: React + TypeScript + Vite + Tailwind v4
- Data pipeline: Excel uploads -> Parquet -> DuckDB views

## Repository Layout

```text
mop-agent/
├── app/                     # FastAPI API + NL-to-SQL agent
├── data/
│   ├── uploads/             # Raw Excel files
│   ├── parquet/             # Generated parquet tables (gitignored)
│   ├── schema/
│   │   ├── metadata.json    # Table/column semantics and warnings
│   │   └── manifest.json    # Generated conversion manifest
│   └── query_log.jsonl      # Runtime query logs (gitignored)
├── frontend/                # React client integrated with backend API
├── scripts/
│   ├── convert_excel.py     # Excel -> parquet converter
│   └── run_local_prod.py    # Local production launcher
├── tests/
│   ├── test_http_surface.py
│   ├── test_production_smoke.py
│   └── test_unit_backend.py
├── docs/
│   ├── ARCHITECTURE.md
│   ├── OPERATIONS.md
│   └── CODE_REVIEW.md
├── .env.example
└── requirements.txt
```

## Setup

### 1) Install dependencies

```bash
# from mop-agent/
pip install -r requirements.txt

cd frontend
npm install
cd ..
```

### 2) Configure environment

```bash
cp .env.example .env
```

Set `DEEPSEEK_API_KEY` in `.env`.

### 3) Convert data

```bash
/usr/bin/python3 scripts/convert_excel.py
```

### 4) Run development services

Backend:

```bash
/usr/bin/python3 -m uvicorn app.main:app --reload --port 8000
```

Frontend:

```bash
cd frontend
npm run dev
```

- Frontend: `http://127.0.0.1:5173`
- Backend: `http://127.0.0.1:8000`

## Production Checks

```bash
/usr/bin/python3 -m compileall app scripts tests
/usr/bin/python3 -m unittest tests.test_unit_backend tests.test_http_surface tests.test_production_smoke -v
cd frontend && npm run check && cd ..
```

## Local Production Mode

```bash
/usr/bin/python3 scripts/run_local_prod.py --host 127.0.0.1 --port 8000
```

FastAPI serves the built SPA from `frontend/dist` and the API from `/api/*`. The launcher builds the frontend first, then starts Uvicorn without reload.

## API

- `GET /health`
- `POST /api/ask`

Example:

```bash
curl -X POST http://127.0.0.1:8000/api/ask \
  -H "Content-Type: application/json" \
  -d '{"question":"Which states have the highest total liabilities per capita? Show top 10.","history":[]}'
```

## Tests

```bash
/usr/bin/python3 -m unittest tests.test_http_surface -v
/usr/bin/python3 -m unittest tests.test_unit_backend -v
/usr/bin/python3 -m unittest tests.test_production_smoke -v
cd frontend && npm run check && cd ..
```

## Quality and Reliability Notes

- API responses include request IDs, security headers, gzip compression, and trusted-host enforcement.
- `/health` reports manifest presence, registered table count, and frontend build readiness.
- SQL extraction supports both `WITH` and `SELECT` queries without truncating CTEs.
- Failed SQL is auto-repaired with configurable retries (`SQL_REPAIR_ATTEMPTS`, `SQL_REPAIR_MODELS`).
- SQL preflight linting/binding checks run before execution (`SQL_PREFLIGHT_ENABLED`) to catch set-op and window/group errors early.
- Ranking questions default to analytical result depth (`DEFAULT_TOP_K`, `MAX_TOP_K`).
- Formatter generates long-form analyst answers with dynamic word targets and evidence, with a minimum floor (`MIN_ANALYTIC_WORDS`).
- Conceptual responses can be expanded with `CONCEPTUAL_MAX_TOKENS`.
- Runtime env defaults also support `APP_VERSION`, `TRUSTED_HOSTS`, and `GZIP_MINIMUM_SIZE`.

See `docs/` for architecture, ops runbook, and review notes.
