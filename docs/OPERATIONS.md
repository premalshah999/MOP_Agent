# Operations Runbook

## Local startup

```bash
/usr/bin/python3 scripts/convert_excel.py
/usr/bin/python3 -m uvicorn app.main:app --reload --port 8000
```

In another terminal:

```bash
cd frontend
npm run dev
```

## Local production deployment

```bash
/usr/bin/python3 -m compileall app scripts tests
/usr/bin/python3 -m unittest tests.test_unit_backend tests.test_http_surface tests.test_production_smoke -v
cd frontend && npm run check && cd ..
/usr/bin/python3 scripts/run_local_prod.py --host 127.0.0.1 --port 8000
```

## Build checks

```bash
/usr/bin/python3 -m compileall app scripts tests
/usr/bin/python3 -m unittest tests.test_unit_backend tests.test_http_surface -v
cd frontend
npm run typecheck
npm run build
```

## Health and smoke tests

```bash
curl http://127.0.0.1:8000/health

curl -X POST http://127.0.0.1:8000/api/ask \
  -H "Content-Type: application/json" \
  -d '{"question":"Which states have the highest total liabilities per capita? Show top 10.","history":[]}'
```

## Key env knobs

- `DEEPSEEK_API_KEY`: required for classifier/sql/formatter LLM calls.
- `APP_VERSION`: version string returned by `/health`.
- `SQL_MAX_TOKENS`: SQL generation budget.
- `SQL_REPAIR_ATTEMPTS`: number of post-failure repair loops.
- `SQL_REPAIR_MODELS`: comma-separated fallback order.
- `SQL_PREFLIGHT_ENABLED`: run `EXPLAIN` preflight before execution to catch binder/parser issues early.
- `FORMATTER_MAX_TOKENS`: max analytical answer depth.
- `CONCEPTUAL_MAX_TOKENS`: max depth for conceptual/explanatory answers.
- `MIN_ANALYTIC_WORDS`: minimum length floor for non-error analytical answers.
- `FORMATTER_PREVIEW_ROWS`: row sample passed to formatter.
- `MAX_RETURN_ROWS`: rows sent to frontend table preview.
- `DEFAULT_TOP_K`, `MAX_TOP_K`: ranking defaults.
- `ALLOWED_ORIGINS`: CORS allowlist.
- `TRUSTED_HOSTS`: allowed host headers for FastAPI.
- `GZIP_MINIMUM_SIZE`: minimum response size for gzip compression.

## Troubleshooting

- `HTTP 500 / parser errors`
  - Check `data/query_log.jsonl` for the failing SQL and error.
  - Confirm `manifest.json` reflects current uploads.
  - Increase `SQL_REPAIR_ATTEMPTS` or adjust few-shot examples.
- `No tables registered`
  - Re-run `python scripts/convert_excel.py`.
  - Ensure expected filenames exist in `data/uploads`.
- `Frontend can’t connect`
  - Verify backend listening on `127.0.0.1:8000`.
  - In dev mode, keep Vite proxy defaults or set `VITE_API_BASE_URL`.
- `Health endpoint shows frontend_built=false`
  - Rebuild with `cd frontend && npm run build`.
- `Blocked by trusted host middleware`
  - Add the host to `TRUSTED_HOSTS` in `.env`.
