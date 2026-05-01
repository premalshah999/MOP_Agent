# Operations Runbook

## Local Startup

```bash
pip install -r requirements.txt
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

In another terminal:

```bash
cd frontend
npm install
npm run dev
```

Backend: `http://127.0.0.1:8000`
Frontend: `http://127.0.0.1:5173`

## Build Checks

```bash
pytest -q
python -m app.evals.run_evals
python -m app.semantic.audit --format markdown
cd frontend
npm run typecheck
npm run build
```

## Docker Production Build

```bash
cp deploy/.env.production.example .env
# edit JWT_SECRET, ALLOWED_ORIGINS, and TRUSTED_HOSTS
docker compose build
docker compose up -d
curl http://127.0.0.1:8000/health
```

The production image serves the FastAPI backend and the built React frontend from one container. Runtime SQLite/DuckDB state lives in the `mop_agent_runtime` Docker volume.

## Hetzner Redeploy

On the Hetzner server, the expected app directory is `/opt/mop-agent`.

```bash
cd /opt/mop-agent
APP_DIR=/opt/mop-agent BRANCH=main bash deploy/redeploy.sh
```

The script:

- creates `.env` from `deploy/.env.production.example` when missing
- pulls the latest `main` branch using fast-forward only
- rebuilds and restarts the Docker Compose service
- checks `/health` before reporting success

## Key Env Knobs

- `APP_VERSION`: version string returned by `/health`.
- `JWT_SECRET`: required signing secret for login tokens.
- `SQLITE_DB_PATH`: SQLite auth/thread/message storage path.
- `DUCKDB_PATH`: runtime DuckDB path.
- `MAX_RETURN_ROWS`: maximum rows returned to the frontend.
- `QUERY_TIMEOUT_SECONDS`: execution budget used by the SQL executor.
- `ALLOWED_ORIGINS`: CORS allowlist.
- `TRUSTED_HOSTS`: allowed Host headers for FastAPI.
- `ASSISTANT_ROUTER_MODE`: `local` by default; `llm` enables optional OpenAI router.
- `OPENAI_API_KEY`: optional, only needed when `ASSISTANT_ROUTER_MODE=llm`.

## Troubleshooting

- `Health endpoint shows frontend_built=false`
  - Run `cd frontend && npm run build`, or rebuild the Docker image.
- `Blocked by trusted host middleware`
  - Add the public domain to `TRUSTED_HOSTS`.
- `CORS failure in browser`
  - Add the frontend origin to `ALLOWED_ORIGINS`.
- `No registered views`
  - Confirm `data/schema/manifest.json` and `data/parquet/*.parquet` are present.
- `Weak answer or wrong semantic resolution`
  - Run `python -m app.semantic.audit --format markdown` and add coverage to the semantic registry instead of adding question-specific branches.
