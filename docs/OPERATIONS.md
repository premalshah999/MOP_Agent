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
python -m app.evals.premium_eval --profile full
python -m app.evals.run_evals --suite both
python -m app.evals.repeatability --repeats 5
python -m app.evals.conversation_eval --mode normal
python -m app.evals.conversation_eval --mode reasoning
python -m app.evals.reasoning_eval
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
curl http://127.0.0.1:8000/health/deep
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
- blocks placeholder secrets, hosts, origins, or provider keys
- creates a protected pre-deployment backup of the database, logs, environment, and Git state
- verifies the current deep health before changing a running installation
- pulls the latest `main` branch using fast-forward only
- rebuilds and restarts the Docker Compose service
- checks both `/health` and `/health/deep` before reporting success
- restores the previous image if post-deployment health checks fail

## Key Env Knobs

- `APP_VERSION`: version string returned by `/health`.
- `APP_ENV`: must be `production` in Docker; enables startup validation.
- `JWT_SECRET`: required signing secret for login tokens.
- `SQLITE_DB_PATH`: SQLite auth/thread/message storage path.
- `DUCKDB_PATH`: runtime DuckDB path.
- `MAX_RETURN_ROWS`: maximum rows returned to the frontend.
- `QUERY_TIMEOUT_SECONDS`: execution budget used by the SQL executor.
- `ALLOWED_ORIGINS`: CORS allowlist.
- `TRUSTED_HOSTS`: allowed Host headers for FastAPI.
- `LLM_PROVIDER`: `deepseek`, `gemini`, `openai`, or legacy `auto`; use an explicit value in production.
- `DEEPSEEK_API_KEY` / `DEEPSEEK_MODEL`: DeepSeek configuration. Use an
  explicit V4 model; the legacy `deepseek-chat` and `deepseek-reasoner` aliases
  are retired and generate a non-blocking health warning.
- `DEEPSEEK_THINKING_MODE`: `enabled` or `disabled`. Keep disabled for an
  initial model migration, then benchmark thinking mode through every live
  gate before enabling it.
- `DEEPSEEK_REASONING_EFFORT`: `low`, `high`, or `max` when thinking is enabled.
- `DEEPSEEK_MIN_COMPLETION_TOKENS`: minimum output budget used in thinking mode.
- `GEMINI_API_KEY` / `GEMINI_MODEL`: Gemini configuration through Google's native `generateContent` endpoint.
- `GEMINI_REASONING_EFFORT`: Gemini thinking level (`low` is the initial evaluation setting).
- `GEMINI_MIN_COMPLETION_TOKENS`: protects Gemini 3 structured responses from being truncated by thinking tokens.
- `OPENAI_API_KEY` / `OPENAI_MODEL`: OpenAI configuration.
- `LLM_BASE_URL` / `LLM_MODEL`: optional explicit-provider overrides; normally leave unset.
- `ASSISTANT_ROUTER_BASE_URL` and `ASSISTANT_ROUTER_MODEL`: legacy overrides honored only in `auto` mode.

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
