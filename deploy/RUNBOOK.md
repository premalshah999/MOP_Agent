# Production Runbook

Operator-side checklist for keeping the MOP Agent box healthy when 40–50
people are using it at once.

## Monitoring

### UptimeRobot (free, no code change)
1. Sign up at https://uptimerobot.com (free tier = 50 monitors @ 5-min interval).
2. **Primary monitor** — `http://<prod-host>/health/deep`. Returns 503 when
   any backend dep is down: LLM (DeepSeek key missing / revoked / rate-limited
   to dead), DuckDB, or SQLite. This catches the silent-failure mode where
   the process is up but every analytical question goes to clarification.
   - Interval: 5 minutes
   - Alert contacts: your email; optionally Slack/Discord webhook
3. **Secondary** (cheap) — `http://<prod-host>/health` for "is the process
   even responding?" shallow check. Useful if `/health/deep` flaps under load.

Alerting threshold: 2 consecutive failures (so a one-off blip doesn't page).

### Sentry (errors + traces)
Set `SENTRY_DSN` in `deploy/.env.production`. Free tier covers 5K events/month
which is plenty at this scale. Already wired in `app/main.py` — env-gated, so
unset DSN = no-op.

## Log rotation

In-process rotation is on by default (`app/observability/logging.py`).
- Rotates `query_log.jsonl` and `feedback.jsonl` when each crosses
  `LOG_ROTATE_MAX_BYTES` (default 10MB).
- Keeps `LOG_ROTATE_KEEP` archives (default 5, FIFO prune).
- Archive naming: `query_log-YYYYmmdd-HHMMSS.jsonl`

At 40–50 users firing ~5 queries/session, expect 1–2 rotations per week.
No cron / logrotate required.

To inspect retention: `ls -lh data/runtime/*.jsonl`.

## Backups

Every deployment runs `deploy/backup.sh` before fetching or building. The
archive contains a transactionally consistent SQLite snapshot, rotating query
and feedback logs, the protected environment file, the current Git commit and
patch, and a repository bundle. Archives are stored in
`/opt/mop-agent-backups` with mode `0600`; the default on-host retention is 14
days.

Run an additional snapshot manually with:

```bash
APP_DIR=/opt/mop-agent bash deploy/backup.sh
```

Copy these archives to approved off-host storage daily. On-host snapshots
protect deployment rollback, but they do not protect against host loss.

## Concurrency notes (40–50 users)

| Component | Bottleneck | Mitigation |
|---|---|---|
| SQLite (threads/messages/feedback) | Write lock under contention | WAL mode enabled at init (see `app/storage/sqlite.py`); 10s busy timeout |
| DuckDB (analytical SQL) | Per-request fresh conn | Fine for read-only workload; views materialised at startup |
| DeepSeek LLM | Single API key | OpenAI fallback wired in `app/llm/client.py`; honours 429 with backoff |
| `lru_cache` on `distinct_values` | Eviction under diverse queries | Increased `maxsize` to 1024 |
| Long analytical requests | Additional evidence checks can take longer | Hard wall/tool/token budgets plus four web workers |

## Model-provider cutover gate

Provider changes are release changes, not environment-only substitutions.
Before changing the model or base URL, run the golden, held-out,
repeatability, reasoning, and multi-turn suites with the candidate provider:

```bash
python -m app.evals.run_evals --suite both
python -m app.evals.repeatability --repeats 5
python -m app.evals.reasoning_eval
python -m app.evals.conversation_eval --mode normal
python -m app.evals.conversation_eval --mode reasoning
```

Do not cut over if any suite regresses. DeepSeek's official documentation says
the `deepseek-chat` alias is scheduled for deprecation on July 24, 2026; either
validate a current DeepSeek model or complete the tested Gemini cutover before
that date. Gemini's OpenAI-compatible endpoint supports the interface used by
this application, but semantic correctness still depends on these gates. See
the official [DeepSeek model documentation](https://api-docs.deepseek.com/quick_start/pricing)
and [Gemini OpenAI compatibility guide](https://ai.google.dev/gemini-api/docs/openai).

## Known data limitations (surface in About page)

- `mart_finra_county` lacks Baltimore City (only 21 of MD's 24+1 county-equivalents present).
  Other county tables (`acs_county`, `contract_county`, `gov_county`) have it.
- Crime, GDP, private-sector employment, historic housing prices: out of scope.
- Most tables peak at FY2023; some FINRA series only at 2021 survey wave.
