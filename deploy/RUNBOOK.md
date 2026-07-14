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

## Backups (TODO — not yet automated)

Critical paths to back up daily:
- `data/runtime/mop.sqlite3` (users, threads, messages, share tokens, feedback)
- `data/runtime/query_log.jsonl*` (audit + analytics signal)
- `data/runtime/feedback.jsonl*`

Recommended: cron job that runs `sqlite3 mop.sqlite3 ".backup /tmp/backup.db"`
then `rclone copy` (or `aws s3 cp`) to off-host storage.

## Concurrency notes (40–50 users)

| Component | Bottleneck | Mitigation |
|---|---|---|
| SQLite (threads/messages/feedback) | Write lock under contention | WAL mode enabled at init (see `app/storage/sqlite.py`); 10s busy timeout |
| DuckDB (analytical SQL) | Per-request fresh conn | Fine for read-only workload; views materialised at startup |
| DeepSeek LLM | Single API key | OpenAI fallback wired in `app/llm/client.py`; honours 429 with backoff |
| `lru_cache` on `distinct_values` | Eviction under diverse queries | Increased `maxsize` to 1024 |
| Long queries blocking workers | Reasoning mode can take 30–60s | TODO: async queue — see open work |

## Known data limitations (surface in About page)

- `mart_finra_county` lacks Baltimore City (only 21 of MD's 24+1 county-equivalents present).
  Other county tables (`acs_county`, `contract_county`, `gov_county`) have it.
- Crime, GDP, private-sector employment, historic housing prices: out of scope.
- Most tables peak at FY2023; some FINRA series only at 2021 survey wave.
