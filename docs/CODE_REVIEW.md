# Code Review Summary

## High Severity (fixed)

1. SQL CTE truncation in extraction logic
- Issue: SQL parser extracted from first `SELECT`, which dropped leading `WITH` clauses.
- Impact: valid generated SQL became invalid and caused parser errors.
- Fix: preserve earliest `WITH` when present before `SELECT`.

2. Single-shot SQL correction
- Issue: one correction attempt was not robust for production traffic.
- Impact: transient or malformed SQL responses surfaced directly to users.
- Fix: added configurable multi-attempt SQL repair loop with model fallback order.

## Medium Severity (fixed)

1. Frontend integration build gaps
- Issue: missing Vite alias and missing `ImportMeta.env` typing.
- Fix: added alias in `vite.config.ts` and Vite client types in TS config.

2. Mobile usability
- Issue: sidebar-only desktop layout did not degrade cleanly on small screens.
- Fix: added responsive mobile drawer and open/close controls.

## Low Severity (fixed)

1. Redundant scaffolding artifacts
- Removed stale Vite default assets and legacy `logs/` runtime directory.
- Replaced template frontend README with project docs.
- Expanded root docs for architecture and operations.

## Residual risks

- Quality is still prompt-sensitive; continue tuning few-shots with query logs.
- LLM latency can vary; for strict SLOs add async job handling or streaming.
- Current tests are smoke-level; add automated API regression tests before deployment.
