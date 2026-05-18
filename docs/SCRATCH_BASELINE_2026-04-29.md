# Scratch Baseline

Date: 2026-04-29

The application logic was intentionally removed, then the first controlled analytics rebuild was started from that baseline.

## Kept

- Dataset files under `data/`
- Runtime schema and semantic metadata under `data/schema/`
- Existing tests under `tests/`
- Existing reports under `reports/`
- Rebuild documentation under `docs/`
- Frontend source as design/reference material

## Deleted

- `app` implementation logic
- `scripts` implementation/benchmark/conversion code
- `deploy` scripts
- generated runtime databases, caches, and frontend build output

## Why

The prior system had too many competing reasoning paths. The next implementation should not preserve that architecture. It should rebuild around one semantic contract, one resolver, one executor model, and one response package format.

## Current Note

The repository is runnable again with a new production-MVP architecture. The new system is intentionally small and strict: semantic registry, retrieval, query plan, validators, generated SQL, DuckDB execution, result verification, and grounded deterministic answers.
