# Architecture

## System Overview

1. User asks a question in the React chat UI.
2. Frontend calls `POST /api/ask` with question + chat history.
3. Backend classifies intent (`DATA_QUERY`, `FOLLOWUP`, `CONCEPTUAL`).
4. For data queries, agent builds schema context and generates DuckDB SQL with DeepSeek.
5. SQL is validated by safety rules and executed on in-memory DuckDB views over Parquet.
6. Result rows are formatted into an analytical response with DeepSeek formatter.
7. Backend returns answer + SQL + row preview + row count.

## Components

- `app/main.py`
  - FastAPI app, CORS, startup table registration, SPA static serving.
- `app/db.py`
  - Reads `manifest.json`, registers Parquet-backed DuckDB views, executes SQL.
- `app/agent.py`
  - NL-to-SQL flow, table routing, SQL safeguards, repair retries, serialization.
- `app/classifier.py`
  - Intent classification with LLM + fallback heuristics.
- `app/formatter.py`
  - Long-form analytical answer generation with deterministic fallback context.
- `app/safety.py`
  - Read-only SQL guardrail.
- `app/query_logger.py`
  - JSONL logging of every request outcome.
- `scripts/convert_excel.py`
  - Converts Excel source files to Parquet and generates `manifest.json`.

## Data Contract

- `data/schema/metadata.json`: semantic schema and critical SQL warnings.
- `data/schema/manifest.json`: generated map of registered tables and column lists.
- `data/parquet/*.parquet`: runtime data source for DuckDB views.

## Reliability Controls

- CTE-safe SQL extraction (preserves `WITH ... SELECT ...`).
- SQL safety validator blocks mutating statements.
- Multi-model SQL repair retries when execution fails.
- JSON-safe result serialization (`NaN`/`Inf` -> `null`).
- Query logging for iterative prompt and few-shot tuning.
