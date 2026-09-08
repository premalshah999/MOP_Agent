# Architecture

## Reliability model

The assistant has one production reasoning path. A user question cannot be
phrase-matched to fixed SQL or routed through an older classifier. Language
models interpret the request and write the response; typed contracts and
validators enforce invariants that must not vary between model runs.

```text
React client
  -> FastAPI endpoint (`app/main.py`)
  -> conversation context (`app/core/conversation.py`)
  -> semantic planner (`app/core/planner.py`)
  -> plan verifier (`app/core/plan_verifier.py`)
  -> typed analysis plan (`app/core/analysis_plan.py`)
  -> schema/value grounding (`app/core/grounding.py`)
  -> SQL generation and repair (`app/core/query_engine.py`)
  -> structural validator (`app/sql/validator.py`)
  -> semantic validator (`app/sql/semantic_validator.py`)
  -> DuckDB execution (`app/duckdb/connection.py`)
  -> response writer (`app/core/response_writer.py`)
  -> faithfulness gate (`app/quality/faithfulness.py`)
  -> visual and map specifications (`app/core/visuals.py`)
```

`app/core/pipeline.py` coordinates this sequence and emits the pipeline trace
returned by the API. Reasoning mode is an optional deeper execution mode, not a
second router: `app/core/reasoning.py` uses the same semantic catalog, safe SQL
execution, response schema, and final verification boundary.

## Evidence provenance

Every reasoning tool observation receives an immutable evidence id (`E1`,
`E2`, ...). A normal quantitative answer must name one primary evidence id and
every supporting id it actually used. The primary observation is the single
source for visible SQL, data rows, charts, and maps; verification receives only
the cited observations, while the complete tool trail remains available for
audit. Exploratory or failed queries can therefore never displace the result
that supports the answer merely because they ran later.

If row-producing evidence exists and the model supplies an invalid or missing
reference, the answer call is rejected and the reasoning loop must correct it.
Forced synthesis after a provider/budget failure retains a compatibility path,
but it cannot claim high confidence without validated rows.

## Determinism boundary

The semantic planner is the only component that interprets the user's
language. Once it emits a typed plan, downstream code must not reread the
question with regexes to change its operation, metric, scope, flow direction,
benchmark, or ordering. The schema verifier may repair an impossible plan, but
it must return another complete typed plan rather than trigger a hidden Python
interpretation.

Deterministic code is limited to facts that are mechanical and testable:

- read-only SQL and table allowlists;
- executing the filters, result shape, direction, and formula already selected
  in the typed plan;
- known units, periods, geography grain, and join keys from catalog metadata;
- response-schema validation and checking claims against returned rows;
- serialization, row limits, timeouts, and authentication.

It does not select an answer or create a second semantic plan. Curated
question/SQL pairs live under `app/evals/` solely as regression evidence. They
are not imported by the runtime pipeline. A new validator is justified only
when it enforces a provider-independent data invariant; a prompt or metadata
fix is preferred for language interpretation and analytical strategy.

## Data contract

- `data/schema/metadata.json` is the semantic source of truth: dataset purpose,
  variables, units, periods, aliases, join rules, and critical warnings.
- `data/schema/manifest.json` maps logical tables to Parquet files and columns.
- `data/parquet/*.parquet` is the read-only analytical store registered as
  DuckDB views at startup.
- `data/uploads/*` retains source workbooks used to produce the runtime tables.

Semantic changes should start in metadata and be covered by the audit and an
evaluation case. Query-specific Python branches are an architectural failure.

## Request outcomes

The planner returns one of five intents: `ANALYTICAL`, `CLARIFY`,
`UNANSWERABLE`, `META`, or `OUT_OF_SCOPE`. Only analytical requests reach SQL.
The response resolution distinguishes answered, answered with assumptions,
partially answered, clarification required, unsupported, no data, and error.

The application does not stream unverified prose. If model prose fails the
faithfulness check twice, the pipeline returns a plain rendering of the already
validated rows. It does not invent an explanation or discard valid evidence.

## Package responsibilities

```text
app/api/             HTTP feature endpoints
app/core/            conversational analysis workflow
app/duckdb/          database registration and execution
app/evals/           offline and live regression suites
app/llm/             provider transport and structured-output handling
app/observability/   structured request logging
app/quality/         evidence-to-answer quality gates
app/schemas/         Pydantic contracts
app/semantic/        dataset registry, discovery, and value resolution
app/sql/             SQL safety and semantic validation
app/storage/         user, thread, message, and feedback persistence
frontend/src/        React application
```

## Provider boundary

The planner, query engine, response writer, and faithfulness judge call the
provider-neutral client in `app/llm/client.py`. Provider selection is entirely
environment-driven. A provider change must not require changes to semantic or
SQL rules and must pass the same release gates.
