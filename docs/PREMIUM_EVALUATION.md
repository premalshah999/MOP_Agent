# Premium Evaluation and Demo Certification

The premium gate is the final pre-demo and pre-release test system. It does not
participate in answering user questions and cannot add phrase-specific behavior
to production. Its reviewed questions and SQL exist only to measure the agent.

## What it certifies

- **Independent factual correctness:** live answers are compared with separate,
  hand-reviewed DuckDB reference queries.
- **Paraphrase invariance:** several natural phrasings must route to the same
  datasets, measures, calculation, period, and returned evidence.
- **Conversational continuity:** follow-ups must retain or replace entities,
  measures, periods, rankings, and flow direction correctly.
- **Safe boundaries:** unsupported analysis, prompt injection, destructive SQL,
  secret extraction, and arbitrary file access must produce no SQL.
- **Repeatability:** identical questions must preserve semantic contracts and
  evidence even if prose varies.
- **Reasoning quality:** the full profile grades groundedness, completeness,
  usefulness, and hallucination resistance at a strict 9/10 per task.
- **Catalog integrity and performance:** every dataset must remain semantically
  certified, while premium probes stay inside declared p95 and maximum latency
  budgets.

Provider/quota failures and product-quality failures are reported separately.
Both block release, but only product failures indicate a semantic regression.

## Profiles

```bash
# Representative paraphrase, safety, catalog, and latency probes.
python -m app.evals.premium_eval --profile smoke

# Two-day demo gate: all premium probes, all independently referenced factual
# cases, two-run repeatability, and normal multi-turn conversations.
python -m app.evals.premium_eval --profile demo

# Final production certification: adds five-run repeatability, reasoning-mode
# conversations, and every complex reasoning task.
python -m app.evals.premium_eval --profile full
```

Each run writes `reports/premium/latest.json` for diagnostics and
`reports/premium/latest.md` for stakeholders. Add `--no-write` for an ephemeral
run or `--output-dir PATH` to retain a named release artifact.

Telemetry in the JSON artifact is scoped to that certification run. It reports
call and token totals, latency by model purpose, and any backend model/fingerprint
drift without logging API keys or prompt/response text.

The pass standard is intentionally strict: every enabled component must pass.
Do not average away a failed safety, factual, conversation, or reasoning gate.

## Adding coverage

Add broad, reviewed invariance groups or safety boundaries to
`app/evals/premium_cases.yaml`. Every analytical group needs independent
reference SQL and at least two paraphrases. Never import that manifest from a
production module, and never add a runtime phrase match to make an evaluation
pass. Fix the responsible metadata, prompt, typed plan, validator, or evidence
layer and rerun the entire gate.
