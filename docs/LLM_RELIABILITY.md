# LLM Drift and Hallucination Reliability

## What “drift” means in this application

The system has several distinct failure modes. Treating all of them as generic
hallucination leads to the wrong fixes.

1. **Provider drift**: the provider changes the model behind an alias or ships a
   new model version. Temperature zero reduces sampling variance but cannot
   freeze provider weights or infrastructure. Published longitudinal testing
   has shown that the behavior of an unchanged API model name can change
   materially over time.
2. **Intent drift**: paraphrases or repeated requests produce different tables,
   measures, periods, direction, or analytical operations.
3. **Schema-linking drift**: the model understands the topic but attaches it to
   the wrong runtime field or misses a field whose description contains the
   user's language.
4. **SQL-semantic hallucination**: executable SQL answers a different question,
   uses the wrong grain, denominator, join side, period, or aggregation.
5. **Synthesis hallucination**: SQL and rows are correct but the prose changes a
   value, scope, rank, entity, or caveat.
6. **Conversation drift**: older answer prose overrides the latest user turn or
   a terse follow-up loses its canonical dataset, metric, or comparison.
7. **Evaluator drift**: an LLM judge can itself contradict visible evidence.
   It is useful as one signal, but it cannot be the only release oracle.

## What the research supports

- Continuous, version-aware monitoring is necessary because model-service
  behavior can change over time: [Chen, Zaharia, and Zou
  (2023)](https://arxiv.org/abs/2307.09009).
- Robust schema linking is a primary bottleneck in realistic text-to-SQL.
  Separating database/table retrieval from field grounding and isolating
  irrelevant schema improves performance: [LinkAlign (EMNLP
  2025)](https://aclanthology.org/2025.emnlp-main.51/).
- Schema-linking improvements and perturbation-based training/evaluation improve
  robustness to paraphrases and renamed schema items: [Solid-SQL (COLING
  2025)](https://aclanthology.org/2025.coling-main.654/).
- Execution alone is not proof of semantic correctness, and exact SQL-string
  matching also produces false decisions. Evaluation needs question, schema,
  execution result, and semantic criteria together: [FLEX (NAACL
  2025)](https://aclanthology.org/2025.naacl-long.228/).
- Self-consistency can help, but majority voting can still select a wrong SQL;
  it should be paired with correction/verification rather than used blindly:
  [CSC-SQL (IJCNLP 2025)](https://aclanthology.org/2025.findings-ijcnlp.91/).

Provider documentation reinforces two operational requirements:

- DeepSeek says lower temperature is more focused, but does not offer a seed in
  its Chat Completions API. Its JSON mode guarantees valid JSON syntax, not
  correct semantics, and tool arguments still require validation:
  [DeepSeek Chat Completions](https://api-docs.deepseek.com/api/create-chat-completion/).
- DeepSeek retired the legacy `deepseek-chat`/`deepseek-reasoner` aliases in
  favor of explicit V4 models. V4 thinking tool calls require
  `reasoning_content` to be preserved across every tool turn:
  [DeepSeek model details](https://api-docs.deepseek.com/quick_start/pricing/)
  and [thinking-mode guide](https://api-docs.deepseek.com/guides/thinking_mode/).
- Google recommends a specific stable Gemini model for production; `latest`,
  preview, and experimental names can be swapped or deprecated:
  [Gemini model version patterns](https://ai.google.dev/gemini-api/docs/models).

## Reliability boundary

No probabilistic model service can guarantee literal zero error. The practical
state-deployment standard is **zero silent error within the certified data
boundary**: stable evidence for repeated requests, evidence-bound answers, and
an explicit limitation only when the required field, denominator, grain, or
period truly is unavailable.

The model should remain responsible for language understanding and analytical
strategy. Runtime deterministic code is appropriate only for invariants that
cannot become “more intelligent” with sampling:

- catalog membership, schema types, units, periods, and join compatibility;
- read-only SQL, allowed views, formula/operation consistency, and result shape;
- exact entity values returned by the database;
- execution, structured-response validation, evidence provenance, row limits,
  authentication, and timeouts.

Runtime code must not map individual phrasings to fixed tables, SQL, or answers.
Ambiguous analytical wording should proceed with a disclosed standard method
when the requested measure, population, geography, and unit are clear.
Clarification is reserved for choices that materially change those meanings.

## Production design

### 1. Version and fingerprint every model contract

Record, without prompt text:

- provider and configured model;
- provider-returned model/version and system fingerprint when available;
- pipeline and semantic-registry version;
- a hash of the complete prompt, tool schemas, temperature, output budget, and
  call purpose;
- a hash of the output, finish reason, latency, and token usage.

This makes a provider change distinguishable from a prompt, catalog, or code
change. `data/runtime/llm_calls.jsonl` is the non-blocking shadow log for these
signals; `data/runtime/query_log.jsonl` records the selected provider and pipeline
versions with each completed request.

Inspect the shadow signal with:

```bash
python -m app.observability.drift_report
```

### 2. Keep one model-led semantic plan

The planner creates a typed meaning contract: tables, metrics, periods, formula,
predicate, observation grain, ranking keys (separate from display-only
measures), output dimensions, and result scope. A
description-rich verifier may repair schema-impossible plans. Downstream code
executes that plan and must not reinterpret the user's prose into a competing
answer path.

### 3. Ground narrowly after broad understanding

The first planning pass sees the complete catalog so it can understand unusual
wording. After routing, SQL generation sees only the selected table schemas,
exact field descriptions, critical caveats, join rules, and live canonical
entity values. This keeps schema recall high without flooding generation with
irrelevant columns.

### 4. Validate semantics, not wording

Before execution, validate table/column existence, join safety, aggregation,
grain, periods, flow direction, formula operands, ranking scope, and expected
row shape. After execution, validate the result shape and generate prose only
from returned evidence. A failed prose audit falls back to the validated rows;
it never invents a replacement answer.

### 5. Use LLM judges as a supplemental signal

Release gates should combine:

- independent reference SQL/results for factual cases;
- semantic-plan and evidence signatures across repeated identical questions;
- paraphrase and typo clusters that must converge on the same evidence;
- multi-turn conversation cases;
- an LLM rubric judge for qualitative completeness;
- human review of judge/evidence disagreements.

An LLM judge may add failures but should not silently redefine ground truth. A
judge contradiction must be logged as evaluator disagreement rather than
converted into query-specific runtime logic.

### 6. Roll out changes as measured migrations

For every provider, model, prompt, or catalog change:

1. Capture the current baseline on the exact production data snapshot.
2. Run static tests and semantic audit.
3. Run independent-reference factual, paraphrase, repeatability, conversation,
   and complex-reasoning suites.
4. Compare plan disagreement, evidence disagreement, error/clarification rate,
   latency, and cost—not answer wording.
5. Shadow or canary the candidate model before full rollout.
6. Keep the previous model/config and application image as a tested rollback.

## RAG boundary

RAG is useful in Phase 2 for unstructured source material, methodology pages,
research descriptions, and definitions that are not represented in runtime
tables. It is not the primary fix for wrong SQL over loaded structured data.
For the current catalog, complete semantic metadata plus schema linking,
execution, and evidence verification is the higher-value reliability path.
