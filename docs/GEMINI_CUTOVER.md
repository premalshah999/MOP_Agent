# Gemini Evaluation and Cost Plan

Pricing snapshot: 2026-08-03. Verify the official pricing page again before
budget approval: https://ai.google.dev/gemini-api/docs/pricing

## Local configuration

Put the key in the local `.env` file (never commit it):

```dotenv
LLM_PROVIDER=gemini
GEMINI_API_KEY=<key-from-google-ai-studio>
GEMINI_MODEL=gemini-3.5-flash-lite
GEMINI_REASONING_EFFORT=minimal
GEMINI_MIN_COMPLETION_TOKENS=512
LLM_TIMEOUT=12
LLM_RETRIES=2
```

Explicit Gemini selection ignores legacy `ASSISTANT_ROUTER_BASE_URL` and
`ASSISTANT_ROUTER_MODEL` values. The provider automatically uses Google's native
`generateContent` endpoint. An existing DeepSeek key may remain in `.env` for
rollback because explicit provider selection prevents it from taking precedence.

Recommended evaluation order:

1. `gemini-3.5-flash-lite` with `low` reasoning: current default and primary candidate.
2. `gemini-3.6-flash` with `low` reasoning: higher-cost quality comparison.
3. `gemini-2.5-flash`: older price/performance comparison.
4. `gemini-2.5-pro`: expensive complex-reasoning comparison.

Run every release gate for each candidate; never compare models from a handful
of manually selected questions only.

```bash
pytest -q
python -m app.evals.run_evals --suite both
python -m app.evals.repeatability --repeats 5
python -m app.evals.reasoning_eval
python -m app.evals.conversation_eval --mode normal
python -m app.evals.conversation_eval --mode reasoning
```

## Monthly cost assumptions

These are planning estimates for paid-tier text usage, excluding hosting,
network egress, Phase 2 RAG, Google Search grounding, and taxes.

- 22 active days per user per month.
- Light: 5 user questions/day = 110 questions/user/month.
- Moderate: 15/day = 330/month.
- Heavy: 30/day = 660/month.
- One analytical question invokes roughly five model stages; a follow-up often
  invokes six. Repairs and reasoning mode can add more.
- Blended planning allowance per user question: 18,000 input tokens and 6,000
  output tokens, including low-level thinking tokens.
- Normal synchronous API pricing, not discounted Batch/Flex pricing.

| Model | Official input/output per 1M | Approx. cost/question | Light, 30–50 users | Moderate, 30–50 | Heavy, 30–50 |
|---|---:|---:|---:|---:|---:|
| Gemini 3.6 Flash | $1.50 / $7.50 | $0.0720 | $238–$396 | $713–$1,188 | $1,426–$2,376 |
| Gemini 3.5 Flash-Lite | $0.30 / $2.50 | $0.0204 | $67–$112 | $202–$337 | $404–$673 |
| Gemini 2.5 Flash | $0.30 / $2.50 | $0.0204 | $67–$112 | $202–$337 | $404–$673 |
| Gemini 2.5 Flash-Lite | $0.10 / $0.40 | $0.0042 | $14–$23 | $42–$69 | $83–$139 |
| Gemini 2.5 Pro | $1.25 / $10.00* | $0.0825 | $272–$454 | $817–$1,361 | $1,633–$2,722 |

`*` Gemini 2.5 Pro rate shown is for prompts up to 200,000 input tokens.

Add a 25% budget reserve for retries, answer repair, traffic bursts, and longer
conversations. Frequent reasoning-mode use can raise model spend by roughly
2–4x, depending on tool-loop depth and thinking-token use. Instrument actual
provider usage before final procurement; this estimate intentionally errs on
the safe side for normal mode.

For a state-facing production system, use Google's paid tier rather than
assuming the free tier: the official page states paid-tier content is not used
to improve Google's products, while free-tier content may be.
