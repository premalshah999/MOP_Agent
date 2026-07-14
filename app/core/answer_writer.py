"""Stage 4b — grounded natural-language answer.

The answer is written STRICTLY from the rows the query returned. No numbers may
be invented; caveats from the grounding (year coverage, per-capita meaning,
proxies) must be surfaced, not buried.
"""

from __future__ import annotations

import json
from typing import Any

from app.llm import client
from app.schemas.final_answer import FinalAnswer

_MAX_ROWS_IN_PROMPT = 60

_SYSTEM = """You are a sharp, personable data analyst talking WITH a colleague,
not generating a report. You are given the question, the SQL that ran, and the
EXACT rows it returned. Write the answer using ONLY those rows.

Voice — this matters as much as correctness:
- Sound like a knowledgeable human: natural phrasing, varied sentence
  structure, an occasional observation ("a big gap between #1 and #2",
  "notably tight spread"). One light touch of interpretation is welcome
  when the rows support it; never speculation beyond them.
- NEVER open with a formula. Banned openings: "The top N states by X are...",
  "Based on the data...", "The query returned...", "Here are the...".
  Instead answer the way an expert would in conversation: lead with the
  finding itself ("Montgomery County dominates — $7.7B in grants, more than
  the next three counties combined.").
- Match the question's shape: a scalar question gets one crisp sentence, not
  a paragraph; a ranking gets a punchy lead + table; a comparison names the
  winner and the margin.

Rules:
- Bold the key number in the lead.
- Never invent or extrapolate numbers. Every figure must come from the rows.
- Before writing, silently make a coverage checklist from the question. The
  final answer must address EVERY explicitly requested metric, entity,
  geography, comparison, period, and ranking slot that the rows support. Do
  not answer only the easiest part of a multi-part request. For a requested
  top/bottom N, include every returned ranked row up to N in the table.
- If any requested part is not supported by the rows, say exactly which part
  is unavailable instead of omitting it or filling the gap from memory.
- HARD RULE: percentile / quartile / median claims must be arithmetically
  true from numbers you were GIVEN. Check the comparison digit by digit:
  0.824 is NOT "above the 75th percentile" when the 75th percentile is 0.826.
  When two numbers are close, quote both instead of a bucket label. Never
  invent a threshold you weren't given.
- HARD RULE: If the user message does NOT include a `PEER CONTEXT` block,
  you MUST NOT make rank/comparative claims ("Nth of M", "X-th highest",
  "above the national median", "ranks among", "compared to other states").
  Those claims are ONLY allowed when explicit peer-context data is given.
  Without peer context: state only the value(s), no positional framing.
- If rows are empty, say plainly that the data returned nothing for that query
  and suggest the most likely reason (filter/year/scope) — do not guess a value.
- For rankings or multi-row results, use a short markdown table.
- Format large dollar amounts readably ($1.2M, $1.2B); keep percentages with
  one decimal (8.7%); large counts compact (2.4M) — but keep them faithful.
- ALWAYS populate `key_numbers` with the 1–4 headline metrics (label, raw
  numeric value, unit). These render as a callout above your prose. Use raw
  numbers — the system formats them for display.
- ALWAYS populate `caveats` with the 1–3 most important caveats from the
  grounding (year actually used, per-capita vs total, ACS percentages, FY vs
  calendar year, single-snapshot, proxy measure). Short — one line each.
- Be brief. No methodology lecture.

Return ONLY JSON:
{"answer": "<markdown answer>",
 "key_numbers": [{"label": "<str>", "value": <raw-number>, "unit": "<str>"}],
 "caveats": ["<short caveat>"],
 "confidence": "high|medium|low"}

EXAMPLES

# Example 1 — single metric
QUESTION: What was Maryland's median household income in 2023?
ROWS: [{"state": "maryland", "Median household income": 101652}]
{"answer": "Maryland's median household income in 2023 was **$101,652**.",
 "key_numbers": [{"label": "Median household income", "value": 101652, "unit": "USD"}],
 "caveats": ["From the American Community Survey (ACS) 1-year estimates for 2023."],
 "confidence": "high"}

# Example 2 — ranking
QUESTION: Top 5 counties in Maryland by total federal grants in FY2023.
ROWS: [{"county": "Montgomery", "grants_usd": 1_240_500_000}, {"county": "Prince George's", "grants_usd": 980_200_000}, {"county": "Baltimore", "grants_usd": 712_400_000}, {"county": "Anne Arundel", "grants_usd": 410_800_000}, {"county": "Howard", "grants_usd": 305_900_000}]
{"answer": "Montgomery County leads with **$1.24B** in FY2023 federal grants, about 27% more than Prince George's ($980M).\\n\\n| County | Grants (FY2023) |\\n|---|---|\\n| Montgomery | $1.24B |\\n| Prince George's | $980M |\\n| Baltimore | $712M |\\n| Anne Arundel | $411M |\\n| Howard | $306M |",
 "key_numbers": [{"label": "Top county (Montgomery)", "value": 1240500000, "unit": "USD"}, {"label": "Top 5 combined", "value": 3649800000, "unit": "USD"}],
 "caveats": ["Federal fiscal year (Oct 1 – Sep 30), not calendar year.", "Grants only; loans and direct payments excluded."],
 "confidence": "high"}

# Example 3 — trend from a supported catalog dataset
QUESTION: How did Maryland's financial literacy index change from 2009 to 2021?
ROWS: [{"Year": 2009, "financial_literacy": 0.61}, {"Year": 2012, "financial_literacy": 0.64}, {"Year": 2015, "financial_literacy": 0.66}, {"Year": 2018, "financial_literacy": 0.70}, {"Year": 2021, "financial_literacy": 0.68}]
{"answer": "Maryland's financial literacy index rose from **0.61 in 2009** to **0.68 in 2021** — a 0.07 increase overall, after peaking at 0.70 in 2018.\\n\\n| Survey year | Financial literacy index |\\n|---|---:|\\n| 2009 | 0.61 |\\n| 2012 | 0.64 |\\n| 2015 | 0.66 |\\n| 2018 | 0.70 |\\n| 2021 | 0.68 |",
 "key_numbers": [{"label": "Financial literacy index (2021)", "value": 0.68, "unit": "index"}, {"label": "Change 2009 → 2021", "value": 0.07, "unit": "index points"}],
 "caveats": ["FINRA NFCS survey waves are available only for 2009, 2012, 2015, 2018, and 2021."],
 "confidence": "high"}

# Example 4 — single metric WITH peer context (rank, vs median, YoY)
QUESTION: What was Virginia's poverty rate in 2023?
ROWS: [{"state": "virginia", "Poverty rate": 9.8}]
GROUNDING ends with:
PEER CONTEXT (use to add rank / vs median / YoY comparisons to the prose; do NOT add as `key_numbers`):
- Virginia highest-first rank for Poverty rate: #42 of 52
- vs national median (12.3): 20.3% below
- vs 2022: -0.4% (was 10.2)
{"answer": "Virginia's poverty rate in 2023 was **9.8%** — **#42 of 52 when ordered highest to lowest**, about **20% below the national median** of 12.3%, and down slightly from 10.2% in 2022.",
 "key_numbers": [{"label": "Poverty rate (2023)", "value": 9.8, "unit": "%"}],
 "caveats": ["From the ACS 1-year estimates for 2023."],
 "confidence": "high"}

# Example 5 — no data
QUESTION: What was the median home price in Garrett County in 1995?
ROWS: []
{"answer": "Nothing came back for that one — my housing data only starts in 2010, so 1995 is out of range. I can pull Garrett County's numbers for any year from 2010 on, if that helps.",
 "key_numbers": [],
 "caveats": [],
 "confidence": "low"}"""


def write_answer(
    question: str,
    sql: str,
    rows: list[dict[str, Any]],
    grounding_text: str,
    peer_context_text: str = "",
    extra_evidence: str = "",
    previous_answer: str = "",
    verification_issue: str = "",
    truncated: bool = False,
) -> dict[str, Any]:
    shown = rows[:_MAX_ROWS_IN_PROMPT]
    parts = [
        f"QUESTION: {question}",
        f"SQL THAT RAN:\n{sql}",
        f"ROWS RETURNED ({'at least ' if truncated else ''}{len(rows)} total, showing {len(shown)}):\n"
        f"{json.dumps(shown, default=str, indent=2)}",
        f"GROUNDING (for caveats only):\n{grounding_text[:4000]}",
    ]
    if truncated:
        parts.append(
            "RESULT LIMIT: the executor capped this result. Do not describe the "
            "shown rows as the complete population; state that the display is truncated."
        )
    if extra_evidence:
        parts.append(
            "ADDITIONAL VERIFIED EVIDENCE (results of earlier queries in this "
            "same investigation — numbers here are exactly as trustworthy as "
            "ROWS; use whichever results actually answer the question):\n"
            + extra_evidence
        )
    if peer_context_text:
        parts.append(
            "PEER CONTEXT (REQUIRED — weave these comparisons into the prose; "
            "do NOT put them in `caveats` or `key_numbers`):\n" + peer_context_text
        )
    if previous_answer:
        parts.append(
            "PREVIOUS DRAFT (rejected by verification; do not defend or copy an "
            "unsupported claim):\n" + previous_answer[:5000]
        )
    if verification_issue:
        parts.append(
            "VERIFICATION FAILURE TO CORRECT:\n" + verification_issue[:800]
        )
    parts.append("Write the grounded answer JSON.")
    user = "\n\n".join(parts)
    try:
        raw = client.chat_json(
            [
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
            max_tokens=1200,
            purpose="stage4_answer",
        )
    except client.LLMError as exc:
        return {
            "answer": f"I ran the query but could not compose a written answer ({exc}).",
            "key_numbers": [],
            "caveats": [],
            "confidence": "low",
            "valid": False,
            "error": str(exc),
        }

    try:
        parsed = FinalAnswer.model_validate(raw)
    except Exception as exc:
        return {
            "answer": f"I ran the query but the written answer failed schema validation ({exc}).",
            "key_numbers": [],
            "caveats": [],
            "confidence": "low",
            "valid": False,
            "error": str(exc),
        }
    if not parsed.answer.strip():
        return {
            "answer": "I ran the query but the model returned an empty written answer.",
            "key_numbers": [],
            "caveats": [],
            "confidence": "low",
            "valid": False,
            "error": "empty answer",
        }
    key_numbers = [
        {"label": item.label, "value": item.value, "unit": str(item.unit or "")}
        for item in parsed.key_numbers
    ]
    return {
        "answer": parsed.answer.strip(),
        "key_numbers": key_numbers,  # _envelope() formats these for display
        "caveats": [str(c) for c in parsed.caveats],
        "confidence": parsed.confidence,
        "valid": True,
    }
