"""LLM judge for evidence faithfulness and question coverage.

Used by the strict golden suite and by the orchestrator to downgrade confidence
and attach a caveat when an answer drifts from the data.
"""

from __future__ import annotations

import json
from typing import Any

from app.llm import client

_SYSTEM = """You grade whether an answer is FAITHFUL to its data and COMPLETE
for the user's question.

You get the question, the SQL that ran, and the EXACT rows it returned.

FAITHFUL means: numbers, rankings, entities, and comparisons in the answer are
supported by the rows. Apply these allowances generously:
- Rounding and unit formatting are CORRECT and faithful: $30.58B, $30.6B, or
  "about $30.58 billion" all faithfully represent 30579948445.74. Do not nitpick
  decimals or significant figures.
- Restating SCOPE that comes from the SQL/question (the year filtered on, the
  state, "per capita", "top 10") is faithful even if that value is not a column
  in the returned rows — e.g. saying "in 2024" when the SQL has year = '2024'.
- Brief, reasonable context that does not assert a new number is fine.

UNFAITHFUL means: fabricated or contradictory numbers, wrong ordering, wrong
entities, or quantitative claims with no support in the rows.

COMPLETE means: the answer directly addresses every metric, entity, geography,
comparison, period, and ranking slot explicitly requested when the evidence
supports it. A concise answer is fine. For a requested top/bottom N whose rows
contain N results, omitting returned entities is incomplete. If evidence for a
requested part is missing, the answer must identify that limitation instead of
silently ignoring the part.

Only mark faithful=false for a real, material discrepancy.

Decide FIRST, then write. `reason` must be ONE short sentence naming the
specific discrepancy (or confirming support) — no deliberation, no
"wait"/"let me re-check"/"actually" thinking-aloud. The `faithful` boolean
MUST match your final conclusion.

Return ONLY JSON: {"faithful": <bool>, "complete": <bool>, "reason": "<one sentence>"}"""


def judge_faithfulness(
    question: str,
    answer: str,
    rows: list[dict[str, Any]],
    sql: str = "",
    tool_results: list[dict[str, Any]] | None = None,
    peer_context: str = "",
    data_notes: list[str] | None = None,
) -> dict[str, Any]:
    extra = ""
    if data_notes:
        # Analyst-authored data-quality warnings (from the semantic registry,
        # NOT model output). An answer that excludes or qualifies rows in line
        # with these notes is faithful — e.g. skipping states whose zeros mean
        # "missing source data".
        extra += (
            "\nANALYST DATA-QUALITY NOTES (authoritative; an answer that "
            "excludes or qualifies rows per these notes is faithful):\n- "
            + "\n- ".join(data_notes)
        )
    if peer_context:
        # Normal-mode answers can include rank / vs-median / YoY context from
        # the peer_context module (separate side-queries from the main SQL).
        # The judge needs to see this evidence or it'll flag legit rank claims
        # as fabricated.
        extra += (
            "\nADDITIONAL PEER-CONTEXT EVIDENCE (independently computed by the "
            "system; treat claims of rank / vs national median / YoY as "
            "supported when they match):\n" + peer_context
        )
    if tool_results:
        trail: list[dict[str, Any]] = []
        for tr in tool_results:
            res = tr.get("result", {}) or {}
            if tr.get("name") == "peer_stats":
                trail.append({"tool": "peer_stats", "args": tr.get("args"), "stats": res.get("stats"), "top5": res.get("top5"), "bottom5": res.get("bottom5")})
            elif tr.get("name") == "run_sql" and not res.get("error"):
                trail.append({"tool": "run_sql", "sql": (res.get("sql") or "")[:200], "row_count": res.get("row_count"), "rows": res.get("rows", [])[:15]})
        if trail:
            extra += (
                "\nADDITIONAL TOOL EVIDENCE (the analyst also gathered these via "
                "tools; count claims as supported if they match):\n"
                + json.dumps(trail, default=str, indent=2)[:4000]
            )
    user = (
        f"QUESTION: {question}\n\n"
        f"SQL THAT RAN:\n{sql or '(not provided)'}\n\n"
        f"ANSWER:\n{answer}\n\n"
        f"ROWS ({len(rows)} total, showing up to 60):\n"
        f"{json.dumps(rows[:60], default=str, indent=2)}\n"
        f"{extra}\n\n"
        "Grade evidence faithfulness and question coverage."
    )
    try:
        raw = client.chat_json(
            [
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
            max_tokens=300,
            purpose="faithfulness_judge",
        )
    except Exception as exc:
        # This is a safety gate, not telemetry.  An unavailable verifier can
        # never turn an unverified answer into an accepted one.
        return {
            "faithful": False,
            "available": False,
            "reason": f"verification unavailable ({exc})",
        }
    if not isinstance(raw, dict):
        return {
            "faithful": False,
            "available": False,
            "reason": "verification returned a non-object response",
        }
    # Do not coerce strings: bool("false") is True in Python. Recorded judge
    # fixtures without `complete` remain compatible; new responses must return
    # a real boolean and incomplete answers fail the existing blocking gate.
    data_faithful = raw.get("faithful") is True
    complete = raw.get("complete") is True if "complete" in raw else True
    faithful = data_faithful and complete
    reason = str(raw.get("reason") or "").strip()
    if not reason:
        reason = "verifier returned no reason"
    if len(reason) > 220:
        reason = reason[:217].rstrip() + "…"
    return {
        "faithful": faithful,
        "data_faithful": data_faithful,
        "complete": complete,
        "available": True,
        "reason": reason,
    }
