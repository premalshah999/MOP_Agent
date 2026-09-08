"""Blocking response-faithfulness gate used by the runtime pipeline.

Used by the strict golden suite and by the orchestrator to downgrade confidence
and attach a caveat when an answer drifts from the data.
"""

from __future__ import annotations

import json
import re
from typing import Any

from app.llm import client

_SYSTEM = """You grade whether an answer is FAITHFUL to its data and COMPLETE
for the user's question.

You get the question, the SQL that ran, and the EXACT rows it returned. You may
also receive explicitly labeled ANALYST DATA-QUALITY NOTES, ADDITIONAL
PEER-CONTEXT EVIDENCE, or ADDITIONAL TOOL EVIDENCE. Those blocks are verified
evidence too—not prose from the answer—and must be considered when grading.

FAITHFUL means: numbers, rankings, entities, and comparisons in the answer are
supported by the rows. Apply these allowances generously:
- Rounding and unit formatting are CORRECT and faithful: $30.58B, $30.6B, or
  "about $30.58 billion" all faithfully represent 30579948445.74. Do not nitpick
  decimals or significant figures.
- Restating SCOPE that comes from the SQL/question (the year filtered on, the
  state, "per capita", "top 10") is faithful even if that value is not a column
  in the returned rows — e.g. saying "in 2024" when the SQL has year = '2024'.
- Brief, reasonable context that does not assert a new number is fine.
- Rank, peer-geography-median, and year-over-year claims do not need to appear in the
  primary rows when they match ADDITIONAL PEER-CONTEXT EVIDENCE. Do not reject
  such a claim merely because the main SQL returned only the focus entity.

UNFAITHFUL means: fabricated or contradictory numbers, wrong ordering, wrong
entities, non-existent dataset identifiers presented as canonical, or
quantitative claims with no support in the evidence. Canonical dataset ids are
visible in SQL as `mart_<dataset_id>` and in the catalog notes.
Calling a typo "the <typo> dataset" is still incorrect even if the answer later
names the canonical table; an explicit "I interpreted <typo> as <id>" is fine.
It is also materially unfaithful to label a marginal demographic result as a
joint subgroup, multiply separate marginal percentages to invent an
intersection, use Total population when the documented denominator is adults
25+ or households, or substitute a broader available category for an absent
requested one. Enforce any supplied ANALYST DATA-QUALITY NOTES on runtime
coverage and denominators when judging both the SQL and the prose.
It is materially unfaithful to call AVG/MEDIAN across ACS state-level rows a
population-weighted national statistic when the analyst notes identify it as an
unweighted summary of represented geographies. Require the answer to distinguish
the 50 states, District of Columbia, and Puerto Rico from a true U.S. national
estimate, especially when the weighting denominator is absent.
It is materially unfaithful to call the federal-spending '2020-2024' summary a
five-year total/sum, to add it to the 2024 row, or to label a contract_* stored
'Per 1000' field as per capita/per 1,000 residents when the analyst notes say
its denominator is untraceable. Missing/placeholder government-finance or
survey observations must not be described as reported zeros. For named flow
questions, verify that inflow filters the subawardee/destination side and
outflow filters the prime-recipient/origin side.

COMPLETE means: the answer directly addresses every metric, entity, geography,
comparison, period, and ranking slot explicitly requested when the evidence
supports it. A concise answer is fine. For a requested top/bottom N whose rows
contain N results, omitting returned entities is incomplete. If evidence for a
requested part is missing, the answer must identify that limitation instead of
silently ignoring the part.
An answer that was asked to identify outliers is incomplete if it supplies only
a range or guesses likely entity names without returned outlier rows and a
stated method. For joined statistics, reject a shared sample-size/coverage claim
when the evidence gives different non-null counts for the component metrics.
For an unbounded ranking containing hundreds of members, an exact classified
count plus clearly labeled representative top/bottom rows is complete and more
useful than hundreds of names in prose. When tool evidence says `truncated` is
true, require the answer to disclose that its visible list is partial; do not
require it to enumerate rows the executor did not return.

Only mark faithful=false for a real, material discrepancy.

Decide FIRST, then write. `reason` must be ONE short sentence naming the
specific discrepancy (or confirming support) — no deliberation, no
"wait"/"let me re-check"/"actually" thinking-aloud. The `faithful` boolean
MUST match your final conclusion.

Return ONLY JSON: {"faithful": <bool>, "complete": <bool>, "reason": "<one sentence>"}"""


_FLOW_SQL_RE = re.compile(r"\bmart_(?:state|county|congress)_flow\b", re.IGNORECASE)
_CROSS_GEOGRAPHY_ONLY_RE = re.compile(
    r"\b(?:to|from)\s+(?:sub-?awardees?\s+in\s+|prime awardees?\s+in\s+)?other states\b"
    r"|\boutside\s+(?:of\s+)?[a-z .-]+",
    re.IGNORECASE,
)
_INTRA_SCOPE_DISCLOSED_RE = re.compile(
    r"\b(?:including|includes?)\s+(?:[a-z .-]+\s+itself|intra[- ]state|same[- ]state)\b"
    r"|\bother states\b.{0,80}\b(?:and|including)\s+[a-z .-]+\b",
    re.IGNORECASE,
)
_FLOW_EXCLUSION_RE = re.compile(
    r"(?:rcpt|subawardee)_(?:state_name|st_cd).{0,80}(?:<>|!=|\bnot\s+in\b)"
    r"|(?:<>|!=).{0,80}(?:rcpt|subawardee)_(?:state_name|st_cd)",
    re.IGNORECASE | re.DOTALL,
)

_MONEY_RE = re.compile(
    r"(?P<sign>[-\u2212])?\$\s*(?P<number>\d[\d,]*(?:\.\d+)?)\s*"
    r"(?P<suffix>thousand|million|billion|trillion|[kmbt])?\b",
    re.IGNORECASE,
)


def _money_values(text: str) -> list[float]:
    scales = {
        "k": 1e3,
        "thousand": 1e3,
        "m": 1e6,
        "million": 1e6,
        "b": 1e9,
        "billion": 1e9,
        "t": 1e12,
        "trillion": 1e12,
    }
    values: list[float] = []
    for match in _MONEY_RE.finditer(text or ""):
        number = float(match.group("number").replace(",", ""))
        scale = scales.get((match.group("suffix") or "").casefold(), 1.0)
        values.append((-1.0 if match.group("sign") else 1.0) * number * scale)
    return values


def _approximately_equal(left: float, right: float, tolerance: float = 0.02) -> bool:
    return abs(left - right) <= tolerance * max(1.0, abs(left), abs(right))


def _numeric_relation_problem(answer: str) -> str | None:
    """Check a few high-impact arithmetic relations stated in prose.

    This deliberately verifies only explicit currency arithmetic whose
    operands are printed in the same sentence. It does not infer intent,
    metrics, or datasets and therefore cannot prevent a supported analysis;
    it catches contradictions such as saying A exceeds B+C when the displayed
    operands prove otherwise.
    """

    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z])|\n+", answer or "")
    for sentence in sentences:
        relation = re.search(r"\b(more than|less than)\b", sentence, re.IGNORECASE)
        combined = re.search(r"\bcombined\b", sentence, re.IGNORECASE)
        if relation and combined and relation.start() < combined.start():
            left_values = _money_values(sentence[: relation.start()])
            right_values = _money_values(sentence[relation.end() : combined.start()])
            if left_values and len(right_values) >= 2:
                left = left_values[-1]
                right = sum(right_values)
                says_more = relation.group(1).casefold() == "more than"
                if (says_more and left <= right) or (not says_more and left >= right):
                    return (
                        "The stated combined-total comparison contradicts the "
                        "currency amounts printed in the answer."
                    )

        if re.search(r"\b(?:gap|difference)\b", sentence, re.IGNORECASE):
            amounts = _money_values(sentence)
            if len(amounts) == 3:
                expected = abs(amounts[0] - amounts[1])
                if not _approximately_equal(abs(amounts[2]), expected):
                    return (
                        "The stated currency gap does not equal the difference "
                        "between the two printed amounts."
                    )
    return None


def _flow_scope_problem(answer: str, sql: str) -> str | None:
    """Catch a material scope claim that numeric row checks cannot detect."""
    if not _FLOW_SQL_RE.search(sql or ""):
        return None
    if not _CROSS_GEOGRAPHY_ONLY_RE.search(answer or ""):
        return None
    if _INTRA_SCOPE_DISCLOSED_RE.search(answer or ""):
        return None
    if _FLOW_EXCLUSION_RE.search(sql or ""):
        return None
    return (
        "The answer says the flow is only to or from other states, but the SQL "
        "does not exclude intra-state subawards."
    )


def judge_faithfulness(
    question: str,
    answer: str,
    rows: list[dict[str, Any]],
    sql: str = "",
    tool_results: list[dict[str, Any]] | None = None,
    peer_context: str = "",
    data_notes: list[str] | None = None,
) -> dict[str, Any]:
    numeric_problem = _numeric_relation_problem(answer)
    if numeric_problem:
        return {
            "faithful": False,
            "data_faithful": False,
            "complete": True,
            "available": True,
            "reason": numeric_problem,
        }
    scope_problem = _flow_scope_problem(answer, sql)
    if scope_problem:
        return {
            "faithful": False,
            "data_faithful": False,
            "complete": True,
            "available": True,
            "reason": scope_problem,
        }
    extra = ""
    if data_notes:
        # Analyst-authored data-quality warnings (from the semantic registry,
        # NOT model output). An answer that excludes or qualifies rows in line
        # with these notes is faithful — e.g. skipping states whose zeros mean
        # "missing source data".
        extra += (
            "\nANALYST DATA-QUALITY NOTES (authoritative; an answer that "
            "excludes or qualifies rows per these notes is faithful):\n- " + "\n- ".join(data_notes)
        )
    if peer_context:
        # Normal-mode answers can include rank / vs-median / YoY context from
        # the peer_context module (separate side-queries from the main SQL).
        # The judge needs to see this evidence or it'll flag legit rank claims
        # as fabricated.
        extra += (
            "\nADDITIONAL PEER-CONTEXT EVIDENCE (independently computed by the "
            "system; treat claims of rank / vs peer-geography median / YoY as "
            "supported when they match):\n" + peer_context
        )
    if tool_results:
        trail: list[dict[str, Any]] = []
        for tr in tool_results:
            res = tr.get("result", {}) or {}
            if tr.get("name") == "peer_stats":
                trail.append(
                    {
                        "tool": "peer_stats",
                        "args": tr.get("args"),
                        "stats": res.get("stats"),
                        "top5": res.get("top5"),
                        "bottom5": res.get("bottom5"),
                    }
                )
            elif tr.get("name") == "run_sql" and not res.get("error"):
                trail.append(
                    {
                        "tool": "run_sql",
                        "sql": (res.get("sql") or "")[:200],
                        "row_count": res.get("row_count"),
                        "truncated": bool(res.get("truncated")),
                        "rows": res.get("rows", [])[:15],
                    }
                )
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
