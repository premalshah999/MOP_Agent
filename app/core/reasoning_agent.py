"""Reasoning-mode agent loop (opt-in, slower, smarter).

A small Plan-Act-Observe loop on top of the existing primitives. The model
plans, calls a tool (`get_schema`, `distinct_values`, `run_sql`, `peer_stats`),
observes the JSON result, and continues until it calls `answer` to terminate
— bounded by a hard budget. Every claim ultimately traces back to a
validator-gated SQL row, and the faithfulness judge still gates the final
answer at the orchestrator layer.

This module is independent of normal mode — the orchestrator dispatches on
`mode`. Normal mode is unchanged.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Callable

from app.core.answer_writer import write_answer
from app.core.tools import TOOL_SCHEMAS, execute_tool
from app.llm import client
from app.schemas.final_answer import FinalAnswer
from app.semantic.registry import catalog_for_prompt, domain_summary


_AGENT_SYSTEM = """You are a careful policy-data analyst with read-only access
to a fixed US public-policy catalog. You answer the user's question by calling
small tools and reasoning over the results — NEVER by inventing values.

TOOLS
- get_schema(table)            see a table's full columns + CRITICAL warnings
- distinct_values(table, col)  list real values of a filter column (with pattern)
- run_sql(sql)                 execute one validator-gated SELECT/WITH
- peer_stats(table, measure)   min/p25/median/mean/p75/max + top5/bottom5
- answer(text, key_numbers, caveats)   TERMINATE with the final answer

RULES
- Stay grounded: every number / ranking / claim in `text` must be supported by
  data returned from a tool call in THIS turn. No fabrication.
- Percentile / quartile / median claims must be arithmetically true from
  numbers a tool returned: 0.824 is NOT "above the 75th percentile" when the
  p75 is 0.826. When two numbers are close, quote both instead of a bucket
  label ("top quartile"/"bottom quartile"). Never invent a threshold.
- If you're not 100% sure of a column name, casing, or filter value, call
  `get_schema` and/or `distinct_values` BEFORE `run_sql`.
- For "is X high?", "where does X stand?", or distributional questions, prefer
  `peer_stats` over hand-rolled SQL.
- IMPORTANT: as soon as a `run_sql` (or `peer_stats`) returns the rows that
  answer the question, CALL `answer` immediately. Do not keep exploring.
  Default to 1 SQL + answer; only add more tool calls if the data genuinely
  doesn't answer the question yet.
- Always end by calling `answer`. Do NOT return prose without `answer`.
- Budget: max 6 tool calls. Stay concise; one statement per SQL.

ANSWER VOICE (when calling `answer`):
- LEAD with the direct answer in one sentence; bold the key number with **…**.
- Use a markdown table for ranking/multi-row results.
- ALWAYS fill `key_numbers` with 1–4 headline metrics (label, RAW numeric
  value, unit like "USD"/"%"/"households"). The system formats them.
- ALWAYS fill `caveats` with 1–3 short notes (year, ACS vs BLS, FY vs CY,
  proxy measure). One line each. The peer/comparative context you observed
  (median, percentile, YoY delta) belongs in `text`, not `caveats`.

DOMAIN
{domain}

CATALOG (use exact table ids)
{catalog}"""


def _build_user(question: str, history: list[dict[str, Any]] | None) -> str:
    parts = [f"QUESTION: {question}"]
    if history:
        # Carry structured focus from the last assistant turn if present.
        for h in reversed(history):
            if h.get("role") == "assistant" and isinstance(h.get("contract"), dict):
                c = h["contract"]
                facts = [
                    ("table(s)", c.get("tables") or ([c.get("family")] if c.get("family") else [])),
                    ("metric", c.get("metric")),
                    ("focus_state", c.get("focus_state")),
                    ("year", c.get("year")),
                ]
                lines = [f"  {k} = {v}" for k, v in facts if v]
                if lines:
                    parts.insert(0, "PRIOR FOCUS (carry over unless overridden):\n" + "\n".join(lines))
                break
        recent = [h for h in history if h.get("role") == "user"][-4:]
        if recent:
            convo = "\n".join(f"{h['role']}: {str(h.get('content', ''))[:300]}" for h in recent)
            parts.append("RECENT CONVERSATION:\n" + convo)
    return "\n\n".join(parts)


def _summarize_tool_result(name: str, result: dict[str, Any]) -> dict[str, Any]:
    """Compact trace entry (avoids bloating pipelineTrace with full row dumps)."""
    if "error" in result and result["error"]:
        return {"error": result["error"][:200]}
    if name == "run_sql":
        return {"row_count": result.get("row_count", 0), "sql": (result.get("sql", "") or "")[:160]}
    if name == "distinct_values":
        return {"count": result.get("count", 0), "sample": result.get("values", [])[:5]}
    if name == "peer_stats":
        s = result.get("stats", {}) or {}
        return {"n": s.get("n"), "min": s.get("min"), "median": s.get("median"), "max": s.get("max")}
    if name == "get_schema":
        return {"len": len(result.get("schema", ""))}
    return {"ok": True}


def _serialize_for_model(result: dict[str, Any], cap: int = 4000) -> str:
    blob = json.dumps(result, default=str)
    return blob if len(blob) <= cap else (blob[:cap] + "…[truncated]")


DEFAULT_MAX_CALLS = int(os.getenv("REASONING_MAX_CALLS", "6"))
DEFAULT_MAX_WALL_S = float(os.getenv("REASONING_MAX_WALL_S", "25"))
DEFAULT_MAX_TOKENS = int(os.getenv("REASONING_MAX_TOKENS", "60000"))


def _evidence_digest(tool_results: list[dict[str, Any]]) -> str:
    """Compact digest of every successful tool result this run. Passed to the
    answer writer so synthesis sees the WHOLE investigation, not just the last
    query — the last call is often a small sanity check, and answering from it
    alone produced wrong "the data can't show this" conclusions."""
    parts: list[dict[str, Any]] = []
    for tr in tool_results:
        name, res = tr.get("name"), tr.get("result") or {}
        if res.get("error"):
            continue
        if name == "run_sql":
            parts.append({
                "tool": "run_sql",
                "sql": (res.get("sql") or "")[:300],
                "row_count": res.get("row_count"),
                "rows": (res.get("rows") or [])[:25],
            })
        elif name == "peer_stats":
            parts.append({
                "tool": "peer_stats",
                "args": tr.get("args"),
                "stats": res.get("stats"),
                "top5": res.get("top5"),
                "bottom5": res.get("bottom5"),
            })
    if not parts:
        return ""
    return json.dumps(parts, default=str)[:6000]


def run_reasoning_agent(
    question: str,
    history: list[dict[str, Any]] | None = None,
    *,
    max_calls: int = DEFAULT_MAX_CALLS,
    max_wall_s: float = DEFAULT_MAX_WALL_S,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    on_event: Callable[[str, dict[str, Any]], None] | None = None,
    semantic_guard: Callable[[str], None] | None = None,
    allowed_tables: set[str] | None = None,
) -> dict[str, Any]:
    emit = on_event or (lambda _n, _d: None)
    """Run the agent. Returns a dict the orchestrator can wrap into the
    standard envelope: {answer, key_numbers, caveats, sql, rows, trace,
    stopped_reason, used_tokens, steps}."""
    system = _AGENT_SYSTEM.format(domain=domain_summary(), catalog=catalog_for_prompt())
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system},
        {"role": "user", "content": _build_user(question, history)},
    ]
    trace: list[dict[str, Any]] = []
    tool_results: list[dict[str, Any]] = []  # full results — used by faithfulness/judge
    sql_history: list[str] = []
    last_rows: list[dict[str, Any]] = []
    used_tokens = 0
    started = time.time()

    def _finish(reason: str, answer: str, key_numbers: list, caveats: list, step: int) -> dict[str, Any]:
        return {
            "answer": answer,
            "key_numbers": key_numbers,
            "caveats": caveats,
            "sql": sql_history[-1] if sql_history else None,
            "sql_history": sql_history,
            "rows": last_rows,
            "trace": trace,
            "tool_results": tool_results,
            "stopped_reason": reason,
            "used_tokens": used_tokens,
            "steps": step,
        }

    for step in range(1, max_calls + 1):
        if time.time() - started > max_wall_s:
            return _finish("wall_budget", "I ran out of time investigating this. Here's what I gathered so far — please rephrase or narrow the question.", [], [], step - 1)
        if used_tokens > max_tokens:
            return _finish("token_budget", "I ran out of token budget on this turn. Please narrow the question.", [], [], step - 1)

        try:
            resp = client.chat_tools(messages, tools=TOOL_SCHEMAS, temperature=0.0, max_tokens=1500, purpose="reasoning_agent")
        except client.LLMError as exc:
            return _finish("llm_error", f"I hit an LLM error mid-investigation: {exc}", [], [], step - 1)

        used_tokens += resp["usage"]["total_tokens"]
        tool_calls = resp["tool_calls"]
        content = resp["content"]

        assistant_msg: dict[str, Any] = {"role": "assistant", "content": content or ""}
        if tool_calls:
            assistant_msg["tool_calls"] = [
                {
                    "id": tc["id"],
                    "type": "function",
                    "function": {"name": tc["name"], "arguments": tc["arguments_str"] or "{}"},
                }
                for tc in tool_calls
            ]
        messages.append(assistant_msg)

        if not tool_calls:
            # Model returned bare prose without invoking any tool — this path
            # was previously accepted as final, but the prose bypasses both
            # write_answer and the faithfulness judge, so a fabricated claim
            # would ship unvalidated. Instead: if we already collected rows,
            # synthesise via the normal-mode answer pipeline (same path as
            # budget-exhausted); otherwise admit we couldn't answer.
            trace.append({"step": step, "name": "no_tool_call", "summary": "model returned prose without a tool call; routing to row-grounded synthesis"})
            if last_rows:
                last_sql = sql_history[-1] if sql_history else ""
                try:
                    synth = write_answer(
                        question, last_sql, last_rows[:60], grounding_text="",
                        extra_evidence=_evidence_digest(tool_results),
                    )
                except Exception:
                    synth = None
                if synth and synth.get("valid", True) and synth.get("answer"):
                    return _finish(
                        "no_tool_call_synthesised",
                        synth["answer"],
                        synth.get("key_numbers", []) or [],
                        synth.get("caveats", []) or [],
                        step,
                    )
            return _finish(
                "no_tool_call_no_rows",
                "I couldn't fully answer that. Try rephrasing with a specific metric, geography, and year.",
                [],
                [],
                step,
            )

        for tc in tool_calls:
            name = tc["name"]
            args = tc["arguments"]
            if name == "answer":
                try:
                    parsed_answer = FinalAnswer.model_validate({
                        "answer": str(args.get("text", "")).strip(),
                        "key_numbers": list(args.get("key_numbers", []) or []),
                        "caveats": list(args.get("caveats", []) or []),
                    })
                    if not parsed_answer.answer:
                        raise ValueError("answer text is empty")
                except Exception as exc:
                    result = {"error": f"answer schema validation: {exc}"}
                    trace.append({
                        "step": step,
                        "name": "answer",
                        "summary": {"error": str(result["error"])[:200]},
                    })
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc["id"],
                        "content": _serialize_for_model(result),
                    })
                    continue
                trace.append({"step": step, "name": "answer", "summary": _summarize_tool_result("answer", {"ok": True})})
                return _finish(
                    "ok",
                    parsed_answer.answer,
                    [item.model_dump() for item in parsed_answer.key_numbers],
                    list(parsed_answer.caveats),
                    step,
                )
            emit("tool_start", {"step": step, "name": name, "args": args})
            result = execute_tool(
                name, args,
                semantic_guard=semantic_guard,
                allowed_tables=allowed_tables,
            )
            summary = _summarize_tool_result(name, result)
            trace.append({"step": step, "name": name, "args": args, "summary": summary})
            tool_results.append({"name": name, "args": args, "result": result})
            emit("tool", {"step": step, "name": name, "summary": summary, "error": result.get("error")})
            if name == "run_sql" and not result.get("error") and result.get("rows"):
                sql_history.append(result["sql"])
                last_rows = result["rows"]
            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": _serialize_for_model(result),
            })

    # Budget exhausted — if we collected rows, use the SAME answer pipeline as
    # normal mode (few-shots, structured envelope, formatted key_numbers).
    # That eliminates the "reasoning answers feel rougher than normal" tax.
    if last_rows:
        last_sql = sql_history[-1] if sql_history else ""
        try:
            synth = write_answer(
                question, last_sql, last_rows[:60], grounding_text="",
                extra_evidence=_evidence_digest(tool_results),
            )
        except Exception:
            synth = None
        if synth and synth.get("valid", True) and synth.get("answer"):
            trace.append({"step": max_calls, "name": "synthesize_from_rows", "summary": "budget exhausted; synthesised via normal-mode answer pipeline"})
            return _finish(
                "budget_synthesised",
                synth["answer"],
                synth.get("key_numbers", []) or [],
                synth.get("caveats", []) or [],
                max_calls,
            )

    return _finish("call_budget", "I used my full tool-call budget without producing a confident answer. Please narrow the question.", [], [], max_calls)
