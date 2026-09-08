"""Bounded multi-step reasoning for complex analytical requests.

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

from app.core.conversation import structured_memory
from app.core.reasoning_tools import TOOL_SCHEMAS, execute_tool
from app.core.response_writer import write_answer
from app.llm import client
from app.schemas.answer import FinalAnswer
from app.semantic.registry import catalog_for_prompt, domain_summary

_AGENT_SYSTEM = """You are a careful policy-data analyst with read-only access
to a fixed US public-policy catalog. You answer the user's question by calling
small tools and reasoning over the results — NEVER by inventing values.

TOOLS
- get_schema(table)            see a table's full columns + CRITICAL warnings
- distinct_values(table, col)  list real values of a filter column (with pattern)
- run_sql(sql)                 execute one validator-gated SELECT/WITH
- peer_stats(table, measure)   min/p25/median/mean/p75/max + top5/bottom5
- answer(text, key_numbers, caveats, primary_evidence_id,
  supporting_evidence_ids)             TERMINATE with the final answer

RULES
- Stay grounded: every number / ranking / claim in `text` must be supported by
  data returned from a tool call in THIS turn. No fabrication.
- Every tool observation includes an `evidence_id`. When calling `answer`, set
  `primary_evidence_id` to the one row-producing result that should power the
  visible SQL, Data, chart, and map. Put every other result actually used in
  `supporting_evidence_ids`. Never select a later sample or sanity check merely
  because it was the most recent call.
- Cover every metric, entity, geography, comparison, and period explicitly
  requested. For top/bottom N, include all N returned rows. If a requested
  part has no evidence, identify that gap instead of silently skipping it.
- When you use a returned supporting statistic to justify the conclusion
  (sample size, average, median, component value, margin, or coefficient),
  state its exact returned value in `text`. Never allude to a supporting
  comparison without showing the numbers that make it auditable.
- For a single highest/lowest entity, check for ties and report every entity at
  the extreme. Prefer RANK/DENSE_RANK = 1; a bare LIMIT 1 can hide a correct
  co-winner.
- For statistics across joined datasets, make sample coverage auditable. Return
  the eligible geography count, the complete-case count actually used by each
  statistic, and missing geography labels when the answer discusses exclusions.
  Never attach one sample size to several medians/correlations whose null
  patterns differ.
- If the user asks for outliers, calculate them with a disclosed method and
  return the actual outlier rows. A range, intuition, or guessed entity name is
  not an outlier analysis.
- When the user prompt includes a REQUIRED ANALYSIS CONTRACT, treat its exact
  tables, metrics, periods, formula, predicate, and direction as authoritative.
  The contract is the planner's resolved interpretation; do not reopen it.
- SQL may reference catalog views only: always write `mart_<table_id>` in FROM
  and JOIN clauses, never the bare table id shown in the contract.
- A subtract formula that repeats the same metric represents a difference
  between two observations of that measure (for example, an upper and lower
  extreme named in the question). Prefer one SQL that returns both entity
  labels, both raw values, and MAX(metric) - MIN(metric) as the gap.
- Percentile / quartile / median claims must be arithmetically true from
  numbers a tool returned: 0.824 is NOT "above the 75th percentile" when the
  p75 is 0.826. When two numbers are close, quote both instead of a bucket
  label ("top quartile"/"bottom quartile"). Never invent a threshold.
- Flow totals include same-geography subawards unless SQL explicitly excludes
  them. Never call an unfiltered flow total funding only to/from "other states";
  say "all states, including intra-state flows" when that scope matters.
- When ranking which geography receives inflow, rank the subawardee/destination
  geography. When ranking which geography sends outflow, rank the rcpt/origin
  geography. Switch sides only when the user explicitly asks for sources or
  destinations.
- If you're not 100% sure of a column name, casing, or filter value, call
  `get_schema` and/or `distinct_values` BEFORE `run_sql`.
- For "is X high?", "where does X stand?", or distributional questions, prefer
  `peer_stats` over hand-rolled SQL.
- For "X compared with its peers" without a named cohort, use every other
  represented geography at the same grain and period. `peer_stats` is usually
  the most efficient evidence source; do not ask the user to define peers.
- For proportionality, per-capita, ratio, or other DERIVED peer comparisons
  spanning multiple tables, `peer_stats` cannot calculate the derived measure.
  Use one joined SQL: compute the derived value for every peer in a CTE, then
  return the focus value together with its rank and peer median/mean from that
  same derived distribution. Do not spend calls trying `peer_stats` on a
  synthetic cross-table column.
- For that one-query pattern, compute RANK/COUNT/AVG/MEDIAN in a second CTE
  across the COMPLETE derived peer set, and only then filter the outer SELECT
  to the focus entity. Filtering before window functions makes the focus rank
  1 of 1 and is wrong. DuckDB supports MEDIAN(value) and QUANTILE_CONT(value,
  fraction); do not use unsupported PERCENTILE_CONT syntax. The final SQL must
  still contain the named focus filter so the semantic contract can verify it.
- For qualitative distribution questions, use a transparent standard method
  and name it in the answer. Compare unevenness across differently scaled
  groups with relative spread (for example, coefficient of variation) rather
  than raw ranges; assess concentration within one distribution with a
  top-share or HHI-style measure. Proceed with that disclosed assumption
  instead of asking the user to choose among equivalent summary conventions.
- When a county/district is compared with its "state median", compute the
  median across represented counties/districts inside that state at the same
  grain. A row from a state-level aggregate table is a statewide value, not a
  median. Clearly label whichever benchmark the user requested.
- IMPORTANT: as soon as a `run_sql` (or `peer_stats`) returns the rows that
  answer the question, CALL `answer` immediately. Do not keep exploring.
  Default to 1 SQL + answer; only add more tool calls if the data genuinely
  doesn't answer the question yet.
- `run_sql` reports whether its returned rows were capped. If `truncated` is
  true, never imply that the visible names are exhaustive and do not retry the
  same large query or pack hundreds of labels into one string. Use one compact
  aggregate for the exact count plus representative top/bottom rows, disclose
  that the displayed list is partial, and answer.
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


def _build_user(
    question: str,
    history: list[dict[str, Any]] | None,
    *,
    operation: str | None = None,
    flow_direction: str | None = None,
    analysis_contract: dict[str, Any] | None = None,
) -> str:
    parts = [f"QUESTION: {question}"]
    if operation:
        contract = [f"operation={operation}"]
        if flow_direction and flow_direction != "none":
            contract.append(f"flow_direction={flow_direction}")
        if operation == "aggregate":
            contract.append(
                "return the requested scalar total/amount as the primary result; "
                "do not replace it with a destination, source, agency, or other breakdown"
            )
        parts.append("REQUIRED ANSWER CONTRACT: " + "; ".join(contract))
    if analysis_contract:
        fields = (
            "tables",
            "metric_columns",
            "geography_level",
            "operation",
            "statistic",
            "result_unit",
            "formula",
            "predicate",
            "observation_grain",
            "result_scope",
            "sort_direction",
            "top_k",
            "flow_direction",
            "period_by_table",
            "output_dimensions",
            "join_plan",
        )
        compact_contract = {
            field: analysis_contract[field]
            for field in fields
            if field in analysis_contract and analysis_contract[field] not in (None, "", [], {})
        }
        parts.append(
            "REQUIRED ANALYSIS CONTRACT (authoritative):\n"
            + json.dumps(compact_contract, default=str)
        )
    if history:
        memory = structured_memory(history)
        if memory:
            parts.insert(
                0,
                memory
                + "\nUse this only for omitted follow-up context; the current QUESTION overrides it.",
            )
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
        return {
            "row_count": result.get("row_count", 0),
            "truncated": bool(result.get("truncated")),
            "sql": (result.get("sql", "") or "")[:160],
        }
    if name == "distinct_values":
        return {"count": result.get("count", 0), "sample": result.get("values", [])[:5]}
    if name == "peer_stats":
        s = result.get("stats", {}) or {}
        return {
            "n": s.get("n"),
            "min": s.get("min"),
            "median": s.get("median"),
            "max": s.get("max"),
        }
    if name == "get_schema":
        return {"len": len(result.get("schema", ""))}
    return {"ok": True}


def _serialize_for_model(result: dict[str, Any], cap: int = 4000) -> str:
    blob = json.dumps(result, default=str)
    if len(blob) <= cap:
        return blob
    rows = result.get("rows")
    if isinstance(rows, list):
        # Preserve control metadata such as the executor's truncation flag.
        # Blindly slicing the JSON loses fields placed after a large row array
        # and makes the model incorrectly treat a capped sample as exhaustive.
        compact = {key: value for key, value in result.items() if key != "rows"}
        compact["rows"] = rows[:12]
        compact["rows_note"] = f"showing 12 of {len(rows)} returned rows" + (
            "; executor result was capped" if result.get("truncated") else ""
        )
        blob = json.dumps(compact, default=str)
    return blob if len(blob) <= cap else (blob[:cap] + "…[payload truncated]")


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
            rows = res.get("rows") or []
            parts.append(
                {
                    "evidence_id": tr.get("evidence_id"),
                    "tool": "run_sql",
                    "sql": (res.get("sql") or "")[:300],
                    "row_count": res.get("row_count"),
                    "truncated": bool(res.get("truncated")),
                    "rows": rows[:12],
                }
            )
        elif name == "peer_stats":
            parts.append(
                {
                    "evidence_id": tr.get("evidence_id"),
                    "tool": "peer_stats",
                    "args": tr.get("args"),
                    "stats": res.get("stats"),
                    "top5": res.get("top5"),
                    "bottom5": res.get("bottom5"),
                    "focus": res.get("focus"),
                    "rank_direction": res.get("rank_direction"),
                }
            )
    if not parts:
        return ""
    # Preserve both the first requested result and later supplemental checks.
    # Blindly slicing the first 6 KB dropped the second half of multi-part
    # questions (for example, a negative subset after a leaderboard).
    if len(parts) > 8:
        parts = parts[:2] + parts[-6:]
    return json.dumps(parts, default=str)[:12000]


def _ranking_evidence_priority(rows: list[dict[str, Any]]) -> int:
    """Prefer a focused rank row over a generic leaderboard or scalar summary."""

    keys = {str(key).casefold() for row in rows for key in row}
    if keys & {"rank", "rank_asc", "rank_desc", "position"}:
        return 3
    if len(rows) > 1:
        return 2
    return 1


def _row_evidence(
    tool_results: list[dict[str, Any]], evidence_id: str | None
) -> tuple[list[dict[str, Any]], str | None, bool] | None:
    """Resolve one explicitly cited, displayable tool observation."""

    if not evidence_id:
        return None
    for tool_result in tool_results:
        if tool_result.get("evidence_id") != evidence_id:
            continue
        result = tool_result.get("result") or {}
        if result.get("error") or tool_result.get("name") not in {"run_sql", "peer_stats"}:
            return None
        rows = [row for row in (result.get("rows") or []) if isinstance(row, dict)]
        sql = result.get("sql")
        if not rows or not isinstance(sql, str) or not sql.strip():
            return None
        return rows, sql, bool(result.get("truncated"))
    return None


def _displayable_evidence_ids(tool_results: list[dict[str, Any]]) -> list[str]:
    return [
        str(tool_result["evidence_id"])
        for tool_result in tool_results
        if _row_evidence(tool_results, str(tool_result.get("evidence_id") or "")) is not None
    ]


def _cite_synthesis_evidence(
    tool_results: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    sql: str | None,
) -> tuple[str | None, list[str]]:
    """Attach row-grounded synthesis to the evidence it actually displays.

    The fallback answer writer receives the complete evidence digest, while
    the visible table/SQL comes from one selected observation. Preserve both
    facts in the same citation contract used by a normal ``answer`` tool call.
    """

    displayable_ids = _displayable_evidence_ids(tool_results)
    primary_id = next(
        (
            evidence_id
            for evidence_id in displayable_ids
            if (resolved := _row_evidence(tool_results, evidence_id)) is not None
            and resolved[0] == rows
            and resolved[1] == sql
        ),
        None,
    )
    supporting_ids = [evidence_id for evidence_id in displayable_ids if evidence_id != primary_id]
    return primary_id, supporting_ids


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
    operation: str | None = None,
    flow_direction: str | None = None,
    analysis_contract: dict[str, Any] | None = None,
    focus_values_by_column: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    emit = on_event or (lambda _n, _d: None)
    """Run the agent. Returns a dict the orchestrator can wrap into the
    standard envelope: {answer, key_numbers, caveats, sql, rows, trace,
    stopped_reason, used_tokens, steps}."""
    system = _AGENT_SYSTEM.format(
        domain=domain_summary(),
        catalog=catalog_for_prompt(allowed_tables) if allowed_tables else catalog_for_prompt(),
    )
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system},
        {
            "role": "user",
            "content": _build_user(
                question,
                history,
                operation=operation,
                flow_direction=flow_direction,
                analysis_contract=analysis_contract,
            ),
        },
    ]
    trace: list[dict[str, Any]] = []
    tool_results: list[dict[str, Any]] = []  # full results — used by faithfulness/judge
    sql_history: list[str] = []
    last_rows: list[dict[str, Any]] = []
    last_truncated = False
    primary_sql: str | None = None
    primary_rows: list[dict[str, Any]] = []
    primary_truncated = False
    answer_evidence_id: str | None = None
    supporting_evidence_ids: list[str] = []
    used_tokens = 0
    started = time.time()

    def _selected_evidence() -> tuple[list[dict[str, Any]], str | None, bool]:
        explicitly_selected = _row_evidence(tool_results, answer_evidence_id)
        if explicitly_selected is not None:
            return explicitly_selected
        if operation in {"aggregate", "ranking"} and primary_rows:
            return primary_rows, primary_sql, primary_truncated
        return last_rows, sql_history[-1] if sql_history else None, last_truncated

    def _finish(
        reason: str, answer: str, key_numbers: list, caveats: list, step: int
    ) -> dict[str, Any]:
        # An investigation may run a direct scalar followed by a supplemental
        # breakdown. For an aggregate question, the scalar is still the
        # requested result and must remain the displayed SQL/data.
        selected_rows, selected_sql, selected_truncated = _selected_evidence()
        cited_ids = {value for value in [answer_evidence_id, *supporting_evidence_ids] if value}
        cited_tool_results = (
            [item for item in tool_results if item.get("evidence_id") in cited_ids]
            if cited_ids
            else list(tool_results)
        )
        return {
            "answer": answer,
            "key_numbers": key_numbers,
            "caveats": caveats,
            "sql": selected_sql,
            "sql_history": sql_history,
            "rows": selected_rows,
            "truncated": selected_truncated,
            "trace": trace,
            "tool_results": tool_results,
            "primary_evidence_id": answer_evidence_id,
            "supporting_evidence_ids": list(supporting_evidence_ids),
            "evidence_digest": _evidence_digest(tool_results),
            "cited_evidence_digest": _evidence_digest(cited_tool_results),
            "stopped_reason": reason,
            "used_tokens": used_tokens,
            "steps": step,
        }

    for step in range(1, max_calls + 1):
        if time.time() - started > max_wall_s:
            return _finish(
                "wall_budget",
                "I couldn't complete this analysis within the request window. Please try again or narrow the scope.",
                [],
                [],
                step - 1,
            )
        if used_tokens > max_tokens:
            return _finish(
                "token_budget",
                "I couldn't complete this analysis within the request window. Please narrow the scope and try again.",
                [],
                [],
                step - 1,
            )

        try:
            resp = client.chat_tools(
                messages,
                tools=TOOL_SCHEMAS,
                temperature=0.0,
                max_tokens=1500,
                purpose="reasoning_agent",
            )
        except client.LLMError:
            return _finish(
                "llm_error",
                "The analysis service became temporarily unavailable before it could finish. Please retry.",
                [],
                [],
                step - 1,
            )

        used_tokens += resp["usage"]["total_tokens"]
        tool_calls = resp["tool_calls"]
        content = resp["content"]

        raw_assistant = resp.get("assistant_message")
        assistant_msg: dict[str, Any] = (
            dict(raw_assistant)
            if isinstance(raw_assistant, dict)
            else {"role": "assistant", "content": content or ""}
        )
        if tool_calls and not assistant_msg.get("tool_calls"):
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
            trace.append(
                {
                    "step": step,
                    "name": "no_tool_call",
                    "summary": "model returned prose without a tool call; routing to row-grounded synthesis",
                }
            )
            synthesis_rows, synthesis_sql, synthesis_truncated = _selected_evidence()
            if synthesis_rows:
                try:
                    synth = write_answer(
                        question,
                        synthesis_sql or "",
                        synthesis_rows[:60],
                        grounding_text="",
                        extra_evidence=_evidence_digest(tool_results),
                        truncated=synthesis_truncated,
                    )
                except Exception:
                    synth = None
                if synth and synth.get("valid", True) and synth.get("answer"):
                    answer_evidence_id, supporting_evidence_ids = _cite_synthesis_evidence(
                        tool_results,
                        synthesis_rows,
                        synthesis_sql,
                    )
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
                requested_primary = str(args.get("primary_evidence_id") or "").strip()
                requested_supporting = list(args.get("supporting_evidence_ids") or [])
                available_evidence = _displayable_evidence_ids(tool_results)
                invalid_supporting = [
                    str(value)
                    for value in requested_supporting
                    if str(value) not in available_evidence or str(value) == requested_primary
                ]
                if available_evidence and (
                    requested_primary not in available_evidence or invalid_supporting
                ):
                    issue = (
                        "answer must cite one valid primary evidence_id and only valid, "
                        "distinct supporting evidence_ids; available row evidence: "
                        + ", ".join(available_evidence)
                    )
                    trace.append(
                        {
                            "step": step,
                            "name": "answer",
                            "summary": {"error": issue},
                        }
                    )
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc["id"],
                            "content": _serialize_for_model({"error": issue}),
                        }
                    )
                    continue
                try:
                    parsed_answer = FinalAnswer.model_validate(
                        {
                            "answer": str(args.get("text", "")).strip(),
                            "key_numbers": list(args.get("key_numbers", []) or []),
                            "caveats": list(args.get("caveats", []) or []),
                        }
                    )
                    if not parsed_answer.answer:
                        raise ValueError("answer text is empty")
                except Exception as exc:
                    result = {"error": f"answer schema validation: {exc}"}
                    trace.append(
                        {
                            "step": step,
                            "name": "answer",
                            "summary": {"error": str(result["error"])[:200]},
                        }
                    )
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc["id"],
                            "content": _serialize_for_model(result),
                        }
                    )
                    continue
                trace.append(
                    {
                        "step": step,
                        "name": "answer",
                        "summary": _summarize_tool_result("answer", {"ok": True}),
                    }
                )
                answer_evidence_id = requested_primary or None
                supporting_evidence_ids = [
                    str(value) for value in requested_supporting if str(value) in available_evidence
                ]
                return _finish(
                    "ok",
                    parsed_answer.answer,
                    [item.model_dump() for item in parsed_answer.key_numbers],
                    list(parsed_answer.caveats),
                    step,
                )
            execution_args = dict(args)
            if name == "peer_stats":
                geo_column = str(execution_args.get("geo_column") or "state")
                focus_values = list((focus_values_by_column or {}).get(geo_column) or [])
                if not execution_args.get("focus_value") and len(focus_values) == 1:
                    execution_args["focus_value"] = focus_values[0]
                planned_direction = str((analysis_contract or {}).get("sort_direction") or "")
                if planned_direction in {"asc", "desc"}:
                    execution_args["sort_direction"] = planned_direction
            emit("tool_start", {"step": step, "name": name, "args": execution_args})
            result = execute_tool(
                name,
                execution_args,
                semantic_guard=semantic_guard,
                allowed_tables=allowed_tables,
            )
            evidence_id = f"E{len(tool_results) + 1}"
            result = {**result, "evidence_id": evidence_id}
            summary = _summarize_tool_result(name, result)
            trace.append({"step": step, "name": name, "args": execution_args, "summary": summary})
            tool_results.append(
                {
                    "evidence_id": evidence_id,
                    "name": name,
                    "args": execution_args,
                    "result": result,
                }
            )
            emit(
                "tool",
                {"step": step, "name": name, "summary": summary, "error": result.get("error")},
            )
            result_rows = result.get("rows")
            if (
                name == "run_sql"
                and not result.get("error")
                and isinstance(result_rows, list)
                and result_rows
            ):
                sql_history.append(result["sql"])
                last_rows = [row for row in result_rows if isinstance(row, dict)]
                last_truncated = bool(result.get("truncated"))
                if not primary_rows:
                    if operation == "aggregate" and len(last_rows) == 1:
                        primary_sql = result["sql"]
                        primary_rows = list(last_rows)
                        primary_truncated = last_truncated
                    elif operation == "ranking":
                        primary_sql = result["sql"]
                        primary_rows = list(last_rows)
                        primary_truncated = last_truncated
                elif operation == "ranking" and _ranking_evidence_priority(
                    last_rows
                ) > _ranking_evidence_priority(primary_rows):
                    primary_sql = result["sql"]
                    primary_rows = list(last_rows)
                    primary_truncated = last_truncated
            elif name == "peer_stats" and not result.get("error"):
                peer_rows = result.get("rows")
                peer_sql = result.get("sql")
                if isinstance(peer_rows, list) and peer_rows and isinstance(peer_sql, str):
                    sql_history.append(peer_sql)
                    last_rows = [row for row in peer_rows if isinstance(row, dict)]
                    if operation == "ranking" and (
                        not primary_rows
                        or _ranking_evidence_priority(last_rows)
                        > _ranking_evidence_priority(primary_rows)
                    ):
                        primary_sql = peer_sql
                        primary_rows = list(last_rows)
                        primary_truncated = False
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": _serialize_for_model(result),
                }
            )

    # Budget exhausted — if we collected rows, use the SAME answer pipeline as
    # normal mode (few-shots, structured envelope, formatted key_numbers).
    # That eliminates the "reasoning answers feel rougher than normal" tax.
    synthesis_rows, synthesis_sql, synthesis_truncated = _selected_evidence()
    if synthesis_rows:
        try:
            synth = write_answer(
                question,
                synthesis_sql or "",
                synthesis_rows[:60],
                grounding_text="",
                extra_evidence=_evidence_digest(tool_results),
                truncated=synthesis_truncated,
            )
        except Exception:
            synth = None
        if synth and synth.get("valid", True) and synth.get("answer"):
            answer_evidence_id, supporting_evidence_ids = _cite_synthesis_evidence(
                tool_results,
                synthesis_rows,
                synthesis_sql,
            )
            trace.append(
                {
                    "step": max_calls,
                    "name": "synthesize_from_rows",
                    "summary": "budget exhausted; synthesised via normal-mode answer pipeline",
                }
            )
            return _finish(
                "budget_synthesised",
                synth["answer"],
                synth.get("key_numbers", []) or [],
                synth.get("caveats", []) or [],
                max_calls,
            )

    return _finish(
        "call_budget",
        "I couldn't complete a well-supported answer for that scope. Please narrow the question and try again.",
        [],
        [],
        max_calls,
    )
