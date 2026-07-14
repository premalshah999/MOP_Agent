"""Pipeline orchestrator — LLM-grounded text-to-SQL.

Stage 1 intent -> Stage 2 routing -> Stage 3 grounding -> Stage 4 SQL (with
self-repair) + grounded answer + faithfulness judge. The `answer_question()`
return contract is unchanged so the FastAPI app, threads, auth, and frontend
keep working.
"""

from __future__ import annotations

from typing import Any, Callable

from app.core import meta_answer
from app.core.answer_writer import write_answer
from app.core.clarifier import generate_clarification_chips
from app.core.formatting import format_key_numbers, validate_key_numbers_against_rows
from app.core.glossary import detect_terms
from app.core.peer_context import compute_peer_context, render_peer_context
from app.core.verified_queries import match as match_verified_query
from app.duckdb.connection import execute_select
from app.sql.validator import SqlValidationError, validate_sql
from app.core.grounding import build_grounding
from app.core.intent import classify_intent  # kept for stage1 tests
from app.core.router import route as route_question  # kept for stage2 tests
from app.core.intent_route import classify_and_route
from app.core.reasoning_agent import run_reasoning_agent
from app.core.sql_writer import generate_and_execute
from app.core.contextualize import contextualize
from app.core.suggestions import suggest_followups
from app.core.visuals import build_visuals, enrich_rows_for_map

import os
import re as _re


def _is_trivial_scalar_answer(
    answer: str, key_numbers: list[dict[str, Any]], rows: list[dict[str, Any]]
) -> bool:
    """A single-row result whose numeric values are all already in the answer
    (or key_numbers) within 1% rounding doesn't need the LLM faithfulness judge."""
    if len(rows) != 1:
        return False
    row = rows[0]
    row_nums = [v for v in row.values() if isinstance(v, (int, float)) and not isinstance(v, bool)]
    if not row_nums:
        return False
    in_keys: list[float] = []
    for k in key_numbers:
        try:
            in_keys.append(float(k.get("value")))
        except (TypeError, ValueError):
            continue
    in_text = [
        float(t.replace(",", ""))
        for t in _re.findall(r"-?\d[\d,]*\.?\d*", answer or "")
    ]
    pool = in_keys + in_text
    if not pool:
        return False

    def covered(v: float) -> bool:
        tol = 0.01 * max(1.0, abs(v))
        return any(abs(c - v) <= tol for c in pool)

    return all(covered(v) for v in row_nums)
from app.evals.faithfulness import judge_faithfulness
from app.observability.logging import log_pipeline_event
from app.semantic.registry import critical_warnings_for, get_dataset
from app.semantic.value_resolver import RESOLVABLE_COLUMNS, resolve_filter_value


_SQL_STR_LITERAL = _re.compile(r"'([^']{2,80})'")


def _find_value_fixes(sql: str, table: str | None) -> list[tuple[str, str, float]]:
    """Extract WHERE string literals from `sql`, fuzz-resolve each against the
    table's real values, return the suggested corrections with scores.

    Skips literals that exactly match a value in any column (the typo is
    elsewhere). Returns only matches with score >= 0.78."""
    if not sql or not table:
        return []
    literals = {m.group(1) for m in _SQL_STR_LITERAL.finditer(sql)}
    if not literals:
        return []
    fixes: list[tuple[str, str, float]] = []
    for lit in literals:
        if lit.isdigit() or len(lit) > 60:
            continue
        exact_seen = False
        best: tuple[str, float] | None = None
        for col in RESOLVABLE_COLUMNS:
            try:
                m = resolve_filter_value(table, col, lit, min_score=0.6)
            except Exception:
                continue
            if not m:
                continue
            if m[0].lower() == lit.lower():
                exact_seen = True
                break
            if best is None or m[1] > best[1]:
                best = m
        if exact_seen or best is None or best[1] < 0.78:
            continue
        fixes.append((lit, best[0], best[1]))
    return fixes


def _did_you_mean(sql: str, table: str | None) -> str:
    """Soft suggestion text for the answer footer when auto-relax decides not
    to act (e.g. no high-confidence fix). Kept for backward compatibility."""
    fixes = _find_value_fixes(sql, table)
    if not fixes:
        return ""
    return "Did you mean: " + "; ".join(f"`{a}` → `{b}`" for a, b, _ in fixes[:3]) + "?"


def _apply_value_fixes(sql: str, fixes: list[tuple[str, str, float]]) -> str:
    """Rewrite SQL string literals in-place. Each (typo, correction) is
    substituted as `'typo'` → `'correction'`. Other occurrences are untouched."""
    out = sql
    for typo, correction, _ in fixes:
        out = out.replace(f"'{typo}'", f"'{correction}'")
    return out

PIPELINE_VERSION = "llm-grounded-v3"
PIPELINE_READY = True


def _empty_map_intent() -> dict[str, Any]:
    return {"enabled": False, "mapType": "none"}


def _stage(name: str, status: str, **data: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {"name": name, "status": status}
    if data:
        entry["data"] = data
    return entry


def _envelope(
    *,
    question: str,
    answer: str,
    resolution: str,
    confidence: str,
    stages: list[dict[str, Any]],
    sql: str | None = None,
    rows: list[dict[str, Any]] | None = None,
    tables: list[str] | None = None,
    geography_level: str | None = None,
    year: Any = None,
    focus_state: str | None = None,
    metric: str | None = None,
    assumptions: list[str] | None = None,
    caveats: list[str] | None = None,
    key_numbers: list[dict[str, Any]] | None = None,
    quality_warnings: list[str] | None = None,
    chart: dict[str, Any] | None = None,
    charts: list[dict[str, Any]] | None = None,
    map_intent: dict[str, Any] | None = None,
    user_id: int | str | None = None,
    request_id: str | None = None,
    intent: str = "",
    verified_match: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = rows or []
    tables = tables or []
    assumptions = assumptions or []
    caveats = caveats or []
    # Validate every LLM-emitted key_number against the actual rows BEFORE
    # formatting. Drops fabricated values (LLM arithmetic miss) and
    # downgrades confidence if anything was dropped — prevents the most
    # visible hallucination class: a wrong number in the headline callout.
    raw_kn = list(key_numbers or [])
    if raw_kn and rows:
        kept_kn, dropped_kn = validate_key_numbers_against_rows(raw_kn, rows)
        if dropped_kn:
            # User-facing wording: sound deliberate, not like debug output.
            # The dropped labels still go to quality_warnings/logs for us.
            caveats.append(
                "One headline figure was omitted because it couldn't be "
                "verified against the query results."
                if len(dropped_kn) == 1
                else f"{len(dropped_kn)} headline figures were omitted because "
                "they couldn't be verified against the query results."
            )
            # Medium, not low: the unverifiable figure was already removed, and
            # everything still shown traced back to the rows. "Low confidence"
            # beside a VERIFIED badge read as contradictory in the UI.
            if (confidence or "").lower() != "low":
                confidence = "medium"
            quality_warnings = list(quality_warnings or []) + [
                "key_numbers_validation_failed: " + ", ".join(dropped_kn[:5])
            ]
        raw_kn = kept_kn
    # Normalize numeric values to human-readable strings ($1.2M / 82.5% / 12,345).
    key_numbers = format_key_numbers(raw_kn)
    quality_warnings = quality_warnings or []
    charts = charts or []
    map_intent = map_intent or _empty_map_intent()
    supported = resolution == "answered"
    log_pipeline_event(
        {
            "request_id": request_id,
            "user_id": user_id,
            "question": question,
            "intent": intent,
            "resolution": resolution,
            "datasets": tables,
            "metrics": [metric] if metric else [],
            "query_count": 1 if sql else 0,
            "row_count": len(rows),
            "confidence": confidence,
            "quality_status": "warning" if quality_warnings else "ok",
            "warnings": quality_warnings,
        }
    )
    return {
        "answer": answer,
        "sql": sql,
        "data": rows,
        "row_count": len(rows),
        "resolution": resolution,
        "mapIntent": map_intent,
        "chart": chart,
        "charts": charts,
        "resultPackage": {
            "status": resolution,
            "contract_type": intent,
            "tables": tables,
            "assumptions": assumptions,
            "sql": sql,
            "rows": rows,
            "map_intent": map_intent,
            "chart_intent": {"enabled": bool(chart), "type": "vega-lite" if chart else None},
            "final_answer": {"answer": answer, "confidence": confidence},
        },
        "contract": {
            "contract_type": intent,
            "family": tables[0] if tables else None,
            "metric": metric,
            "operation": None,
            "unit": None,
            "geography_level": geography_level,
            "year": year,
            "focus_state": focus_state,
            "sort_direction": None,
            "top_k": None,
            "tables": tables,
            "supported": supported,
            "missing_slots": [],
            "assumptions": assumptions,
            "validation_message": quality_warnings[0] if quality_warnings else None,
        },
        "pipelineTrace": {"version": PIPELINE_VERSION, "stages": stages},
        "quality": {"status": "warning" if quality_warnings else "ok", "warnings": quality_warnings},
        "confidence": confidence,
        "key_numbers": key_numbers,
        "assumptions": assumptions,
        "caveats": caveats,
        # Only the terms actually present in the answer prose or caveats;
        # the frontend wraps matches with <abbr title="..."> tooltips.
        "glossary": detect_terms((answer or "") + "\n" + "\n".join(caveats)),
        # When the Verified Query Repository was hit, surface a small badge
        # so the user sees this answer's SQL was analyst-reviewed.
        "verified_query": verified_match,
    }


_MART_RE = _re.compile(r"\bmart_([a-z_]+)", _re.IGNORECASE)


def _reasoning_mode(
    *,
    question: str,
    q: str,
    history: list[dict[str, Any]],
    stages: list[dict[str, Any]],
    user_id: int | str | None,
    request_id: str | None,
    emit: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Reasoning-mode path: agent loop, then faithfulness + envelope."""
    _emit = emit or (lambda _n, _d: None)

    def _push(name: str, status: str, **data: Any) -> None:
        entry = _stage(name, status, **data)
        stages.append(entry)
        _emit("stage", entry)

    agent = run_reasoning_agent(q, history, on_event=_emit)
    _push(
        "reasoning_agent",
        "completed",
        steps=agent["steps"],
        tool_calls=len(agent["trace"]),
        stopped=agent["stopped_reason"],
        used_tokens=agent["used_tokens"],
    )

    rows = agent["rows"]
    sql = agent["sql"]
    answer = agent["answer"] or "I could not answer that."
    caveats = list(agent["caveats"])
    key_numbers = list(agent["key_numbers"])
    quality_warnings: list[str] = []
    confidence = "high" if agent["stopped_reason"] == "ok" else "low"

    # Derive tables used from the SQL history (best effort) for the contract.
    tables: list[str] = []
    for s in agent.get("sql_history") or []:
        for t in _MART_RE.findall(s):
            t_low = t.lower()
            if t_low and t_low not in tables:
                tables.append(t_low)
    primary_table = tables[0] if tables else None
    dataset = get_dataset(primary_table) if primary_table else None
    routing_stub = {
        "tables": tables,
        "geography_level": dataset.geography if dataset else "none",
        "year_strategy": "",
        "join_plan": "",
        "columns": [],
    }
    display_rows = enrich_rows_for_map(q, routing_stub, {}, rows)
    visuals = build_visuals(q, routing_stub, {}, display_rows)
    _push(
            "visual_recommender",
            "completed",
            chart=bool(visuals["chart"]),
            map=visuals["map_intent"].get("mapType") if visuals["map_intent"].get("enabled") else "none",
        )

    resolution_preview = "answered" if rows else ("answered" if agent["stopped_reason"] == "ok" else "error")
    _emit("answer_preview", {
        "answer": answer,
        "sql": sql,
        "data": display_rows,
        "row_count": len(display_rows),
        "chart": visuals["chart"],
        "charts": visuals["charts"],
        "mapIntent": visuals["map_intent"],
        "resolution": resolution_preview,
        "key_numbers": key_numbers,
    })

    # Faithfulness gate on the final answer; the agent may have used
    # peer_stats / multiple SQLs, so we pass the full tool trail too.
    tool_results = agent.get("tool_results") or []
    if (rows or tool_results) and not _is_trivial_scalar_answer(answer, key_numbers, rows):
        verdict = judge_faithfulness(q, answer, rows, sql or "", tool_results)
        _push("faithfulness_judge", "completed", **verdict)
        _emit("faithfulness", verdict)
        if not verdict["faithful"]:
            confidence = "low"
            caveats.append(f"An automated check couldn't verify part of this answer: {verdict['reason']}")
            quality_warnings.append("answer failed automated faithfulness check")
    else:
        _push("faithfulness_judge", "skipped", reason="no_rows_or_trivial")

    # Without rows, only call it "answered" when the agent terminated cleanly
    # (stopped_reason == "ok"); budget exhaustion / errors are surfaced honestly.
    if rows:
        resolution = "answered"
    elif agent["stopped_reason"] == "ok":
        resolution = "no_data"
    else:
        resolution = "error"
    envelope = _envelope(
        question=question,
        answer=answer,
        resolution=resolution,
        confidence=confidence,
        stages=stages,
        sql=sql,
        rows=display_rows,
        tables=tables,
        geography_level=dataset.geography if dataset else None,
        year=None,
        focus_state=None,
        metric=None,
        caveats=caveats,
        key_numbers=key_numbers,
        quality_warnings=quality_warnings,
        chart=visuals["chart"],
        charts=visuals["charts"],
        map_intent=visuals["map_intent"],
        intent="ANALYTICAL",
        user_id=user_id,
        request_id=request_id,
    )
    # Surface the full agent tool trail so the faithfulness judge and the
    # reasoning evaluator can see peer_stats / multi-SQL evidence, not just
    # the most recent run_sql rows.
    envelope["resultPackage"]["tool_results"] = agent.get("tool_results", [])
    envelope["resultPackage"]["sql_history"] = agent.get("sql_history", [])
    if envelope.get("resolution") == "answered":
        sf = suggest_followups(question, answer, envelope.get("contract"))
        envelope["suggested_followups"] = sf
        _emit("suggested_followups", {"items": sf})
    return envelope


def answer_question(
    question: str,
    history: list[dict[str, Any]] | None = None,
    *,
    user_id: int | str | None = None,
    request_id: str | None = None,
    mode: str = "normal",
    on_event: "Callable[[str, dict[str, Any]], None] | None" = None,
) -> dict[str, Any]:
    history = history or []
    mode = (mode or "normal").lower()
    _emit = on_event or (lambda _name, _data: None)

    def _push(name: str, status: str, **data: Any) -> None:
        entry = _stage(name, status, **data)
        stages.append(entry)
        _emit("stage", entry)
    if mode not in {"normal", "reasoning"}:
        mode = "normal"
    stages: list[dict[str, Any]] = []

    q = contextualize(question, history)
    if q != question:
        _push("contextualize", "completed", standalone=q)

    ir = classify_and_route(q, history)
    intent = {k: ir[k] for k in ("intent", "requires_sql", "needs_clarification", "clarification_question", "reason")}
    _push("stage1_intent", "completed", **intent)

    if intent["intent"] != "ANALYTICAL":
        meta = meta_answer.respond(q, intent["intent"], intent)
        _push("non_analytical_responder", "completed", intent=intent["intent"])
        env = _envelope(
            question=question,
            answer=meta["answer"],
            resolution=meta["resolution"],
            confidence=meta["confidence"],
            stages=stages,
            intent=intent["intent"],
            user_id=user_id,
            request_id=request_id,
        )
        # CLARIFY intent — attach the concrete clickable chips so users don't
        # have to retype the question. (The earlier analytical-needs_clarification
        # branch does the same; this matches behavior for the LLM-classified
        # CLARIFY path that bypasses routing.)
        if intent["intent"] == "CLARIFY":
            try:
                chips = generate_clarification_chips(q)
            except Exception:
                chips = []
            if chips:
                env["suggested_followups"] = chips
        return env

    if mode == "reasoning":
        return _reasoning_mode(
            question=question, q=q, history=history, stages=stages,
            user_id=user_id, request_id=request_id, emit=_emit,
        )

    routing = {k: ir[k] for k in (
        "tables", "columns", "geography_level", "year_strategy", "join_plan",
        "needs_clarification", "clarification", "confidence", "reason",
    )}
    _push("stage2_routing", "completed", **routing)
    if not routing["tables"] or routing["needs_clarification"]:
        ask = routing["clarification"] or "Which dataset and measure should I use?"
        # Attach 3-5 concrete clickable alternatives so the user doesn't have
        # to retype the question from scratch. Falls back to LLM-grounded
        # suggestions for novel ambiguity shapes.
        chips: list[str] = []
        try:
            chips = generate_clarification_chips(q)
        except Exception:
            chips = []
        env = _envelope(
            question=question,
            answer=ask,
            resolution="needs_clarification",
            confidence="medium",
            stages=stages,
            tables=routing["tables"],
            geography_level=routing["geography_level"],
            intent="ANALYTICAL",
            user_id=user_id,
            request_id=request_id,
        )
        if chips:
            env["suggested_followups"] = chips
        return env

    grounding = build_grounding(
        q,
        routing["tables"],
        year_strategy=routing["year_strategy"],
        join_plan=routing["join_plan"],
    )
    _push("stage3_retrieval", "completed", tables=routing["tables"], resolved=grounding["resolved"])

    # Verified Query Repository. Two modes, chosen by the matcher:
    #   exact    — the question means the same thing as the blessed one
    #              (same direction/numbers/geography): execute the blessed
    #              SQL verbatim. Zero hallucination surface, VERIFIED badge.
    #   exemplar — lexically similar but salient tokens differ (bottom vs
    #              top, 2019 vs 2023, outflow vs inflow): the LLM still
    #              writes the SQL, with the blessed pair injected as a
    #              verified reference to adapt. The LLM decides; the
    #              repository only informs. Executing fuzzy matches verbatim
    #              served opposite-direction answers in adversarial testing.
    verified_match: dict[str, Any] | None = None
    exemplar: dict[str, Any] | None = None
    try:
        verified_match = match_verified_query(q)
    except Exception:
        verified_match = None
    if verified_match and verified_match.get("_mode") != "exact":
        exemplar = verified_match
        _push("verified_query_match", "exemplar",
              id=exemplar.get("id"), score=round(exemplar.get("_score", 0.0), 3))
        verified_match = None
    if verified_match:
        _push("verified_query_match", "matched",
              id=verified_match.get("id"), score=round(verified_match.get("_score", 0.0), 3))
        try:
            blessed_sql = str(verified_match["sql"]).strip()
            validate_sql(blessed_sql)
            blessed_rows = execute_select(
                blessed_sql,
                max_rows=int(os.getenv("MAX_RETURN_ROWS", "250")),
            )
            gen = {
                "sql": blessed_sql,
                "rows": blessed_rows,
                "error": None if blessed_rows else "empty_result",
                "attempts": [],
            }
            _push("stage4_sql_generation", "skipped",
                  reason="verified_query_used", row_count=len(blessed_rows))
        except (SqlValidationError, Exception) as exc:
            # Blessed SQL failed — fall back to LLM path with a quality warning.
            _push("verified_query_match", "fallback", reason=str(exc)[:120])
            exemplar = verified_match
            verified_match = None

    if verified_match is None:
        gen = generate_and_execute(
            q, grounding["text"], history, routing["tables"], exemplar=exemplar
        )
        _push(
            "stage4_sql_generation",
            "completed" if gen["sql"] else "failed",
            attempts=len(gen["attempts"]),
            error=gen["error"],
            row_count=len(gen["rows"]),
        )

    if not gen["sql"] or (gen["error"] and gen["error"] != "empty_result" and not gen["rows"]):
        return _envelope(
            question=question,
            answer=(
                "I could not produce a valid query for that. Try rephrasing, or "
                "specify the measure, geography level, and time period."
            ),
            resolution="error",
            confidence="low",
            stages=stages,
            sql=gen["sql"] or None,
            tables=routing["tables"],
            geography_level=routing["geography_level"],
            intent="ANALYTICAL",
            quality_warnings=[f"SQL generation failed: {gen['error']}"],
            user_id=user_id,
            request_id=request_id,
        )

    # Auto-relax: 0-row result + a high-confidence value-fix → rewrite the SQL
    # literal and re-run BEFORE composing the answer. Avoids the dead-end where
    # the user sees "no data" plus a "did you mean" suggestion they have to
    # retype manually.
    relax_note: str | None = None
    if not gen["rows"] and routing["tables"]:
        fixes = _find_value_fixes(gen["sql"] or "", routing["tables"][0])
        strong = [f for f in fixes if f[2] >= 0.85]
        if strong:
            relaxed_sql = _apply_value_fixes(gen["sql"] or "", strong)
            if relaxed_sql != gen["sql"]:
                try:
                    validate_sql(relaxed_sql)
                    relaxed_rows = execute_select(
                        relaxed_sql,
                        max_rows=int(os.getenv("MAX_RETURN_ROWS", "250")),
                    )
                except (SqlValidationError, Exception):
                    relaxed_rows = []
                if relaxed_rows:
                    subs = "; ".join(f"`{a}` → `{b}`" for a, b, _ in strong[:3])
                    relax_note = f"I couldn't find data for {subs}. Showing the closest match instead."
                    gen = {**gen, "sql": relaxed_sql, "rows": relaxed_rows, "error": None}
                    _push("auto_relax", "completed", substitutions=[{"from": a, "to": b, "score": s} for a, b, s in strong])

    # Peer / comparative context: cheap side-queries to give a single-state
    # answer real meaning (rank, vs national median, YoY). Never load-bearing
    # — failures silently produce None. Passed as a dedicated prompt section.
    peer_text = ""
    if gen["rows"] and routing["tables"]:
        resolved0 = grounding["resolved"].get(routing["tables"][0], {})
        fstate_val = resolved0.get("state", {}).get("value") if isinstance(resolved0.get("state"), dict) else None
        peer = compute_peer_context(
            table=routing["tables"][0],
            focus_state=fstate_val,
            year=routing["year_strategy"],
            routing_columns=routing["columns"] or [],
            rows=gen["rows"],
        )
        peer_text = render_peer_context(peer) if peer else ""
        if peer_text:
            _push("peer_context", "completed", **{k: v for k, v in (peer or {}).items() if k in ("rank", "total_states", "yoy_change_pct")})

    final = write_answer(q, gen["sql"], gen["rows"], grounding["text"], peer_text)
    _push("stage4_answer_generation", "completed", confidence=final["confidence"])

    if relax_note:
        final["answer"] = f"*{relax_note}*\n\n{final['answer']}"

    # If auto-relax didn't act (or didn't find rows) and we still have 0 rows,
    # fall back to surfacing the soft suggestion.
    if not gen["rows"] and routing["tables"]:
        hint = _did_you_mean(gen["sql"] or "", routing["tables"][0])
        if hint:
            final["answer"] = (final["answer"] or "").rstrip() + f"\n\n*{hint}*"

    # Compute visuals BEFORE faithfulness so the answer + chart + map can be
    # streamed to the UI the moment the answer is written — faithfulness +
    # suggested follow-ups attach as later events.
    resolution = "answered" if gen["rows"] else "no_data"
    primary_table = routing["tables"][0]
    dataset = get_dataset(primary_table)
    resolved = grounding["resolved"].get(primary_table, {})
    focus_state = resolved.get("state", {}).get("value") if isinstance(resolved.get("state"), dict) else None

    display_rows = enrich_rows_for_map(q, routing, grounding["resolved"], gen["rows"])
    visuals = build_visuals(q, routing, grounding["resolved"], display_rows)
    _push(
            "visual_recommender",
            "completed",
            chart=bool(visuals["chart"]),
            map=visuals["map_intent"].get("mapType") if visuals["map_intent"].get("enabled") else "none",
        )

    _emit("answer_preview", {
        "answer": final["answer"],
        "sql": gen["sql"],
        "data": display_rows,
        "row_count": len(display_rows),
        "chart": visuals["chart"],
        "charts": visuals["charts"],
        "mapIntent": visuals["map_intent"],
        "resolution": resolution,
        "key_numbers": final["key_numbers"],
    })

    caveats = list(final["caveats"])
    confidence = final["confidence"]
    quality_warnings: list[str] = []
    if gen["rows"]:
        if _is_trivial_scalar_answer(final["answer"], final["key_numbers"], gen["rows"]):
            _push("faithfulness_judge", "skipped", reason="trivial_scalar")
        else:
            verdict = judge_faithfulness(
                q, final["answer"], gen["rows"], gen["sql"],
                peer_context=peer_text,
                data_notes=critical_warnings_for(routing["tables"]),
            )
            _push("faithfulness_judge", "completed", **verdict)
            _emit("faithfulness", verdict)
            if not verdict["faithful"]:
                confidence = "low"
                caveats.append(f"An automated check couldn't verify part of this answer: {verdict['reason']}")
                quality_warnings.append("answer failed automated faithfulness check")
    else:
        _push("faithfulness_judge", "skipped")

    envelope = _envelope(
        question=question,
        answer=final["answer"],
        resolution=resolution,
        confidence=confidence,
        stages=stages,
        sql=gen["sql"],
        rows=display_rows,
        tables=routing["tables"],
        # Flag verified-query path so the UI can render a "Verified" badge.
        verified_match=({"id": verified_match["id"], "score": verified_match.get("_score")} if verified_match else None),
        geography_level=dataset.geography if dataset else routing["geography_level"],
        year=routing["year_strategy"] or (dataset.default_year if dataset else None),
        focus_state=focus_state,
        metric=(routing["columns"][0] if routing["columns"] else None),
        caveats=caveats,
        key_numbers=final["key_numbers"],
        quality_warnings=quality_warnings,
        chart=visuals["chart"],
        charts=visuals["charts"],
        map_intent=visuals["map_intent"],
        intent="ANALYTICAL",
        user_id=user_id,
        request_id=request_id,
    )
    if envelope.get("resolution") == "answered":
        sf = suggest_followups(question, final["answer"], envelope.get("contract"))
        envelope["suggested_followups"] = sf
        _emit("suggested_followups", {"items": sf})
    return envelope
