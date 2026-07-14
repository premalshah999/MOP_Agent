"""Pipeline orchestrator — LLM-grounded text-to-SQL.

Stage 1 intent -> Stage 2 routing -> Stage 3 grounding -> Stage 4 SQL (with
self-repair) + grounded answer + faithfulness judge. The `answer_question()`
return contract is unchanged so the FastAPI app, threads, auth, and frontend
keep working.
"""

from __future__ import annotations

import json
from typing import Any, Callable

from app.core import meta_answer
from app.core.answer_writer import write_answer
from app.core.analysis_contract import AnalysisContract, build_analysis_contract
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
from app.core.contextualize import contextualize, prior_history
from app.core.evidence_renderer import render_verified_rows
from app.sql.semantic_validator import stabilize_verified_ranking_sql, validate_semantic_sql
from app.core.suggestions import suggest_followups
from app.core.visuals import build_visuals, enrich_rows_for_map

import os
import re as _re


from app.evals.faithfulness import judge_faithfulness
from app.observability.logging import log_pipeline_event
from app.semantic.registry import critical_warnings_for, get_dataset
from app.semantic.value_resolver import RESOLVABLE_COLUMNS, resolve_filter_value


_SQL_STR_LITERAL = _re.compile(r"'([^']{2,80})'")


def _find_value_fixes(sql: str, table: str | None) -> list[tuple[str, str, float]]:
    """Extract WHERE string literals from `sql`, fuzz-resolve each against the
    table's real values, return the suggested corrections with scores.

    Skips literals that exactly match a value in any column (the typo is
    elsewhere). Returns only high-confidence matches."""
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
                m = resolve_filter_value(table, col, lit, min_score=0.88)
            except Exception:
                continue
            if not m:
                continue
            if m[0].lower() == lit.lower():
                exact_seen = True
                break
            if best is None or m[1] > best[1]:
                best = m
        if exact_seen or best is None or best[1] < 0.9:
            continue
        fixes.append((lit, best[0], best[1]))
    return fixes


def _did_you_mean(sql: str, table: str | None) -> str:
    """Soft suggestion text for a high-confidence typo; never changes SQL."""
    fixes = _find_value_fixes(sql, table)
    if not fixes:
        return ""
    return "Did you mean: " + "; ".join(f"`{a}` → `{b}`" for a, b, _ in fixes[:3]) + "?"


PIPELINE_VERSION = "llm-grounded-v4"
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
    analysis_contract: dict[str, Any] | None = None,
    data_truncated: bool = False,
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
    analysis_contract = analysis_contract or {}
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
        "data_truncated": data_truncated,
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
            "data_truncated": data_truncated,
            "map_intent": map_intent,
            "chart_intent": {"enabled": bool(chart), "type": "vega-lite" if chart else None},
            "final_answer": {"answer": answer, "confidence": confidence},
            "analysis_contract": analysis_contract or None,
        },
        "contract": {
            "contract_type": intent,
            "family": tables[0] if tables else None,
            "metric": metric,
            "operation": analysis_contract.get("operation"),
            "unit": None,
            "geography_level": geography_level,
            "year": year,
            "focus_state": focus_state,
            "sort_direction": analysis_contract.get("sort_direction"),
            "top_k": analysis_contract.get("top_k"),
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
    routing: dict[str, Any],
    grounding: dict[str, Any],
    analysis: AnalysisContract,
    emit: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Reasoning-mode path: agent loop, then faithfulness + envelope."""
    _emit = emit or (lambda _n, _d: None)

    def _push(name: str, status: str, **data: Any) -> None:
        entry = _stage(name, status, **data)
        stages.append(entry)
        _emit("stage", entry)

    def _reasoning_sql_guard(sql_text: str) -> None:
        validate_semantic_sql(
            sql_text, q, analysis, grounding.get("resolved") or {},
            enforce_shape=False,
        )

    agent = run_reasoning_agent(
        q,
        history,
        on_event=_emit,
        semantic_guard=_reasoning_sql_guard,
        allowed_tables=set(analysis.tables),
    )
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
    routing_stub = {**routing, "tables": tables or analysis.tables}
    display_rows = enrich_rows_for_map(q, routing_stub, grounding.get("resolved") or {}, rows)
    visuals = build_visuals(q, routing_stub, grounding.get("resolved") or {}, display_rows)
    _push(
            "visual_recommender",
            "completed",
            chart=bool(visuals["chart"]),
            map=visuals["map_intent"].get("mapType") if visuals["map_intent"].get("enabled") else "none",
        )

    # Faithfulness is blocking here too.  Reasoning mode previously bypassed
    # the normal semantic contract and streamed its draft before verification.
    tool_results = agent.get("tool_results") or []
    if rows or tool_results:
        verdict = judge_faithfulness(q, answer, rows, sql or "", tool_results)
        _push("faithfulness_judge", "completed", attempt=1, **verdict)
        _emit("faithfulness", verdict)
        if not verdict["faithful"] and verdict.get("available", True) and rows:
            repaired = write_answer(
                q,
                sql or "",
                rows,
                grounding.get("text") or "",
                extra_evidence=json.dumps(tool_results, default=str)[:6000],
                previous_answer=answer,
                verification_issue=verdict["reason"],
            )
            _push("answer_repair", "completed", reason=verdict["reason"])
            repaired_verdict = (
                judge_faithfulness(q, repaired["answer"], rows, sql or "", tool_results)
                if repaired.get("valid", True)
                else {
                    "faithful": False,
                    "available": False,
                    "reason": "repaired answer failed response-schema validation",
                }
            )
            _push("faithfulness_judge", "completed", attempt=2, **repaired_verdict)
            _emit("faithfulness", repaired_verdict)
            if repaired_verdict["faithful"]:
                answer = repaired["answer"]
                key_numbers = repaired["key_numbers"]
                caveats = list(repaired["caveats"])
            verdict = repaired_verdict
        elif not verdict["faithful"]:
            retry_verdict = judge_faithfulness(q, answer, rows, sql or "", tool_results)
            _push("faithfulness_judge", "completed", attempt=2, **retry_verdict)
            _emit("faithfulness", retry_verdict)
            verdict = retry_verdict
        if verdict["faithful"]:
            confidence = "high"
        else:
            fallback = render_verified_rows(rows)
            answer = fallback["answer"]
            key_numbers = fallback["key_numbers"]
            caveats = list(fallback["caveats"])
            confidence = "high"
            quality_warnings.append("model prose replaced with verified evidence-only fallback")
            _push("evidence_fallback", "completed", reason=verdict["reason"])
    else:
        _push("faithfulness_judge", "skipped", reason="no_evidence")

    # Without rows, only call it "answered" when the agent terminated cleanly
    # (stopped_reason == "ok"); budget exhaustion / errors are surfaced honestly.
    if rows:
        resolution = "answered"
    elif agent["stopped_reason"] == "ok":
        resolution = "no_data"
    else:
        resolution = "error"

    _emit("answer_preview", {
        "answer": answer,
        "sql": sql,
        "data": display_rows,
        "row_count": len(display_rows),
        "chart": visuals["chart"],
        "charts": visuals["charts"],
        "mapIntent": visuals["map_intent"],
        "resolution": resolution,
        "key_numbers": key_numbers,
    })
    envelope = _envelope(
        question=question,
        answer=answer,
        resolution=resolution,
        confidence=confidence,
        stages=stages,
        sql=sql,
        rows=display_rows,
        tables=tables or analysis.tables,
        geography_level=dataset.geography if dataset else None,
        year=analysis.effective_period,
        focus_state=None,
        metric=(analysis.metric_columns[0] if analysis.metric_columns else None),
        caveats=caveats,
        key_numbers=key_numbers,
        quality_warnings=quality_warnings,
        chart=visuals["chart"],
        charts=visuals["charts"],
        map_intent=visuals["map_intent"],
        intent="ANALYTICAL",
        analysis_contract=analysis.model_dump(),
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
    history = prior_history(history, question)
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

    routing = {k: ir[k] for k in (
        "tables", "columns", "geography_level", "year_strategy", "join_plan",
        "needs_clarification", "clarification", "confidence", "reason",
        "operation", "flow_direction", "sort_direction", "top_k", "assumptions",
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

    analysis = build_analysis_contract(q, routing)
    analysis_data = analysis.model_dump()
    _push("analysis_contract", "completed", **analysis_data)

    grounding = build_grounding(
        q,
        routing["tables"],
        year_strategy=routing["year_strategy"],
        join_plan=routing["join_plan"],
    )
    _push("stage3_retrieval", "completed", tables=routing["tables"], resolved=grounding["resolved"])

    if mode == "reasoning":
        return _reasoning_mode(
            question=question,
            q=q,
            history=history,
            stages=stages,
            user_id=user_id,
            request_id=request_id,
            routing=routing,
            grounding=grounding,
            analysis=analysis,
            emit=_emit,
        )

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
            blessed_sql = stabilize_verified_ranking_sql(blessed_sql, analysis)
            validate_sql(blessed_sql)
            validate_semantic_sql(blessed_sql, q, analysis, grounding["resolved"])
            fetched_blessed_rows = execute_select(
                blessed_sql,
                max_rows=int(os.getenv("MAX_RETURN_ROWS", "250")) + 1,
            )
            max_rows = int(os.getenv("MAX_RETURN_ROWS", "250"))
            blessed_rows = fetched_blessed_rows[:max_rows]
            gen = {
                "sql": blessed_sql,
                "rows": blessed_rows,
                "error": None if blessed_rows else "empty_result",
                "attempts": [],
                "truncated": len(fetched_blessed_rows) > max_rows,
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
            q, grounding["text"], history, routing["tables"], exemplar=exemplar,
            contract=analysis, resolved=grounding["resolved"],
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
            analysis_contract=analysis_data,
            quality_warnings=[f"SQL generation failed: {gen['error']}"],
            user_id=user_id,
            request_id=request_id,
        )

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
            year=analysis.effective_period,
            routing_columns=routing["columns"] or [],
            rows=gen["rows"],
        )
        peer_text = render_peer_context(peer) if peer else ""
        if peer_text:
            _push("peer_context", "completed", **{k: v for k, v in (peer or {}).items() if k in ("rank", "total_states", "yoy_change_pct")})

    final = write_answer(
        q, gen["sql"], gen["rows"], grounding["text"], peer_text,
        truncated=bool(gen.get("truncated")),
    )
    _push("stage4_answer_generation", "completed", confidence=final["confidence"])
    if gen["rows"] and not final.get("valid", True):
        retry = write_answer(
            q,
            gen["sql"],
            gen["rows"],
            grounding["text"],
            peer_text,
            previous_answer=final["answer"],
            verification_issue=final.get("error") or "answer schema validation failed",
            truncated=bool(gen.get("truncated")),
        )
        _push("answer_repair", "completed", reason="answer schema validation failed")
        if retry.get("valid", True):
            final = retry

    # Never silently substitute a different entity.  A close match is offered
    # as a question, while the result remains honestly empty.
    if not gen["rows"] and routing["tables"]:
        hint = _did_you_mean(gen["sql"] or "", routing["tables"][0])
        if hint:
            final["answer"] = (final["answer"] or "").rstrip() + f"\n\n*{hint}*"

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

    caveats = list(final["caveats"])
    confidence = "low"
    quality_warnings: list[str] = []
    if gen["rows"] and final.get("valid", True):
        verdict = judge_faithfulness(
            q, final["answer"], gen["rows"], gen["sql"],
            peer_context=peer_text,
            data_notes=critical_warnings_for(routing["tables"]),
        )
        _push("faithfulness_judge", "completed", attempt=1, **verdict)
        _emit("faithfulness", verdict)

        if not verdict["faithful"] and verdict.get("available", True):
            repaired = write_answer(
                q, gen["sql"], gen["rows"], grounding["text"], peer_text,
                previous_answer=final["answer"],
                verification_issue=verdict["reason"],
                truncated=bool(gen.get("truncated")),
            )
            _push("answer_repair", "completed", reason=verdict["reason"])
            repaired_verdict = (
                judge_faithfulness(
                    q, repaired["answer"], gen["rows"], gen["sql"],
                    peer_context=peer_text,
                    data_notes=critical_warnings_for(routing["tables"]),
                )
                if repaired.get("valid", True)
                else {
                    "faithful": False,
                    "available": False,
                    "reason": "repaired answer failed response-schema validation",
                }
            )
            _push("faithfulness_judge", "completed", attempt=2, **repaired_verdict)
            _emit("faithfulness", repaired_verdict)
            if repaired_verdict["faithful"]:
                final = repaired
                caveats = list(repaired["caveats"])
            verdict = repaired_verdict
        elif not verdict["faithful"]:
            # One retry handles a transient verifier outage, but the answer is
            # never accepted merely because the safety service is unavailable.
            retry_verdict = judge_faithfulness(
                q, final["answer"], gen["rows"], gen["sql"],
                peer_context=peer_text,
                data_notes=critical_warnings_for(routing["tables"]),
            )
            _push("faithfulness_judge", "completed", attempt=2, **retry_verdict)
            _emit("faithfulness", retry_verdict)
            verdict = retry_verdict

        if verdict["faithful"]:
            confidence = "high" if routing.get("confidence") == "high" else "medium"
        else:
            final = render_verified_rows(
                gen["rows"], truncated=bool(gen.get("truncated"))
            )
            caveats = list(final["caveats"])
            confidence = "high"
            quality_warnings.append("model prose replaced with verified evidence-only fallback")
            _push("evidence_fallback", "completed", reason=verdict["reason"])
    elif gen["rows"]:
        _push("faithfulness_judge", "skipped", reason="answer_schema_invalid")
        final = render_verified_rows(
            gen["rows"], truncated=bool(gen.get("truncated"))
        )
        caveats = list(final["caveats"])
        confidence = "high"
        quality_warnings.append("invalid model response replaced with verified evidence-only fallback")
        _push("evidence_fallback", "completed", reason="answer_schema_invalid")
    else:
        _push("faithfulness_judge", "skipped")
        confidence = "low"

    if gen.get("truncated"):
        caveats.append(
            f"The displayed data is capped at {len(gen['rows'])} rows; it is not the complete result set."
        )
        quality_warnings.append("result set truncated at MAX_RETURN_ROWS")

    # The UI sees prose only after the blocking verification gate.  Previously
    # answer_preview exposed a known-bad draft and the later warning could not
    # retract it.
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
        year=analysis.effective_period,
        focus_state=focus_state,
        metric=(routing["columns"][0] if routing["columns"] else None),
        caveats=caveats,
        key_numbers=final["key_numbers"],
        quality_warnings=quality_warnings,
        chart=visuals["chart"],
        charts=visuals["charts"],
        map_intent=visuals["map_intent"],
        intent="ANALYTICAL",
        analysis_contract=analysis_data,
        data_truncated=bool(gen.get("truncated")),
        user_id=user_id,
        request_id=request_id,
    )
    if envelope.get("resolution") == "answered":
        sf = suggest_followups(question, final["answer"], envelope.get("contract"))
        envelope["suggested_followups"] = sf
        _emit("suggested_followups", {"items": sf})
    return envelope
