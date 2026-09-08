"""Coordinate the production question-answering workflow.

Conversation context, semantic planning, grounding, SQL generation, evidence
validation, response writing, and visualization all pass through
``answer_question``. The API contract remains stable while these internal
stages evolve.
"""

from __future__ import annotations

import hashlib
import json
import re as _re
from typing import Any, Callable

from app.core import meta_answer
from app.core.analysis_plan import AnalysisContract, build_analysis_contract
from app.core.clarifier import generate_clarification
from app.core.conversation import contextualize, prior_history
from app.core.evidence_renderer import render_validated_rows
from app.core.formatting import (
    format_key_numbers,
    validate_key_numbers_against_row_sets,
)
from app.core.glossary import detect_terms
from app.core.grounding import build_grounding
from app.core.peer_context import compute_peer_context, render_peer_context
from app.core.period_guard import (
    canonical_period_notes,
    mixed_period_note,
    period_claim_issues,
)
from app.core.planner import classify_and_route
from app.core.query_engine import generate_and_execute
from app.core.reasoning import run_reasoning_agent
from app.core.response_writer import write_answer
from app.core.suggestions import suggest_followups
from app.core.visuals import build_visuals, enrich_rows_for_map
from app.llm import client as llm_client
from app.observability.logging import log_pipeline_event
from app.quality.faithfulness import judge_faithfulness
from app.semantic.registry import critical_warnings_for, get_dataset, load_registry
from app.semantic.value_resolver import RESOLVABLE_COLUMNS, resolve_filter_value
from app.sql.semantic_validator import validate_semantic_sql

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


PIPELINE_VERSION = "llm-grounded-v5"
PIPELINE_READY = True


def _contract_fingerprint(value: Any) -> str:
    blob = json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _empty_map_intent() -> dict[str, Any]:
    return {"enabled": False, "mapType": "none"}


def _stage(name: str, status: str, **data: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {"name": name, "status": status}
    if data:
        entry["data"] = data
    return entry


def _build_context_memory(
    question: str,
    analysis: AnalysisContract,
    resolved: dict[str, Any],
) -> dict[str, Any]:
    """Persist the full analytical intent needed by later follow-ups.

    Values come from the typed analysis contract and deterministic entity
    resolver, never from generated answer prose.
    """
    filters: list[dict[str, Any]] = []
    entities: list[str] = []
    state_entities: list[str] = []
    for table, columns in resolved.items():
        if not isinstance(columns, dict):
            continue
        for column, info in columns.items():
            if not isinstance(info, dict):
                continue
            raw_values = info.get("values")
            values = (
                list(raw_values) if isinstance(raw_values, (list, tuple)) else [info.get("value")]
            )
            clean_values = [str(value) for value in values if value not in (None, "")]
            if not clean_values:
                continue
            filters.append({"table": table, "column": column, "values": clean_values})
            for value in clean_values:
                if value not in entities:
                    entities.append(value)
                if "state" in str(column).casefold() and value not in state_entities:
                    state_entities.append(value)
    memory: dict[str, Any] = {
        "standalone_question": question,
        "tables": list(analysis.tables),
        "metrics": list(analysis.metric_columns),
        "geography_level": analysis.geography_level,
        "operation": analysis.operation,
        "flow_direction": analysis.flow_direction,
        "period": analysis.effective_period,
        "requested_period": analysis.requested_period,
        "requested_years": list(analysis.requested_years),
        "sort_direction": analysis.sort_direction,
        "sort_columns": list(analysis.sort_columns),
        "top_k": analysis.top_k,
        "statistic": analysis.statistic,
        "result_unit": analysis.result_unit,
        "formula": analysis.formula.model_dump(),
        "predicate": analysis.predicate.model_dump(),
        "observation_grain": analysis.observation_grain,
        "result_scope": analysis.result_scope,
        "include_component_measures": analysis.include_component_measures,
        "output_dimensions": list(analysis.output_dimensions),
        "filters": filters,
        "entities": entities,
        "comparison_entities": state_entities if len(state_entities) > 1 else [],
        "focus_state": state_entities[0] if len(state_entities) == 1 else None,
    }
    return {key: value for key, value in memory.items() if value not in (None, "", [], {})}


def _add_result_context(
    memory: dict[str, Any],
    rows: list[dict[str, Any]],
    tables: list[str],
) -> dict[str, Any]:
    """Persist compact, structured result identities for later follow-ups.

    Requests such as "show the states for the counties above" cannot be
    resolved from the analytical contract alone.  Storing generated prose is
    unsafe, but the executed row labels are verified evidence.  Keep only
    dimension-like values from the first 25 displayed rows, never free-form
    answer text or unbounded result data.
    """
    output = dict(memory)
    dimension_keys: set[str] = {
        "state",
        "county",
        "fips",
        "cd_118",
        "agency",
        "agency_name",
        "rcpt_state_name",
        "subawardee_state_name",
        "rcpt_state",
        "subawardee_state",
        "rcpt_cty_name",
        "subawardee_cty_name",
        "rcpt_full_name",
        "subawardee_full_name",
        "rcpt_cd_name",
        "subawardee_cd_name",
        "naics_2digit_title",
        "comparison",
        "metric",
    }
    for table in tables:
        dataset = get_dataset(table)
        if dataset is not None:
            dimension_keys.update(dataset.dimensions)
    result_entities: list[dict[str, Any]] = []
    for row in rows[:25]:
        item = {
            key: value
            for key, value in row.items()
            if key in dimension_keys and value not in (None, "")
        }
        if item and item not in result_entities:
            result_entities.append(item)
    if result_entities:
        output["result_entities"] = result_entities
    output["result_row_count"] = len(rows)
    return output


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
    analysis_contract: dict[str, Any] | None = None,
    data_truncated: bool = False,
    key_number_row_sets: list[list[dict[str, Any]]] | None = None,
    context_memory: dict[str, Any] | None = None,
    peer_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = rows or []
    tables = tables or []
    assumptions = list(assumptions or [])
    caveats = list(caveats or [])
    # Validate every LLM-emitted key_number against the actual rows BEFORE
    # formatting. Drops fabricated values (LLM arithmetic miss) and
    # downgrades confidence if anything was dropped — prevents the most
    # visible hallucination class: a wrong number in the headline callout.
    raw_kn = list(key_numbers or [])
    validation_sets = key_number_row_sets if key_number_row_sets is not None else [rows]
    if raw_kn and any(validation_sets):
        kept_kn, dropped_kn = validate_key_numbers_against_row_sets(raw_kn, validation_sets)
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
    context_memory = context_memory or {}
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
            "pipeline_version": PIPELINE_VERSION,
            "semantic_registry_version": load_registry().version,
            "provider": llm_client.active_provider(),
            "provider_warnings": llm_client.provider_warnings(),
            "semantic_contract_fingerprint": _contract_fingerprint(analysis_contract),
            "sql_fingerprint": _contract_fingerprint(sql or ""),
            "evidence_fingerprint": _contract_fingerprint(rows),
            "route_verification_changed": any(
                stage.get("name") == "route_verification"
                and bool((stage.get("data") or {}).get("changed"))
                for stage in stages
            ),
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
            "context_memory": context_memory or None,
            "statistics": {"peer_context": peer_context} if peer_context else {},
        },
        "contract": {
            "contract_type": intent,
            "resolution": resolution,
            "family": tables[0] if tables else None,
            "metric": metric,
            "operation": analysis_contract.get("operation"),
            "unit": analysis_contract.get("result_unit"),
            "geography_level": geography_level,
            "year": year,
            "focus_state": focus_state,
            "flow_direction": analysis_contract.get("flow_direction"),
            "sort_direction": analysis_contract.get("sort_direction"),
            "top_k": analysis_contract.get("top_k"),
            "tables": tables,
            "supported": supported,
            "missing_slots": [],
            "assumptions": assumptions,
            "validation_message": quality_warnings[0] if quality_warnings else None,
            "context_memory": context_memory or None,
        },
        "pipelineTrace": {
            "version": PIPELINE_VERSION,
            "semantic_registry_version": load_registry().version,
            "provider": llm_client.active_provider(),
            "stages": stages,
        },
        "quality": {
            "status": "warning" if quality_warnings else "ok",
            "warnings": quality_warnings,
        },
        "confidence": confidence,
        "key_numbers": key_numbers,
        "assumptions": assumptions,
        "caveats": caveats,
        # Only the terms actually present in the answer prose or caveats;
        # the frontend wraps matches with <abbr title="..."> tooltips.
        "glossary": detect_terms((answer or "") + "\n" + "\n".join(caveats)),
    }


_MART_RE = _re.compile(r"\bmart_([a-z_]+)", _re.IGNORECASE)


def _reasoning_evidence_row_sets(
    display_rows: list[dict[str, Any]],
    tool_results: list[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    """Return the independent numeric evidence sets collected by the agent."""
    row_sets: list[list[dict[str, Any]]] = [display_rows] if display_rows else []
    for tool_result in tool_results:
        result = tool_result.get("result") or {}
        if result.get("error"):
            continue
        if tool_result.get("name") == "run_sql":
            result_rows = [row for row in (result.get("rows") or []) if isinstance(row, dict)]
            if result_rows:
                row_sets.append(result_rows)
        elif tool_result.get("name") == "peer_stats":
            stats = result.get("stats")
            if isinstance(stats, dict) and stats:
                row_sets.append([stats])
            for key in ("top5", "bottom5"):
                result_rows = [row for row in (result.get(key) or []) if isinstance(row, dict)]
                if result_rows:
                    row_sets.append(result_rows)
    return row_sets


def _cited_reasoning_tool_results(agent: dict[str, Any]) -> list[dict[str, Any]]:
    """Return only evidence the final reasoning answer explicitly cited.

    Forced synthesis paths predate explicit citations, so they retain the full
    trail. A normal successful tool answer always supplies a primary evidence
    id and is verified against exactly that result plus named supporting
    results, never unrelated exploration.
    """

    tool_results = list(agent.get("tool_results") or [])
    cited_ids = {
        value
        for value in [
            agent.get("primary_evidence_id"),
            *(agent.get("supporting_evidence_ids") or []),
        ]
        if value
    }
    if not cited_ids:
        return tool_results
    return [result for result in tool_results if result.get("evidence_id") in cited_ids]


def _reasoning_requires_final_shape(analysis: AnalysisContract) -> bool:
    """Identify contracts whose evidence is one atomic statistical result.

    A reasoning agent may explore with partial queries for broad comparisons,
    but a single derived value or correlation must keep its answer, displayed
    SQL, and displayed rows aligned. Requiring the final shape for those typed
    contracts prevents a prose answer assembled from two hidden one-row calls.
    """

    return analysis.result_scope == "single" and (
        analysis.statistic in {"derived", "correlation"}
        or analysis.formula.operator not in {"none", "identity"}
    )


def _reasoning_focus_values(resolved: dict[str, Any]) -> dict[str, list[str]]:
    """Collect canonical named entities for peer tools without guessing scope."""

    output: dict[str, list[str]] = {}
    for table_entities in resolved.values():
        if not isinstance(table_entities, dict):
            continue
        for column, info in table_entities.items():
            if not isinstance(info, dict):
                continue
            values = info.get("values") or [info.get("value")]
            bucket = output.setdefault(str(column), [])
            for value in values:
                text = str(value or "").strip()
                if text and text not in bucket:
                    bucket.append(text)
    return output


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
            sql_text,
            q,
            analysis,
            grounding.get("resolved") or {},
            enforce_shape=_reasoning_requires_final_shape(analysis),
        )

    agent = run_reasoning_agent(
        q,
        history,
        on_event=_emit,
        semantic_guard=_reasoning_sql_guard,
        allowed_tables=set(analysis.tables),
        operation=analysis.operation,
        flow_direction=analysis.flow_direction,
        analysis_contract=analysis.model_dump(),
        focus_values_by_column=_reasoning_focus_values(grounding.get("resolved") or {}),
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
    data_truncated = bool(agent.get("truncated"))
    answer = agent["answer"] or "I could not answer that."
    caveats = list(agent["caveats"])
    key_numbers = list(agent["key_numbers"])
    quality_warnings: list[str] = []
    confidence = "high" if agent["stopped_reason"] == "ok" else "low"

    cited_tool_results = _cited_reasoning_tool_results(agent)

    # Derive tables from cited evidence, not abandoned exploratory queries.
    tables: list[str] = []
    cited_sql = [
        str((item.get("result") or {}).get("sql") or "")
        for item in cited_tool_results
        if item.get("name") in {"run_sql", "peer_stats"}
    ]
    for s in cited_sql or ([sql] if sql else []):
        for t in _MART_RE.findall(s):
            t_low = t.lower()
            if t_low and t_low not in tables:
                tables.append(t_low)
    routing_stub = {
        **routing,
        "tables": tables or analysis.tables,
        "operation": analysis.operation,
        "result_unit": analysis.result_unit,
        "formula": analysis.formula.model_dump(),
        "effective_period": analysis.effective_period,
        "requested_period": analysis.requested_period,
        "requested_years": list(analysis.requested_years),
        "data_truncated": data_truncated,
    }

    # Faithfulness is blocking here too.  Reasoning mode previously bypassed
    # the normal semantic contract and streamed its draft before verification.
    data_notes = critical_warnings_for(tables or analysis.tables)
    if rows or cited_tool_results:
        verdict = judge_faithfulness(
            q,
            answer,
            rows,
            sql or "",
            cited_tool_results,
            data_notes=data_notes,
        )
        _push("faithfulness_judge", "completed", attempt=1, **verdict)
        _emit("faithfulness", verdict)
        if not verdict["faithful"] and verdict.get("available", True) and rows:
            repaired = write_answer(
                q,
                sql or "",
                rows,
                grounding.get("text") or "",
                extra_evidence=agent.get("cited_evidence_digest") or "",
                previous_answer=answer,
                verification_issue=verdict["reason"],
                truncated=data_truncated,
            )
            _push("answer_repair", "completed", reason=verdict["reason"])
            repaired_verdict = (
                judge_faithfulness(
                    q,
                    repaired["answer"],
                    rows,
                    sql or "",
                    cited_tool_results,
                    data_notes=data_notes,
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
                answer = repaired["answer"]
                key_numbers = repaired["key_numbers"]
                caveats = list(repaired["caveats"])
            verdict = repaired_verdict
        elif not verdict["faithful"]:
            retry_verdict = judge_faithfulness(
                q,
                answer,
                rows,
                sql or "",
                cited_tool_results,
                data_notes=data_notes,
            )
            _push("faithfulness_judge", "completed", attempt=2, **retry_verdict)
            _emit("faithfulness", retry_verdict)
            verdict = retry_verdict
        if verdict["faithful"]:
            confidence = "high"
        elif rows:
            fallback = render_validated_rows(rows, truncated=data_truncated)
            answer = fallback["answer"]
            key_numbers = fallback["key_numbers"]
            caveats = list(fallback["caveats"])
            confidence = "high"
            quality_warnings.append("model prose replaced with validated evidence-only fallback")
            _push("evidence_fallback", "completed", reason=verdict["reason"])
        else:
            confidence = "low"
            quality_warnings.append("analysis ended without row-producing evidence")
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

    if data_truncated:
        quality_warnings.append("result set truncated at MAX_RETURN_ROWS")

    # Visuals are a view of the final cited evidence. Build them only after
    # answer verification/repair so prose, SQL, rows, and chart cannot describe
    # different stages of the investigation.
    display_rows = enrich_rows_for_map(q, routing_stub, grounding.get("resolved") or {}, rows)
    visuals = build_visuals(q, routing_stub, grounding.get("resolved") or {}, display_rows)
    _push(
        "visual_recommender",
        "completed",
        chart=bool(visuals["chart"]),
        map=(
            visuals["map_intent"].get("mapType") if visuals["map_intent"].get("enabled") else "none"
        ),
        evidence_id=agent.get("primary_evidence_id"),
    )

    _emit(
        "answer_preview",
        {
            "answer": answer,
            "sql": sql,
            "data": display_rows,
            "row_count": len(display_rows),
            "data_truncated": data_truncated,
            "chart": visuals["chart"],
            "charts": visuals["charts"],
            "mapIntent": visuals["map_intent"],
            "resolution": resolution,
            "key_numbers": key_numbers,
        },
    )
    envelope = _envelope(
        question=question,
        answer=answer,
        resolution=resolution,
        confidence=confidence,
        stages=stages,
        sql=sql,
        rows=display_rows,
        tables=tables or analysis.tables,
        geography_level=analysis.geography_level,
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
        data_truncated=data_truncated,
        key_number_row_sets=_reasoning_evidence_row_sets(display_rows, cited_tool_results),
        user_id=user_id,
        request_id=request_id,
        context_memory=_build_context_memory(q, analysis, grounding.get("resolved") or {}),
    )
    # Surface the full agent tool trail so the faithfulness judge and the
    # reasoning evaluator can see peer_stats / multi-SQL evidence, not just
    # the most recent run_sql rows.
    envelope["resultPackage"]["tool_results"] = agent.get("tool_results", [])
    envelope["resultPackage"]["sql_history"] = agent.get("sql_history", [])
    envelope["resultPackage"]["primary_evidence_id"] = agent.get("primary_evidence_id")
    envelope["resultPackage"]["supporting_evidence_ids"] = agent.get("supporting_evidence_ids", [])
    envelope["resultPackage"]["cited_tool_results"] = cited_tool_results
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
    intent = {
        k: ir[k]
        for k in (
            "intent",
            "requires_sql",
            "needs_clarification",
            "clarification_question",
            "reason",
        )
    }
    _push("stage1_intent", "completed", **intent)
    if ir.get("route_verification"):
        verification = dict(ir["route_verification"])
        _push(
            "route_verification",
            "completed" if verification.get("available", True) else "unavailable",
            **verification,
        )

    if ir.get("service_unavailable"):
        _push("analysis_service", "unavailable")
        return _envelope(
            question=question,
            answer=(
                "The analysis service is temporarily unavailable. Your question "
                "was not rejected or reinterpreted; please try it again shortly."
            ),
            resolution="error",
            confidence="low",
            stages=stages,
            intent="ERROR",
            quality_warnings=["intent and routing model unavailable"],
            user_id=user_id,
            request_id=request_id,
        )

    if intent["intent"] != "ANALYTICAL":
        meta = meta_answer.respond(q, intent["intent"], ir)
        _push("non_analytical_responder", "completed", intent=intent["intent"])
        env = _envelope(
            question=question,
            answer=meta["answer"],
            resolution=meta["resolution"],
            confidence=meta["confidence"],
            stages=stages,
            intent=intent["intent"],
            context_memory=meta.get("context_memory"),
            user_id=user_id,
            request_id=request_id,
        )
        clarification_chips = list(meta.get("suggestions") or [])
        if clarification_chips:
            env["suggested_followups"] = clarification_chips
        return env

    routing = {
        k: ir[k]
        for k in (
            "tables",
            "columns",
            "geography_level",
            "year_strategy",
            "join_plan",
            "needs_clarification",
            "clarification",
            "confidence",
            "reason",
            "operation",
            "flow_direction",
            "sort_direction",
            "top_k",
            "assumptions",
        )
    }
    # Compatibility for older in-process callers and recorded fixtures. The
    # production planner always supplies this typed list.
    routing["filter_columns"] = list(ir.get("filter_columns") or [])
    # Compatibility for tests/older integrations; production planner responses
    # always include the typed semantic plan.
    routing["semantic_plan"] = ir.get("semantic_plan")
    _push("stage2_routing", "completed", **routing)
    if not routing["tables"] or routing["needs_clarification"]:
        ask = routing["clarification"] or "Which dataset and measure should I use?"
        try:
            guidance = generate_clarification(q, ask)
        except Exception:
            guidance = {
                "answer": ask,
                "suggestions": [],
                "context_memory": {},
            }
        env = _envelope(
            question=question,
            answer=str(guidance.get("answer") or ask),
            resolution="needs_clarification",
            confidence="medium",
            stages=stages,
            tables=routing["tables"],
            geography_level=routing["geography_level"],
            intent="ANALYTICAL",
            context_memory=guidance.get("context_memory"),
            user_id=user_id,
            request_id=request_id,
        )
        clarification_chips = list(guidance.get("suggestions") or [])
        if clarification_chips:
            env["suggested_followups"] = clarification_chips
        return env

    analysis = build_analysis_contract(q, routing)
    # Keep execution, validation, visuals, and the response envelope on the
    # same normalized flow semantics selected by the typed contract.
    routing.update(
        {
            "columns": list(analysis.metric_columns) or list(routing.get("columns") or []),
            "geography_level": analysis.geography_level,
            "operation": analysis.operation,
            "flow_direction": analysis.flow_direction,
            "sort_direction": analysis.sort_direction,
            "top_k": analysis.top_k,
            "result_unit": analysis.result_unit,
            "formula": analysis.formula.model_dump(),
            "effective_period": analysis.effective_period,
            "requested_period": analysis.requested_period,
            "requested_years": list(analysis.requested_years),
        }
    )
    analysis_data = analysis.model_dump()
    _push("analysis_contract", "completed", **analysis_data)

    grounding = build_grounding(
        q,
        analysis.tables,
        year_strategy=routing["year_strategy"],
        join_plan=routing["join_plan"],
        filter_columns=routing["filter_columns"],
        output_dimensions=analysis.output_dimensions,
        flow_direction=analysis.flow_direction,
    )
    _push("stage3_retrieval", "completed", tables=analysis.tables, resolved=grounding["resolved"])
    context_memory = _build_context_memory(q, analysis, grounding["resolved"])

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

    gen = generate_and_execute(
        q,
        grounding["text"],
        history,
        analysis.tables,
        contract=analysis,
        resolved=grounding["resolved"],
    )
    _push(
        "stage4_sql_generation",
        "completed" if gen["sql"] else "failed",
        attempts=len(gen["attempts"]),
        error=gen["error"],
        row_count=len(gen["rows"]),
    )

    if not gen["sql"] or (gen["error"] and gen["error"] != "empty_result" and not gen["rows"]):
        provider_unavailable = str(gen.get("error") or "").startswith("LLM error:")
        return _envelope(
            question=question,
            answer=(
                "The analysis service became temporarily unavailable before it could finish. Please retry."
                if provider_unavailable
                else "I could not produce a valid query for that. Try rephrasing, or specify the measure, geography level, and time period."
            ),
            resolution="error",
            confidence="low",
            stages=stages,
            sql=gen["sql"] or None,
            tables=routing["tables"],
            geography_level=routing["geography_level"],
            intent="ANALYTICAL",
            analysis_contract=analysis_data,
            context_memory=context_memory,
            quality_warnings=[f"SQL generation failed: {gen['error']}"],
            user_id=user_id,
            request_id=request_id,
        )

    # Peer / comparative context: cheap side-queries to give a single-state
    # answer real meaning (rank, vs peer-geography median, YoY). Never load-bearing
    # — failures silently produce None. Passed as a dedicated prompt section.
    peer: dict[str, Any] | None = None
    peer_text = ""
    if gen["rows"] and routing["tables"]:
        resolved0 = grounding["resolved"].get(routing["tables"][0], {})
        fstate_val = (
            resolved0.get("state", {}).get("value")
            if isinstance(resolved0.get("state"), dict)
            else None
        )
        peer = compute_peer_context(
            table=routing["tables"][0],
            focus_state=fstate_val,
            year=analysis.effective_period,
            routing_columns=routing["columns"] or [],
            rows=gen["rows"],
        )
        peer_text = render_peer_context(peer) if peer else ""
        if peer_text:
            _push(
                "peer_context",
                "completed",
                **{
                    k: v
                    for k, v in (peer or {}).items()
                    if k in ("rank", "total_states", "yoy_change_pct")
                },
            )

    final = write_answer(
        q,
        gen["sql"],
        gen["rows"],
        grounding["text"],
        peer_text,
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
    focus_state = context_memory.get("focus_state")

    routing["data_truncated"] = bool(gen.get("truncated"))
    display_rows = enrich_rows_for_map(q, routing, grounding["resolved"], gen["rows"])
    context_memory = _add_result_context(context_memory, display_rows, routing["tables"])
    visuals = build_visuals(q, routing, grounding["resolved"], display_rows)
    _push(
        "visual_recommender",
        "completed",
        chart=bool(visuals["chart"]),
        map=visuals["map_intent"].get("mapType")
        if visuals["map_intent"].get("enabled")
        else "none",
    )

    caveats = list(final["caveats"])
    confidence = "low"
    quality_warnings: list[str] = []
    if gen["rows"] and final.get("valid", True):
        verdict = judge_faithfulness(
            q,
            final["answer"],
            gen["rows"],
            gen["sql"],
            peer_context=peer_text,
            data_notes=critical_warnings_for(routing["tables"]),
        )
        _push("faithfulness_judge", "completed", attempt=1, **verdict)
        _emit("faithfulness", verdict)

        if not verdict["faithful"] and verdict.get("available", True):
            repaired = write_answer(
                q,
                gen["sql"],
                gen["rows"],
                grounding["text"],
                peer_text,
                previous_answer=final["answer"],
                verification_issue=verdict["reason"],
                truncated=bool(gen.get("truncated")),
            )
            _push("answer_repair", "completed", reason=verdict["reason"])
            repaired_verdict = (
                judge_faithfulness(
                    q,
                    repaired["answer"],
                    gen["rows"],
                    gen["sql"],
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
                q,
                final["answer"],
                gen["rows"],
                gen["sql"],
                peer_context=peer_text,
                data_notes=critical_warnings_for(routing["tables"]),
            )
            _push("faithfulness_judge", "completed", attempt=2, **retry_verdict)
            _emit("faithfulness", retry_verdict)
            verdict = retry_verdict

        if verdict["faithful"]:
            confidence = "high" if routing.get("confidence") == "high" else "medium"
        else:
            final = render_validated_rows(gen["rows"], truncated=bool(gen.get("truncated")))
            caveats = list(final["caveats"])
            confidence = "high"
            quality_warnings.append("model prose replaced with validated evidence-only fallback")
            _push("evidence_fallback", "completed", reason=verdict["reason"])
    elif gen["rows"]:
        _push("faithfulness_judge", "skipped", reason="answer_schema_invalid")
        final = render_validated_rows(gen["rows"], truncated=bool(gen.get("truncated")))
        caveats = list(final["caveats"])
        confidence = "high"
        quality_warnings.append(
            "invalid model response replaced with validated evidence-only fallback"
        )
        _push("evidence_fallback", "completed", reason="answer_schema_invalid")
    else:
        _push("faithfulness_judge", "skipped")
        confidence = "low"

    period_issues = period_claim_issues(
        final.get("answer", ""),
        list(final.get("caveats") or []),
        routing["tables"],
        routing["columns"],
    )
    if period_issues:
        final = render_validated_rows(gen["rows"], truncated=bool(gen.get("truncated")))
        caveats = list(final["caveats"]) + canonical_period_notes(routing["tables"])
        confidence = "high"
        quality_warnings.extend(f"period_claim_guard: {issue}" for issue in period_issues)
        _push("period_claim_guard", "replaced", issues=period_issues)

    period_note = mixed_period_note(routing["tables"], analysis.effective_period)
    if period_note:
        final["answer"] = (final.get("answer") or "").rstrip() + f"\n\n*Period note: {period_note}*"
        if period_note not in caveats:
            caveats.append(period_note)

    if gen.get("truncated"):
        caveats.append(
            f"The displayed data is capped at {len(gen['rows'])} rows; it is not the complete result set."
        )
        quality_warnings.append("result set truncated at MAX_RETURN_ROWS")

    # The UI sees prose only after the blocking verification gate.  Previously
    # answer_preview exposed a known-bad draft and the later warning could not
    # retract it.
    _emit(
        "answer_preview",
        {
            "answer": final["answer"],
            "sql": gen["sql"],
            "data": display_rows,
            "row_count": len(display_rows),
            "chart": visuals["chart"],
            "charts": visuals["charts"],
            "mapIntent": visuals["map_intent"],
            "resolution": resolution,
            "key_numbers": final["key_numbers"],
        },
    )

    envelope = _envelope(
        question=question,
        answer=final["answer"],
        resolution=resolution,
        confidence=confidence,
        stages=stages,
        sql=gen["sql"],
        rows=display_rows,
        tables=routing["tables"],
        geography_level=analysis.geography_level,
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
        context_memory=context_memory,
        peer_context=peer,
    )
    if envelope.get("resolution") == "answered":
        sf = suggest_followups(question, final["answer"], envelope.get("contract"))
        envelope["suggested_followups"] = sf
        _emit("suggested_followups", {"items": sf})
    return envelope
