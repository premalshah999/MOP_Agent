"""Pipeline orchestrator — LLM-grounded text-to-SQL.

Stage 1 intent -> Stage 2 routing -> Stage 3 grounding -> Stage 4 SQL (with
self-repair) + grounded answer + faithfulness judge. The `answer_question()`
return contract is unchanged so the FastAPI app, threads, auth, and frontend
keep working.
"""

from __future__ import annotations

from typing import Any

from app.core import meta_answer
from app.core.answer_writer import write_answer
from app.core.grounding import build_grounding
from app.core.intent import classify_intent
from app.core.router import route as route_question
from app.core.sql_writer import generate_and_execute
from app.core.visuals import build_visuals, enrich_rows_for_map
from app.evals.faithfulness import judge_faithfulness
from app.observability.logging import log_pipeline_event
from app.semantic.registry import get_dataset

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
) -> dict[str, Any]:
    rows = rows or []
    tables = tables or []
    assumptions = assumptions or []
    caveats = caveats or []
    key_numbers = key_numbers or []
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
    }


def answer_question(
    question: str,
    history: list[dict[str, Any]] | None = None,
    *,
    user_id: int | str | None = None,
    request_id: str | None = None,
) -> dict[str, Any]:
    history = history or []
    stages: list[dict[str, Any]] = []

    intent = classify_intent(question, history)
    stages.append(_stage("stage1_intent", "completed", **intent))

    if intent["intent"] != "ANALYTICAL":
        meta = meta_answer.respond(question, intent["intent"], intent)
        stages.append(_stage("non_analytical_responder", "completed", intent=intent["intent"]))
        return _envelope(
            question=question,
            answer=meta["answer"],
            resolution=meta["resolution"],
            confidence=meta["confidence"],
            stages=stages,
            intent=intent["intent"],
            user_id=user_id,
            request_id=request_id,
        )

    routing = route_question(question, history)
    stages.append(_stage("stage2_routing", "completed", **routing))
    if not routing["tables"] or routing["needs_clarification"]:
        ask = routing["clarification"] or "Which dataset and measure should I use?"
        return _envelope(
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

    grounding = build_grounding(
        question,
        routing["tables"],
        year_strategy=routing["year_strategy"],
        join_plan=routing["join_plan"],
    )
    stages.append(_stage("stage3_retrieval", "completed", tables=routing["tables"], resolved=grounding["resolved"]))

    gen = generate_and_execute(question, grounding["text"], history)
    stages.append(
        _stage(
            "stage4_sql_generation",
            "completed" if gen["sql"] else "failed",
            attempts=len(gen["attempts"]),
            error=gen["error"],
            row_count=len(gen["rows"]),
        )
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

    final = write_answer(question, gen["sql"], gen["rows"], grounding["text"])
    stages.append(_stage("stage4_answer_generation", "completed", confidence=final["confidence"]))

    caveats = list(final["caveats"])
    confidence = final["confidence"]
    quality_warnings: list[str] = []
    if gen["rows"]:
        verdict = judge_faithfulness(question, final["answer"], gen["rows"], gen["sql"])
        stages.append(_stage("faithfulness_judge", "completed", **verdict))
        if not verdict["faithful"]:
            confidence = "low"
            caveats.append(f"Faithfulness check flagged this answer: {verdict['reason']}")
            quality_warnings.append("answer failed automated faithfulness check")
    else:
        stages.append(_stage("faithfulness_judge", "skipped"))

    resolution = "answered" if gen["rows"] else "no_data"
    primary_table = routing["tables"][0]
    dataset = get_dataset(primary_table)
    resolved = grounding["resolved"].get(primary_table, {})
    focus_state = resolved.get("state", {}).get("value") if isinstance(resolved.get("state"), dict) else None

    display_rows = enrich_rows_for_map(question, routing, grounding["resolved"], gen["rows"])
    visuals = build_visuals(question, routing, grounding["resolved"], display_rows)
    stages.append(
        _stage(
            "visual_recommender",
            "completed",
            chart=bool(visuals["chart"]),
            map=visuals["map_intent"].get("mapType") if visuals["map_intent"].get("enabled") else "none",
        )
    )

    return _envelope(
        question=question,
        answer=final["answer"],
        resolution=resolution,
        confidence=confidence,
        stages=stages,
        sql=gen["sql"],
        rows=display_rows,
        tables=routing["tables"],
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
