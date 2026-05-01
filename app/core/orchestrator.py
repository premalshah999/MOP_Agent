from __future__ import annotations

import os
from typing import Any

from app.core.answer_generator import generate_answer
from app.core.conversation import build_conversation_state
from app.core.intent_classifier import classify_intent
from app.core.metadata_answerer import assistant_help_answer, dataset_discovery_answer, metric_definition_answer, out_of_scope_answer
from app.core.query_planner import create_query_plan
from app.core.result_verifier import verify_results
from app.core.router import route_message
from app.core.visuals import build_chart, build_map_intent
from app.observability.logging import log_pipeline_event
from app.schemas.query_plan import QueryPlan
from app.semantic.retriever import retrieve_semantic_context
from app.semantic.registry import get_dataset
from app.semantic.validators import validate_query_plan
from app.sql.executor import execute_sql_bundle
from app.sql.generator import generate_sql
from app.sql.validator import validate_sql


PIPELINE_VERSION = "controlled-analytics-v2"


def _stage(name: str, status: str, **data: Any) -> dict[str, Any]:
    payload = {"name": name, "status": status}
    if data:
        payload["data"] = data
    return payload


def _empty_map_intent() -> dict[str, Any]:
    return {"enabled": False, "mapType": "none"}


def _response_from_non_sql(
    question: str,
    final,
    route: dict[str, Any],
    stages: list[dict[str, Any]],
    user_id: int | str | None,
    request_id: str | None,
    *,
    resolution: str = "answered",
) -> dict[str, Any]:
    stages.append(_stage("workflow_selector", "completed", workflow=route.get("mode")))
    stages.append(_stage("response_composer", "completed", confidence=final.confidence))
    result_package = {
        "status": resolution,
        "contract_type": route.get("mode"),
        "assumptions": final.assumptions,
        "sql": None,
        "rows": [],
        "map_intent": _empty_map_intent(),
        "chart_intent": {"enabled": False, "type": None},
        "final_answer": final.model_dump(),
        "router": route,
    }
    log_pipeline_event(
        {
            "request_id": request_id,
            "user_id": user_id,
            "question": question,
            "intent": route.get("mode"),
            "resolution": resolution,
            "datasets": [],
            "metrics": [],
            "query_count": 0,
            "row_count": 0,
            "confidence": final.confidence,
            "quality_status": "ok",
            "warnings": [],
        }
    )
    return {
        "answer": final.answer,
        "sql": None,
        "data": [],
        "row_count": 0,
        "resolution": resolution,
        "mapIntent": _empty_map_intent(),
        "chart": None,
        "resultPackage": result_package,
        "contract": {
            "contract_type": route.get("mode"),
            "family": None,
            "metric": None,
            "operation": None,
            "unit": None,
            "geography_level": None,
            "year": None,
            "focus_state": None,
            "sort_direction": None,
            "top_k": None,
            "tables": [],
            "supported": resolution == "answered",
            "missing_slots": [],
            "assumptions": final.assumptions,
            "validation_message": None,
        },
        "pipelineTrace": {"version": PIPELINE_VERSION, "stages": stages},
        "quality": {"status": "ok", "warnings": []},
        "confidence": final.confidence,
        "key_numbers": [item.model_dump() for item in final.key_numbers],
        "assumptions": final.assumptions,
        "caveats": final.caveats,
    }


def answer_question(
    question: str,
    history: list[dict[str, Any]] | None = None,
    *,
    user_id: int | str | None = None,
    request_id: str | None = None,
) -> dict[str, Any]:
    stages: list[dict[str, Any]] = []
    history = history or []
    conversation_state = build_conversation_state(history, question)
    stages.append(
        _stage(
            "conversation_manager",
            "completed",
            previous_turns=len(conversation_state.previous_user_messages),
            last_analytical_question=conversation_state.last_analytical_question,
            pending_clarification=bool(conversation_state.pending_clarification_question),
            carried_states=conversation_state.last_states,
        )
    )
    route = route_message(question, conversation_state)
    stages.append(_stage("assistant_router", "completed", **route.model_dump()))

    if route.mode in {"ASSISTANT_HELP", "GENERAL_ASSISTANT"}:
        final = assistant_help_answer()
        return _response_from_non_sql(question, final, route.model_dump(), stages, user_id, request_id)
    if route.mode == "DATASET_DISCOVERY":
        final = dataset_discovery_answer()
        return _response_from_non_sql(question, final, route.model_dump(), stages, user_id, request_id)
    if route.mode == "METRIC_DEFINITION":
        final = metric_definition_answer(question)
        return _response_from_non_sql(question, final, route.model_dump(), stages, user_id, request_id)
    if route.mode == "OUT_OF_SCOPE":
        final = out_of_scope_answer()
        return _response_from_non_sql(question, final, route.model_dump(), stages, user_id, request_id, resolution="unsupported")

    intent = classify_intent(question)
    intent["mode"] = route.mode
    if route.mode == "ROOT_CAUSE_ANALYSIS":
        intent["intent"] = "ROOT_CAUSE"
    elif route.mode == "FOLLOW_UP_ANALYTICS":
        intent["intent"] = "AMBIGUOUS"
    elif route.mode == "CLARIFICATION_RESPONSE":
        intent["intent"] = "AMBIGUOUS"
        intent["clarifies_question"] = conversation_state.pending_clarification_question or conversation_state.last_analytical_question
    stages.append(_stage("intent_classifier", "completed", **intent))

    context = retrieve_semantic_context(question)
    stages.append(_stage("semantic_retrieval", "completed", datasets=[item.dataset_id for item in context.datasets], metrics=[item.metric_id for item in context.metrics]))

    plan = create_query_plan(question, intent, context, history)
    stages.append(_stage("query_planner", "completed", intent=plan.intent, datasets=plan.datasets, metrics=plan.metrics, ambiguities=plan.ambiguities))
    ambiguity_resolution = "needs_clarification" if plan.intent == "AMBIGUOUS" else "unsupported" if plan.intent == "UNANSWERABLE" else "resolved"
    if any("proxy" in assumption.lower() for assumption in plan.assumptions):
        ambiguity_resolution = "applied_documented_default"
    stages.append(
        _stage(
            "ambiguity_resolver",
            "completed",
            resolution=ambiguity_resolution,
            assumptions=plan.assumptions,
            ambiguities=plan.ambiguities,
            alternatives=plan.alternatives,
        )
    )

    executions: list[dict[str, Any]] = []
    sql_items: list[dict[str, str]] = []
    verification = {"status": "ok", "warnings": []}

    if plan.intent not in {"DEFINITION", "AMBIGUOUS", "UNANSWERABLE"}:
        validate_query_plan(plan)
        stages.append(_stage("plan_validator", "completed"))

        sql_items = generate_sql(plan)
        stages.append(_stage("sql_generator", "completed", query_count=len(sql_items)))

        for item in sql_items:
            validate_sql(item["sql"])
        stages.append(_stage("sql_validator", "completed"))

        max_rows = int(os.getenv("MAX_RETURN_ROWS", "250"))
        executions = execute_sql_bundle(sql_items, max_rows=max_rows)
        stages.append(_stage("duckdb_executor", "completed", row_count=sum(item["row_count"] for item in executions)))

        verification = verify_results(executions)
        stages.append(_stage("result_verifier", verification["status"], warnings=verification["warnings"]))
    else:
        stages.append(_stage("plan_validator", "skipped"))
        stages.append(_stage("sql_generator", "skipped"))
        stages.append(_stage("sql_validator", "skipped"))
        stages.append(_stage("duckdb_executor", "skipped"))
        stages.append(_stage("result_verifier", "skipped"))

    final = generate_answer(question, plan, executions, verification)
    stages.append(_stage("grounded_answer_generator", "completed", confidence=final.confidence))

    primary_execution = executions[0] if executions else {}
    rows = primary_execution.get("rows", [])
    chart = build_chart(plan, executions)
    map_intent = build_map_intent(plan, executions) if executions else _empty_map_intent()
    if chart or map_intent.get("enabled"):
        stages.append(_stage("visual_recommender", "completed", chart=bool(chart), map=map_intent.get("mapType")))
    else:
        stages.append(_stage("visual_recommender", "skipped"))
    resolution = "needs_clarification" if plan.intent == "AMBIGUOUS" else "unsupported" if plan.intent == "UNANSWERABLE" else "answered"
    dataset = get_dataset(plan.datasets[0]) if plan.datasets else None
    metric = dataset.metrics.get(plan.metrics[0]) if dataset and plan.metrics else None
    year = None
    focus_state = None
    for filter_ in plan.queries[0].filters if plan.queries else []:
        if filter_.field == "year":
            year = filter_.value
        if filter_.field == "state" and isinstance(filter_.value, str):
            focus_state = filter_.value
    result_package = {
        "status": resolution,
        "contract_type": plan.intent,
        "family": plan.datasets[0] if plan.datasets else None,
        "metric": plan.metrics[0] if plan.metrics else None,
        "unit": metric.unit if metric else None,
        "scope": {"year": year, "focus_state": focus_state, "geography_level": dataset.geography if dataset else None},
        "assumptions": plan.assumptions,
        "sql": primary_execution.get("sql"),
        "rows": rows,
        "map_intent": map_intent,
        "chart_intent": {"enabled": bool(chart), "type": chart.get("mark", {}).get("type") if chart else None},
        "alternatives": plan.alternatives,
        "query_plan": plan.model_dump(),
        "semantic_context": context.model_dump(),
        "executions": executions,
        "verification": verification,
        "final_answer": final.model_dump(),
    }
    log_pipeline_event(
        {
            "request_id": request_id,
            "user_id": user_id,
            "question": question,
            "intent": plan.intent,
            "resolution": resolution,
            "datasets": plan.datasets,
            "metrics": plan.metrics,
            "query_count": len(sql_items),
            "row_count": len(rows),
            "confidence": final.confidence,
            "quality_status": verification["status"],
            "warnings": verification["warnings"],
        }
    )
    return {
        "answer": final.answer,
        "sql": primary_execution.get("sql"),
        "data": rows,
        "row_count": len(rows),
        "resolution": resolution,
        "mapIntent": map_intent,
        "chart": chart,
        "resultPackage": result_package,
        "contract": {
            "contract_type": plan.intent,
            "family": plan.datasets[0] if plan.datasets else None,
            "metric": plan.metrics[0] if plan.metrics else None,
            "operation": plan.queries[0].operation if plan.queries else None,
            "unit": metric.unit if metric else None,
            "geography_level": dataset.geography if dataset else None,
            "year": year,
            "focus_state": focus_state,
            "sort_direction": plan.queries[0].order if plan.queries else None,
            "top_k": plan.queries[0].limit if plan.queries else None,
            "tables": plan.datasets,
            "supported": resolution == "answered",
            "missing_slots": plan.ambiguities,
            "assumptions": plan.assumptions,
            "validation_message": verification["warnings"][0] if verification.get("warnings") else None,
        },
        "pipelineTrace": {"version": PIPELINE_VERSION, "stages": stages},
        "quality": {"status": verification["status"], "warnings": verification["warnings"]},
        "confidence": final.confidence,
        "key_numbers": [item.model_dump() for item in final.key_numbers],
        "assumptions": final.assumptions,
        "caveats": final.caveats,
    }
