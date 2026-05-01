from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware

from app.api.auth import LoginRequest, RegisterRequest, authenticate_user, create_token, get_current_user, register_user
from app.api.datasets import dataset_catalog, download_path
from app.api.map_values import fetch_values
from app.api.threads import (
    create_message,
    create_thread,
    delete_all_threads,
    delete_thread,
    format_message,
    format_thread,
    get_thread,
    list_messages,
    list_threads,
    update_thread,
)
from app.core.orchestrator import PIPELINE_VERSION, answer_question
from app.duckdb.connection import initialize_duckdb, list_registered_views
from app.paths import DATA_DIR, FRONTEND_DIST, MANIFEST_PATH
from app.semantic.registry import load_registry
from app.storage.sqlite import init_storage


load_dotenv()


class AskRequest(BaseModel):
    question: str
    thread_id: str | None = None
    history: list[dict[str, Any]] = Field(default_factory=list)


class CreateThreadRequest(BaseModel):
    dataset_id: str = "contract_county"
    title: str = "New thread"


class UpdateThreadRequest(BaseModel):
    title: str | None = None
    dataset_id: str | None = None


def _frontend_built() -> bool:
    return (FRONTEND_DIST / "index.html").exists()


def _json(status_code: int, payload: dict[str, Any], request_id: str | None = None) -> JSONResponse:
    headers = {"Cache-Control": "no-store"}
    if request_id:
        headers["X-Request-ID"] = request_id
    return JSONResponse(status_code=status_code, content=jsonable_encoder(payload), headers=headers)


def _health_payload() -> dict[str, Any]:
    views = list_registered_views()
    registry = load_registry()
    return {
        "status": "ok",
        "service": "mop-agent",
        "version": os.getenv("APP_VERSION", "0.1.0"),
        "checks": {
            "manifest_present": MANIFEST_PATH.exists(),
            "registered_view_count": len(views),
            "semantic_dataset_count": len(registry.datasets),
            "frontend_built": _frontend_built(),
            "pipeline_ready": True,
        },
        "pipeline": {
            "version": PIPELINE_VERSION,
            "architecture": [
                "conversation_manager",
                "assistant_router",
                "intent_classifier",
                "semantic_retrieval",
                "query_planner",
                "ambiguity_resolver",
                "plan_validator",
                "sql_generator",
                "sql_validator",
                "duckdb_executor",
                "result_verifier",
                "grounded_answer_generator",
            ],
        },
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_storage()
    initialize_duckdb()
    yield


app = FastAPI(title="MOP Controlled Analytics Assistant", lifespan=lifespan)

origins = [item.strip() for item in os.getenv("ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").split(",") if item.strip()]
app.add_middleware(CORSMiddleware, allow_origins=origins or ["*"], allow_methods=["*"], allow_headers=["*"])
app.add_middleware(TrustedHostMiddleware, allowed_hosts=[item.strip() for item in os.getenv("TRUSTED_HOSTS", "127.0.0.1,localhost,testserver").split(",") if item.strip()])


@app.middleware("http")
async def request_context(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", uuid4().hex)
    request.state.request_id = request_id
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception as exc:
        debug_errors = os.getenv("DEBUG_ERRORS", "").lower() in {"1", "true", "yes"}
        detail = str(exc) if debug_errors else "Unexpected server error."
        response = _json(500, {"error": "Internal server error", "detail": detail, "request_id": request_id}, request_id)
    response.headers["X-Request-ID"] = request_id
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("X-Response-Time-Ms", str(int((time.perf_counter() - started) * 1000)))
    return response


@app.get("/health")
def health(request: Request):
    return _json(200, _health_payload(), request.state.request_id)


@app.get("/api/health")
def api_health(request: Request):
    return _json(200, _health_payload(), request.state.request_id)


@app.post("/api/auth/register")
def register(body: RegisterRequest, request: Request):
    user = register_user(body)
    return _json(201, {"token": create_token(user), "user": user}, request.state.request_id)


@app.post("/api/auth/login")
def login(body: LoginRequest, request: Request):
    user = authenticate_user(body)
    return _json(200, {"token": create_token(user), "user": user}, request.state.request_id)


@app.get("/api/auth/me")
def me(request: Request, user: dict[str, Any] = Depends(get_current_user)):
    return _json(200, {"user": user}, request.state.request_id)


@app.get("/api/threads")
def api_list_threads(request: Request, user: dict[str, Any] = Depends(get_current_user)):
    return _json(200, {"threads": [format_thread(thread) for thread in list_threads(user["id"])]}, request.state.request_id)


@app.post("/api/threads")
def api_create_thread(body: CreateThreadRequest, request: Request, user: dict[str, Any] = Depends(get_current_user)):
    thread = create_thread(user["id"], body.dataset_id, body.title)
    return _json(201, {"thread": format_thread(thread, messages=[])}, request.state.request_id)


@app.get("/api/threads/{thread_id}")
def api_get_thread(thread_id: str, request: Request, user: dict[str, Any] = Depends(get_current_user)):
    thread = get_thread(thread_id, user["id"])
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    return _json(200, {"thread": format_thread(thread, messages=list_messages(thread_id))}, request.state.request_id)


@app.put("/api/threads/{thread_id}")
def api_update_thread(thread_id: str, body: UpdateThreadRequest, request: Request, user: dict[str, Any] = Depends(get_current_user)):
    thread = update_thread(thread_id, user["id"], title=body.title, dataset_id=body.dataset_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    return _json(200, {"thread": format_thread(thread)}, request.state.request_id)


@app.delete("/api/threads/{thread_id}")
def api_delete_thread(thread_id: str, request: Request, user: dict[str, Any] = Depends(get_current_user)):
    if not delete_thread(thread_id, user["id"]):
        raise HTTPException(status_code=404, detail="Thread not found")
    return _json(200, {"ok": True}, request.state.request_id)


@app.delete("/api/threads")
def api_delete_all_threads(request: Request, user: dict[str, Any] = Depends(get_current_user)):
    return _json(200, {"deleted": delete_all_threads(user["id"])}, request.state.request_id)


@app.get("/api/threads/{thread_id}/messages")
def api_thread_messages(thread_id: str, request: Request, user: dict[str, Any] = Depends(get_current_user)):
    if not get_thread(thread_id, user["id"]):
        raise HTTPException(status_code=404, detail="Thread not found")
    return _json(200, {"messages": [format_message(message) for message in list_messages(thread_id)]}, request.state.request_id)


@app.post("/api/ask")
def ask(body: AskRequest, request: Request, user: dict[str, Any] = Depends(get_current_user)):
    thread = get_thread(body.thread_id, user["id"]) if body.thread_id else None
    if not thread:
        title = body.question[:60] + ("..." if len(body.question) > 60 else "")
        thread = create_thread(user["id"], title=title)
    user_message = create_message(thread["id"], "user", body.question)

    stored_history = []
    for message in list_messages(thread["id"]):
        if message["role"] not in {"user", "assistant"}:
            continue
        formatted = format_message(message)
        item = {"role": formatted["role"], "content": formatted["content"]}
        if formatted.get("contract"):
            item["contract"] = formatted["contract"]
        stored_history.append(item)
    stored_history = stored_history[-12:]
    result = answer_question(body.question, body.history or stored_history, user_id=user["id"], request_id=request.state.request_id)
    assistant_payload = {
        "sqlQuery": result.get("sql"),
        "data": result.get("data"),
        "rowCount": result.get("row_count"),
        "chart": result.get("chart"),
        "resolution": result.get("resolution"),
        "mapIntent": result.get("mapIntent"),
        "resultPackage": result.get("resultPackage"),
        "contract": result.get("contract"),
        "pipelineTrace": result.get("pipelineTrace"),
        "quality": result.get("quality"),
    }
    assistant_message = create_message(thread["id"], "assistant", result["answer"], assistant_payload)
    payload = {
        **result,
        "thread_id": thread["id"],
        "user_message_id": user_message["id"],
        "assistant_message_id": assistant_message["id"],
        "request_id": request.state.request_id,
    }
    return _json(200, payload, request.state.request_id)


@app.get("/api/datasets")
def api_datasets(request: Request):
    return _json(200, {"datasets": dataset_catalog()}, request.state.request_id)


@app.get("/api/datasets/download/{table_name}")
def api_download_dataset(table_name: str, format: str = "parquet"):
    return download_path(table_name, format)


@app.get("/api/values")
def api_values(dataset: str, level: str, variable: str, request: Request, year: str | None = None, state: str | None = None):
    rows = fetch_values(dataset, level, variable, year=year, state=state)
    return _json(200, {"rows": rows, "row_count": len(rows)}, request.state.request_id)


BOUNDARIES_DIR = DATA_DIR / "boundaries"
if BOUNDARIES_DIR.exists():
    app.mount("/geo", StaticFiles(directory=str(BOUNDARIES_DIR), html=False), name="geo")

if _frontend_built():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIST), html=True), name="frontend")
