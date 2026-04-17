from __future__ import annotations

import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware

from app.agent import ask_agent
from app.dataset_catalog import build_dataset_catalog, dataset_download_path
from app.auth import (
    AuthResponse,
    LoginRequest,
    RegisterRequest,
    authenticate_user,
    create_token,
    create_user,
    get_current_user,
)
from app.database import (
    create_message,
    create_thread,
    delete_all_threads,
    delete_thread,
    get_messages_for_thread,
    get_thread,
    get_threads_for_user,
    init_db,
    update_thread,
)
from app.db import get_registered_tables, register_all_tables
from app.map_api import fetch_map_values
from app.query_logger import log_query


load_dotenv()

LOGGER = logging.getLogger("mop_agent.api")
SERVICE_NAME = "mop-agent"
SERVICE_VERSION = os.getenv("APP_VERSION", "1.0.0")
FRONTEND_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
MANIFEST_PATH = Path(__file__).resolve().parent.parent / "data" / "schema" / "manifest.json"
BOUNDARIES_DIR = Path(__file__).resolve().parent.parent / "data" / "boundaries"
SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "X-Permitted-Cross-Domain-Policies": "none",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
    "Cross-Origin-Opener-Policy": "same-origin",
}
CONTENT_SECURITY_POLICY = "; ".join(
    [
        "default-src 'self'",
        "base-uri 'self'",
        "object-src 'none'",
        "frame-ancestors 'none'",
        "connect-src 'self' https://basemaps.cartocdn.com https://*.basemaps.cartocdn.com",
        "img-src 'self' data: blob: https://basemaps.cartocdn.com https://*.basemaps.cartocdn.com",
        "font-src 'self' data: https://fonts.gstatic.com",
        "script-src 'self'",
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
        "worker-src 'self' blob:",
    ]
)


def _parse_csv_env(name: str, default: str) -> list[str]:
    return [value.strip() for value in os.getenv(name, default).split(",") if value.strip()]


def _frontend_built() -> bool:
    return (FRONTEND_DIST / "index.html").exists() and (FRONTEND_DIST / "assets").exists()


def _health_payload() -> dict[str, Any]:
    registered_tables = get_registered_tables()
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "checks": {
            "manifest_present": MANIFEST_PATH.exists(),
            "registered_table_count": len(registered_tables),
            "frontend_built": _frontend_built(),
        },
    }


def _json_response(
    status_code: int,
    content: dict[str, Any],
    *,
    request_id: str | None,
    cache_control: str = "no-store",
) -> JSONResponse:
    headers = {"Cache-Control": cache_control}
    if request_id:
        headers["X-Request-ID"] = request_id
    return JSONResponse(status_code=status_code, content=content, headers=headers)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    register_all_tables()
    app.state.startup_info = _health_payload()
    LOGGER.info(
        "startup_complete service=%s version=%s registered_tables=%s frontend_built=%s",
        SERVICE_NAME,
        SERVICE_VERSION,
        app.state.startup_info["checks"]["registered_table_count"],
        app.state.startup_info["checks"]["frontend_built"],
    )
    yield


app = FastAPI(title="MOP Chat Agent", lifespan=lifespan)

origins = _parse_csv_env(
    "ALLOWED_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000,http://127.0.0.1:3000",
)
trusted_hosts = _parse_csv_env("TRUSTED_HOSTS", "127.0.0.1,localhost,testserver")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if origins == ["*"] else origins,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=int(os.getenv("GZIP_MINIMUM_SIZE", "1000")))
app.add_middleware(TrustedHostMiddleware, allowed_hosts=trusted_hosts)


@app.middleware("http")
async def add_request_context(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", uuid4().hex)
    request.state.request_id = request_id
    started_at = time.perf_counter()

    try:
        response = await call_next(request)
    except Exception:
        LOGGER.exception("unhandled_error request_id=%s path=%s", request_id, request.url.path)
        response = _json_response(
            500,
            {
                "error": "Internal server error",
                "request_id": request_id,
                "sql": None,
                "data": [],
                "row_count": 0,
            },
            request_id=request_id,
        )

    duration_ms = int((time.perf_counter() - started_at) * 1000)
    response.headers["X-Request-ID"] = request_id
    for header, value in SECURITY_HEADERS.items():
        response.headers.setdefault(header, value)
    if _frontend_built():
        response.headers.setdefault("Content-Security-Policy", CONTENT_SECURITY_POLICY)
    LOGGER.info(
        "request_complete request_id=%s method=%s path=%s status=%s duration_ms=%s",
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


# ── Request models ──

class Question(BaseModel):
    question: str
    history: list[Any] = Field(default_factory=list)


class AskRequest(BaseModel):
    question: str
    thread_id: str | None = None
    history: list[Any] = Field(default_factory=list)


class CreateThreadRequest(BaseModel):
    dataset_id: str = "government_finance"
    title: str = "New thread"


class UpdateThreadRequest(BaseModel):
    title: str | None = None
    dataset_id: str | None = None


class MapValuesResponse(BaseModel):
    rows: list[dict[str, Any]]
    row_count: int


# ── Auth routes ──


@app.post("/api/auth/register")
def register(body: RegisterRequest, request: Request):
    request_id = getattr(request.state, "request_id", None)
    user = create_user(body.name, body.email, body.password)
    token = create_token(user["id"], user["email"], user["name"])
    return _json_response(
        201,
        {
            "token": token,
            "user": {"id": user["id"], "name": user["name"], "email": user["email"]},
        },
        request_id=request_id,
    )


@app.post("/api/auth/login")
def login(body: LoginRequest, request: Request):
    request_id = getattr(request.state, "request_id", None)
    user = authenticate_user(body.email, body.password)
    token = create_token(user["id"], user["email"], user["name"])
    return _json_response(
        200,
        {
            "token": token,
            "user": {"id": user["id"], "name": user["name"], "email": user["email"]},
        },
        request_id=request_id,
    )


@app.get("/api/auth/me")
def me(request: Request, current_user: dict = Depends(get_current_user)):
    request_id = getattr(request.state, "request_id", None)
    return _json_response(
        200,
        {"user": {"id": current_user["sub"], "name": current_user["name"], "email": current_user["email"]}},
        request_id=request_id,
    )


# ── Thread CRUD ──


def _format_thread(t: dict[str, Any], messages: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Format a thread row for API response."""
    result = {
        "id": t["id"],
        "title": t["title"],
        "datasetId": t["dataset_id"],
        "createdAt": t["created_at"],
        "updatedAt": t["updated_at"],
    }
    if messages is not None:
        result["messages"] = [_format_message(m) for m in messages]
    return result


def _format_message(m: dict[str, Any]) -> dict[str, Any]:
    """Format a message row for API response."""
    result: dict[str, Any] = {
        "id": m["id"],
        "role": m["role"],
        "content": m["content"],
        "ts": m["created_at"],
    }
    if m.get("sql_query"):
        result["sqlQuery"] = m["sql_query"]
    if m.get("data_json"):
        try:
            result["data"] = json.loads(m["data_json"])
        except (json.JSONDecodeError, TypeError):
            result["data"] = []
    if m.get("row_count"):
        result["rowCount"] = m["row_count"]
    if m.get("error"):
        result["error"] = m["error"]
    return result


@app.get("/api/threads")
def list_threads(request: Request, current_user: dict = Depends(get_current_user)):
    request_id = getattr(request.state, "request_id", None)
    user_id = current_user["sub"]
    threads = get_threads_for_user(user_id)
    return _json_response(
        200,
        {"threads": [_format_thread(t) for t in threads]},
        request_id=request_id,
    )


@app.post("/api/threads")
def create_thread_endpoint(body: CreateThreadRequest, request: Request, current_user: dict = Depends(get_current_user)):
    request_id = getattr(request.state, "request_id", None)
    user_id = current_user["sub"]
    thread_id = uuid4().hex
    thread = create_thread(thread_id, user_id, body.dataset_id, body.title)
    return _json_response(
        201,
        {"thread": _format_thread(thread, messages=[])},
        request_id=request_id,
    )


@app.get("/api/threads/{thread_id}")
def get_thread_endpoint(thread_id: str, request: Request, current_user: dict = Depends(get_current_user)):
    request_id = getattr(request.state, "request_id", None)
    user_id = current_user["sub"]
    thread = get_thread(thread_id, user_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    messages = get_messages_for_thread(thread_id)
    return _json_response(
        200,
        {"thread": _format_thread(thread, messages=messages)},
        request_id=request_id,
    )


@app.put("/api/threads/{thread_id}")
def update_thread_endpoint(thread_id: str, body: UpdateThreadRequest, request: Request, current_user: dict = Depends(get_current_user)):
    request_id = getattr(request.state, "request_id", None)
    user_id = current_user["sub"]
    fields = {}
    if body.title is not None:
        fields["title"] = body.title
    if body.dataset_id is not None:
        fields["dataset_id"] = body.dataset_id
    thread = update_thread(thread_id, user_id, **fields)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    return _json_response(200, {"thread": _format_thread(thread)}, request_id=request_id)


@app.delete("/api/threads/{thread_id}")
def delete_thread_endpoint(thread_id: str, request: Request, current_user: dict = Depends(get_current_user)):
    request_id = getattr(request.state, "request_id", None)
    user_id = current_user["sub"]
    deleted = delete_thread(thread_id, user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Thread not found")
    return _json_response(200, {"deleted": True}, request_id=request_id)


@app.delete("/api/threads")
def clear_all_threads(request: Request, current_user: dict = Depends(get_current_user)):
    request_id = getattr(request.state, "request_id", None)
    user_id = current_user["sub"]
    count = delete_all_threads(user_id)
    return _json_response(200, {"deleted_count": count}, request_id=request_id)


@app.get("/api/threads/{thread_id}/messages")
def list_messages(thread_id: str, request: Request, current_user: dict = Depends(get_current_user)):
    request_id = getattr(request.state, "request_id", None)
    user_id = current_user["sub"]
    thread = get_thread(thread_id, user_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    messages = get_messages_for_thread(thread_id)
    return _json_response(
        200,
        {"messages": [_format_message(m) for m in messages]},
        request_id=request_id,
    )


@app.get("/api/datasets")
def list_datasets(request: Request):
    request_id = getattr(request.state, "request_id", None)
    return _json_response(
        200,
        {"datasets": build_dataset_catalog()},
        request_id=request_id,
    )


@app.get("/api/datasets/download/{table_name}")
def download_dataset(table_name: str, request: Request, format: str = "parquet"):
    try:
        path, filename = dataset_download_path(table_name, format)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    media_type = "application/octet-stream"
    if filename.endswith(".parquet"):
        media_type = "application/vnd.apache.parquet"
    elif filename.endswith(".xlsx"):
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return FileResponse(str(path), filename=filename, media_type=media_type)


@app.get("/api/values")
def values(
    request: Request,
    dataset: str,
    level: str,
    variable: str,
    year: str | None = None,
    state: str | None = None,
    agency: str | None = None,
):
    request_id = getattr(request.state, "request_id", None)
    try:
        rows = fetch_map_values(
            dataset=dataset,
            level=level,
            variable=variable,
            year=year,
            state=state,
            agency=agency,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return _json_response(
        200,
        {"rows": rows, "row_count": len(rows)},
        request_id=request_id,
    )


# ── Main query endpoint (requires auth, persists to DB) ──


@app.post("/api/ask")
def ask(body: AskRequest, request: Request, current_user: dict = Depends(get_current_user)):
    request_id = getattr(request.state, "request_id", None)
    user_id = current_user["sub"]
    thread_id = body.thread_id

    # Auto-create thread if not provided
    if not thread_id:
        thread_id = uuid4().hex
        title = body.question[:60].strip()
        if len(body.question) > 60:
            title += "..."
        create_thread(thread_id, user_id, "government_finance", title)

    # Verify thread belongs to user
    thread = get_thread(thread_id, user_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    # Persist user message
    user_msg_id = uuid4().hex
    create_message(user_msg_id, thread_id, "user", body.question)

    # Update thread title from first user message if still default
    if thread["title"] == "New thread":
        title = body.question[:60].strip()
        if len(body.question) > 60:
            title += "..."
        update_thread(thread_id, user_id, title=title)

    # Build history from DB if not provided
    history = body.history
    if not history:
        db_messages = get_messages_for_thread(thread_id)
        history = [
            {"role": m["role"], "content": m["content"]}
            for m in db_messages[:-1]  # exclude the message we just inserted
        ]

    try:
        result = ask_agent(body.question, history)
    except Exception as exc:
        LOGGER.exception("ask_failed request_id=%s", request_id)
        log_query(question=body.question, sql=None, success=False, row_count=0, error=str(exc), user_id=user_id, thread_id=thread_id)

        error_msg = "Something went wrong processing your question. Please try rephrasing."
        assistant_msg_id = uuid4().hex
        create_message(assistant_msg_id, thread_id, "assistant", error_msg, error=str(exc))

        return _json_response(
            500,
            {
                "error": "Internal server error",
                "request_id": request_id,
                "thread_id": thread_id,
                "sql": None,
                "data": [],
                "row_count": 0,
                "user_message_id": user_msg_id,
                "assistant_message_id": assistant_msg_id,
            },
            request_id=request_id,
        )

    sql = result.get("sql") if isinstance(result, dict) else None
    err = result.get("error") if isinstance(result, dict) else None
    row_count = int(result.get("row_count", 0)) if isinstance(result, dict) else 0
    answer = result.get("answer", "") if isinstance(result, dict) else ""
    data = result.get("data", []) if isinstance(result, dict) else []
    chart = result.get("chart") if isinstance(result, dict) else None

    log_query(question=body.question, sql=sql, success=err is None, row_count=row_count, error=err, user_id=user_id, thread_id=thread_id)

    if not isinstance(result, dict):
        error_msg = "Received an invalid response. Please try again."
        assistant_msg_id = uuid4().hex
        create_message(assistant_msg_id, thread_id, "assistant", error_msg, error="Invalid agent response")

        return _json_response(
            500,
            {
                "error": "Invalid agent response",
                "request_id": request_id,
                "thread_id": thread_id,
                "sql": None,
                "data": [],
                "row_count": 0,
                "user_message_id": user_msg_id,
                "assistant_message_id": assistant_msg_id,
            },
            request_id=request_id,
        )

    # Persist assistant message
    assistant_msg_id = uuid4().hex
    data_json = json.dumps(data) if data else None
    create_message(
        assistant_msg_id,
        thread_id,
        "assistant",
        answer or err or "No answer returned.",
        sql_query=sql,
        data_json=data_json,
        row_count=row_count,
        error=err,
    )

    payload = {
        **result,
        "request_id": request_id,
        "thread_id": thread_id,
        "user_message_id": user_msg_id,
        "assistant_message_id": assistant_msg_id,
    }
    if err is not None:
        return _json_response(502, payload, request_id=request_id)
    return _json_response(200, payload, request_id=request_id)


@app.get("/health")
def health(request: Request):
    request_id = getattr(request.state, "request_id", None)
    return _json_response(200, _health_payload(), request_id=request_id)


@app.get("/api/health")
def api_health(request: Request):
    request_id = getattr(request.state, "request_id", None)
    return _json_response(200, _health_payload(), request_id=request_id)


if FRONTEND_DIST.exists() and (FRONTEND_DIST / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST / "assets")), name="assets")

if BOUNDARIES_DIR.exists():
    app.mount("/geo", StaticFiles(directory=str(BOUNDARIES_DIR)), name="geo")


@app.get("/", include_in_schema=False)
def root():
    index_file = FRONTEND_DIST / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    raise HTTPException(status_code=503, detail="Frontend build not found. Run `cd frontend && npm run build`.")


@app.get("/{full_path:path}", include_in_schema=False)
def spa_fallback(full_path: str):
    if full_path.startswith("api") or full_path == "health":
        raise HTTPException(status_code=404, detail="Not Found")

    if not FRONTEND_DIST.exists():
        raise HTTPException(status_code=404, detail="Frontend build not found")

    candidate = FRONTEND_DIST / full_path
    if candidate.exists() and candidate.is_file():
        return FileResponse(str(candidate))

    index_file = FRONTEND_DIST / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))

    raise HTTPException(status_code=404, detail="Not Found")
