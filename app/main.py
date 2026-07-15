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
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from pydantic import BaseModel, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware

from app.api.auth import LoginRequest, RegisterRequest, authenticate_user, create_token, get_current_user, register_user, update_profile
from app.api.datasets import dataset_catalog, download_path
from app.api.admin import is_admin, recent_feedback, recent_questions, usage_summary
from app.api.feedback import FeedbackRequest, record_feedback
from app.api.map_values import fetch_values
from app.api.threads import (
    create_message,
    create_share,
    create_thread,
    delete_all_threads,
    delete_thread,
    format_message,
    format_thread,
    get_thread,
    list_messages,
    list_threads,
    lookup_share,
    update_thread,
)
from app.core.orchestrator import PIPELINE_VERSION, answer_question
from app.duckdb.connection import initialize_duckdb, list_registered_views
from app.paths import DATA_DIR, FRONTEND_DIST, MANIFEST_PATH
from app.semantic.registry import load_registry
from app.storage.sqlite import init_storage


load_dotenv()

# Sentry — env-gated. When SENTRY_DSN is unset (dev / no observability stack)
# this is a complete no-op; nothing breaks if sentry-sdk isn't available.
_SENTRY_DSN = os.getenv("SENTRY_DSN", "").strip()
if _SENTRY_DSN:
    try:
        import sentry_sdk
        sentry_sdk.init(
            dsn=_SENTRY_DSN,
            traces_sample_rate=float(os.getenv("SENTRY_TRACES_RATE", "0.05")),
            environment=os.getenv("APP_VERSION", "production"),
            send_default_pii=False,
        )
    except Exception:
        pass


class AskRequest(BaseModel):
    question: str
    thread_id: str | None = None
    history: list[dict[str, Any]] = Field(default_factory=list)
    mode: str = "normal"  # "normal" (default, fast) | "reasoning" (agent loop)


class CreateThreadRequest(BaseModel):
    dataset_id: str = "contract_county"
    title: str = "New thread"


class UpdateThreadRequest(BaseModel):
    title: str | None = None
    dataset_id: str | None = None


def _frontend_built() -> bool:
    return (FRONTEND_DIST / "index.html").exists()


def _validate_production_config() -> None:
    if os.getenv("APP_ENV", "development").strip().lower() != "production":
        return
    problems: list[str] = []
    secret = os.getenv("JWT_SECRET", "").strip()
    weak_secrets = {
        "local-dev-secret",
        "change-me-before-public-use",
        "change-me-to-a-random-string",
        "replace-with-a-long-random-secret",
    }
    if len(secret) < 32 or secret in weak_secrets:
        problems.append("JWT_SECRET must be a non-placeholder secret of at least 32 characters")
    deepseek_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    fallback_key = os.getenv("OPENAI_API_KEY", "").strip()
    provider_key = deepseek_key or fallback_key
    if not provider_key or provider_key.startswith(("replace-", "change-me")):
        problems.append("a configured LLM provider key is required")
    origins = os.getenv("ALLOWED_ORIGINS", "").strip()
    hosts = os.getenv("TRUSTED_HOSTS", "").strip()
    if not origins or "your-domain.example" in origins or origins == "*":
        problems.append("ALLOWED_ORIGINS must name the deployed origin")
    if not hosts or "your-domain.example" in hosts or hosts == "*":
        problems.append("TRUSTED_HOSTS must name the deployed host")
    if problems:
        raise RuntimeError("Invalid production configuration: " + "; ".join(problems))


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
                "stage1_intent",
                "stage2_routing",
                "stage3_retrieval",
                "stage4_sql_generation",
                "sql_validator",
                "duckdb_executor",
                "self_repair_loop",
                "stage4_answer_generation",
                "faithfulness_judge",
            ],
        },
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Multi-worker startup: N uvicorn workers run this concurrently against
    # the same SQLite/DuckDB files. SQLite briefly locks during WAL/schema
    # setup and DuckDB holds an exclusive file lock while creating views —
    # retry with backoff instead of killing the worker (first worker wins,
    # the rest settle within a few seconds).
    import time as _time
    _validate_production_config()
    for attempt in range(30):
        try:
            init_storage()
            initialize_duckdb()
            break
        except Exception as exc:  # sqlite "database is locked" / duckdb IO lock
            msg = str(exc).lower()
            if attempt < 29 and ("lock" in msg or "conflicting" in msg):
                _time.sleep(0.5 + attempt * 0.25)
                continue
            raise
    yield


def _client_ip(request: Request) -> str:
    """Real client IP behind nginx (which forwards X-Forwarded-For)."""
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "anon"


limiter = Limiter(key_func=_client_ip)

app = FastAPI(title="MOP Controlled Analytics Assistant", lifespan=lifespan)
app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def _rate_limit_handler(request: Request, exc: RateLimitExceeded):
    request_id = getattr(request.state, "request_id", None)
    return _json(
        429,
        {
            "error": "rate_limited",
            "detail": "You're going a bit fast — please pause a moment and try again.",
            "limit": str(exc.detail),
        },
        request_id,
    )

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
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Permissions-Policy", "geolocation=(), camera=(), microphone=()")
    # HSTS only meaningful when served over HTTPS, but harmless to send anyway
    # — browsers ignore it on HTTP. Switch max-age higher (e.g. 63072000) once
    # TLS is in place and you've verified no HTTP regressions.
    response.headers.setdefault("Strict-Transport-Security", "max-age=300; includeSubDomains")
    response.headers.setdefault("X-Response-Time-Ms", str(int((time.perf_counter() - started) * 1000)))
    return response


@app.get("/health")
def health(request: Request):
    return _json(200, _health_payload(), request.state.request_id)


@app.get("/api/health")
def api_health(request: Request):
    return _json(200, _health_payload(), request.state.request_id)


def _probe_llm() -> tuple[bool, str]:
    """Trivial JSON probe against the configured LLM. Catches the failure
    mode where the API key is missing/revoked but everything else is up —
    in that state the app would silently fall to `_safe_default()` on every
    analytical question. Caller decides whether to 200 or 503 on this."""
    try:
        from app.llm import client as _llm
        out = _llm.chat_json(
            [
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": 'Reply {"ok": true}'},
            ],
            temperature=0.0,
            max_tokens=20,
            purpose="health_probe",
        )
        return (bool(out.get("ok")), "ok")
    except Exception as exc:
        return (False, str(exc)[:240])


def _probe_duckdb() -> tuple[bool, str]:
    try:
        from app.duckdb.connection import execute_select
        execute_select("SELECT 1 AS v", max_rows=1)
        return (True, "ok")
    except Exception as exc:
        return (False, str(exc)[:240])


def _probe_sqlite() -> tuple[bool, str]:
    try:
        from app.storage.sqlite import connect
        with connect() as conn:
            conn.execute("SELECT 1").fetchone()
        return (True, "ok")
    except Exception as exc:
        return (False, str(exc)[:240])


@app.get("/health/deep")
def health_deep(request: Request):
    """Active liveness probe for UptimeRobot. Tests LLM + DuckDB + SQLite.
    Returns 503 when any backend dependency is down; UptimeRobot will alert.

    Light-weight enough for the default 5-min interval (one trivial LLM
    call + two SQL pings, ~1-2s total). Authoritative for the failure
    mode where everything looks up but analytical answers silently
    fall to clarification."""
    llm_ok, llm_detail = _probe_llm()
    ddb_ok, ddb_detail = _probe_duckdb()
    sq_ok, sq_detail = _probe_sqlite()
    healthy = llm_ok and ddb_ok and sq_ok
    payload = {
        "status": "ok" if healthy else "degraded",
        "service": "mop-agent",
        "version": os.getenv("APP_VERSION", "0.1.0"),
        "probes": {
            "llm": {"ok": llm_ok, "detail": llm_detail},
            "duckdb": {"ok": ddb_ok, "detail": ddb_detail},
            "sqlite": {"ok": sq_ok, "detail": sq_detail},
        },
    }
    return _json(200 if healthy else 503, payload, request.state.request_id)


@app.post("/api/auth/register")
@limiter.limit("5/minute")
def register(body: RegisterRequest, request: Request):
    user = register_user(body)
    return _json(201, {"token": create_token(user), "user": user}, request.state.request_id)


@app.post("/api/auth/login")
@limiter.limit("10/minute")
def login(body: LoginRequest, request: Request):
    user = authenticate_user(body)
    return _json(200, {"token": create_token(user), "user": user}, request.state.request_id)


@app.get("/api/auth/me")
def me(request: Request, user: dict[str, Any] = Depends(get_current_user)):
    return _json(200, {"user": {**user, "is_admin": is_admin(user)}}, request.state.request_id)


class UpdateProfileRequest(BaseModel):
    name: str


@app.patch("/api/auth/me")
def update_me(body: UpdateProfileRequest, request: Request, user: dict[str, Any] = Depends(get_current_user)):
    updated = update_profile(user["id"], name=body.name)
    # Re-issue the token so the embedded name matches the new profile.
    return _json(200, {"user": {**updated, "is_admin": is_admin(updated)}, "token": create_token(updated)}, request.state.request_id)


@app.get("/api/admin/usage")
def admin_usage(request: Request, user: dict[str, Any] = Depends(get_current_user)):
    return _json(200, usage_summary(user), request.state.request_id)


@app.get("/api/admin/questions")
def admin_questions(request: Request, user: dict[str, Any] = Depends(get_current_user), limit: int = 50):
    return _json(200, {"items": recent_questions(user, limit=limit)}, request.state.request_id)


@app.get("/api/admin/feedback")
def admin_feedback(request: Request, user: dict[str, Any] = Depends(get_current_user), limit: int = 50):
    return _json(200, {"items": recent_feedback(user, limit=limit)}, request.state.request_id)


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


@app.post("/api/threads/{thread_id}/share")
def api_share_thread(thread_id: str, request: Request, user: dict[str, Any] = Depends(get_current_user)):
    result = create_share(thread_id, user["id"])
    if not result:
        raise HTTPException(status_code=404, detail="Thread not found")
    return _json(200, result, request.state.request_id)


@app.get("/api/share/{token}")
def api_shared_thread(token: str, request: Request):
    """Public read-only view of a shared thread. Token itself is the auth."""
    result = lookup_share(token)
    if not result:
        raise HTTPException(status_code=404, detail="Shared thread not found")
    return _json(
        200,
        {
            "thread": format_thread(result["thread"]),
            "messages": [format_message(m) for m in result["messages"]],
        },
        request.state.request_id,
    )


@app.get("/api/threads/{thread_id}/messages")
def api_thread_messages(thread_id: str, request: Request, user: dict[str, Any] = Depends(get_current_user)):
    if not get_thread(thread_id, user["id"]):
        raise HTTPException(status_code=404, detail="Thread not found")
    return _json(200, {"messages": [format_message(message) for message in list_messages(thread_id)]}, request.state.request_id)


@app.post("/api/ask/stream")
@limiter.limit("30/minute;200/hour")
def ask_stream(body: AskRequest, request: Request, user: dict[str, Any] = Depends(get_current_user)):
    """SSE stream of pipeline events.

    Emits `stage` / `tool` / `tool_start` events as the pipeline progresses, then
    one final `done` event with the complete JSON envelope (same shape as
    /api/ask). The frontend can render live progress; failure falls back to a
    final `error` event.
    """
    import json as _json
    import queue
    import threading

    thread = get_thread(body.thread_id, user["id"]) if body.thread_id else None
    if not thread:
        title = body.question[:60] + ("..." if len(body.question) > 60 else "")
        thread = create_thread(user["id"], title=title)
    elif thread.get("title") in (None, "", "New thread"):
        # First question into a placeholder thread names it server-side, so
        # titles survive even if the client never sends its rename call.
        title = body.question[:60] + ("..." if len(body.question) > 60 else "")
        update_thread(thread["id"], user["id"], title=title)
    stored_history: list[dict[str, Any]] = []
    for message in list_messages(thread["id"]):
        if message["role"] not in {"user", "assistant"}:
            continue
        formatted = format_message(message)
        item: dict[str, Any] = {"role": formatted["role"], "content": formatted["content"]}
        if formatted.get("contract"):
            item["contract"] = formatted["contract"]
        if formatted.get("suggestedFollowups"):
            item["suggested_followups"] = formatted["suggestedFollowups"]
        stored_history.append(item)
    stored_history = stored_history[-12:]
    # Capture context before persisting the current turn.  Otherwise the
    # pipeline receives the same question once as history and once as input.
    user_message = create_message(thread["id"], "user", body.question)

    events: "queue.Queue[tuple[str, Any]]" = queue.Queue()
    SENTINEL = object()

    def _on_event(kind: str, payload: dict[str, Any]) -> None:
        events.put((kind, payload))

    final_result: dict[str, Any] = {}
    final_error: list[str] = []

    def _run() -> None:
        try:
            result = answer_question(
                body.question,
                body.history or stored_history,
                user_id=user["id"],
                request_id=request.state.request_id,
                mode=body.mode,
                on_event=_on_event,
            )
            assistant_payload = {
                "sqlQuery": result.get("sql"),
                "data": result.get("data"),
                "rowCount": result.get("row_count"),
                "chart": result.get("chart"),
                "charts": result.get("charts"),
                "keyNumbers": result.get("key_numbers"),
                "caveats": result.get("caveats"),
                "confidence": result.get("confidence"),
                "glossary": result.get("glossary"),
                "verifiedQuery": result.get("verified_query"),
                "suggestedFollowups": result.get("suggested_followups"),
                "resolution": result.get("resolution"),
                "mapIntent": result.get("mapIntent"),
                "resultPackage": result.get("resultPackage"),
                "contract": result.get("contract"),
                "pipelineTrace": result.get("pipelineTrace"),
                "quality": result.get("quality"),
            }
            assistant_message = create_message(thread["id"], "assistant", result["answer"], assistant_payload)
            final_result.update(
                {
                    **result,
                    "thread_id": thread["id"],
                    "user_message_id": user_message["id"],
                    "assistant_message_id": assistant_message["id"],
                    "request_id": request.state.request_id,
                }
            )
        except Exception as exc:  # surface any pipeline failure as an SSE error
            final_error.append(str(exc))
        finally:
            events.put((SENTINEL, None))  # type: ignore[arg-type]

    threading.Thread(target=_run, daemon=True).start()

    def _format(kind: str, payload: Any) -> str:
        return f"event: {kind}\ndata: {_json.dumps(payload, default=str)}\n\n"

    def _generator():
        yield _format("open", {"request_id": request.state.request_id})
        while True:
            kind, payload = events.get()
            if kind is SENTINEL:
                break
            yield _format(kind, payload)
        if final_error:
            yield _format("error", {"detail": final_error[0]})
        else:
            yield _format("done", final_result)

    return StreamingResponse(
        _generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "X-Request-ID": request.state.request_id,
        },
    )


@app.post("/api/ask")
@limiter.limit("30/minute;200/hour")
def ask(body: AskRequest, request: Request, user: dict[str, Any] = Depends(get_current_user)):
    thread = get_thread(body.thread_id, user["id"]) if body.thread_id else None
    if not thread:
        title = body.question[:60] + ("..." if len(body.question) > 60 else "")
        thread = create_thread(user["id"], title=title)
    elif thread.get("title") in (None, "", "New thread"):
        # First question into a placeholder thread names it server-side, so
        # titles survive even if the client never sends its rename call.
        title = body.question[:60] + ("..." if len(body.question) > 60 else "")
        update_thread(thread["id"], user["id"], title=title)
    stored_history = []
    for message in list_messages(thread["id"]):
        if message["role"] not in {"user", "assistant"}:
            continue
        formatted = format_message(message)
        item = {"role": formatted["role"], "content": formatted["content"]}
        if formatted.get("contract"):
            item["contract"] = formatted["contract"]
        if formatted.get("suggestedFollowups"):
            item["suggested_followups"] = formatted["suggestedFollowups"]
        stored_history.append(item)
    stored_history = stored_history[-12:]
    user_message = create_message(thread["id"], "user", body.question)
    result = answer_question(
        body.question,
        body.history or stored_history,
        user_id=user["id"],
        request_id=request.state.request_id,
        mode=body.mode,
    )
    assistant_payload = {
        "sqlQuery": result.get("sql"),
        "data": result.get("data"),
        "rowCount": result.get("row_count"),
        "chart": result.get("chart"),
        "charts": result.get("charts"),
        "keyNumbers": result.get("key_numbers"),
        "caveats": result.get("caveats"),
        "confidence": result.get("confidence"),
        "glossary": result.get("glossary"),
        "verifiedQuery": result.get("verified_query"),
        "suggestedFollowups": result.get("suggested_followups"),
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


@app.post("/api/feedback")
def api_feedback(
    body: FeedbackRequest,
    request: Request,
    user: dict[str, Any] = Depends(get_current_user),
):
    result = record_feedback(body, user)
    return _json(200, result, request.state.request_id)


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
