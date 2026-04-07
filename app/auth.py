"""JWT authentication with centralized SQLite user storage."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import secrets
import sqlite3
import time
from typing import Any

from fastapi import HTTPException, Request
from pydantic import BaseModel, EmailStr, Field

from app.database import create_user as db_create_user, get_user_by_email

LOGGER = logging.getLogger("mop_agent.auth")

# ── Configuration ──

JWT_SECRET = os.getenv("JWT_SECRET", "")
if not JWT_SECRET:
    JWT_SECRET = secrets.token_hex(32)
    LOGGER.warning("JWT_SECRET not set — using random secret (tokens won't survive restarts)")

JWT_ALGORITHM = "HS256"
JWT_EXPIRY_SECONDS = int(os.getenv("JWT_EXPIRY_SECONDS", "86400"))  # 24 hours


# ── Simple JWT (no external dependency) ──

def _b64url_encode(data: bytes) -> str:
    import base64
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    import base64
    padding = 4 - len(s) % 4
    return base64.urlsafe_b64decode(s + "=" * padding)


def _jwt_sign(payload: dict[str, Any]) -> str:
    header = {"alg": JWT_ALGORITHM, "typ": "JWT"}
    h = _b64url_encode(json.dumps(header, separators=(",", ":")).encode())
    p = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode())
    msg = f"{h}.{p}".encode()
    sig = hmac.new(JWT_SECRET.encode(), msg, hashlib.sha256).digest()
    return f"{h}.{p}.{_b64url_encode(sig)}"


def _jwt_verify(token: str) -> dict[str, Any] | None:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        h, p, s = parts
        msg = f"{h}.{p}".encode()
        expected = hmac.new(JWT_SECRET.encode(), msg, hashlib.sha256).digest()
        actual = _b64url_decode(s)
        if not hmac.compare_digest(expected, actual):
            return None
        payload = json.loads(_b64url_decode(p))
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except Exception:
        return None


def create_token(user_id: int, email: str, name: str) -> str:
    return _jwt_sign({
        "sub": user_id,
        "email": email,
        "name": name,
        "exp": time.time() + JWT_EXPIRY_SECONDS,
        "iat": time.time(),
    })


# ── Password hashing (PBKDF2 — stdlib, no bcrypt dependency) ──

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260_000)
    return f"pbkdf2:sha256:260000${salt}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        _, _, rest = stored.partition(":")
        _, _, rest = rest.partition(":")
        iterations_str, salt, dk_hex = rest.split("$")
        iterations = int(iterations_str)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), iterations)
        return hmac.compare_digest(dk.hex(), dk_hex)
    except Exception:
        return False


# ── Public auth functions ──

def create_user(name: str, email: str, password: str) -> dict[str, Any]:
    try:
        user = db_create_user(email, name, hash_password(password))
        return user
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="An account with this email already exists")


def authenticate_user(email: str, password: str) -> dict[str, Any]:
    user = get_user_by_email(email)
    if not user or not verify_password(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return user


# ── Pydantic models ──

class RegisterRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    email: EmailStr
    password: str = Field(..., min_length=6, max_length=128)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=1)


class AuthResponse(BaseModel):
    token: str
    user: dict[str, Any]


# ── FastAPI dependency ──

def get_current_user(request: Request) -> dict[str, Any]:
    """Extract and verify JWT from Authorization header."""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization header")
    token = auth_header[7:]
    payload = _jwt_verify(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return payload
