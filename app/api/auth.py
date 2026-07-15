from __future__ import annotations

import hashlib
import hmac
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr, Field

from app.storage.sqlite import connect, row_dict


_security = HTTPBearer(auto_error=False)


def _jwt_secret() -> str:
    # Read lazily so `.env` loaded by app.main is honored even though this
    # module is imported before application initialization finishes.
    return os.getenv("JWT_SECRET", "local-dev-secret")


def _jwt_expiry_seconds() -> int:
    return int(os.getenv("JWT_EXPIRY_SECONDS", "86400"))


class RegisterRequest(BaseModel):
    name: str = Field(min_length=1)
    email: EmailStr
    password: str = Field(min_length=8)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1)


_PBKDF2_ITERATIONS = 600_000


def _hash_password(
    password: str,
    *,
    salt: str | None = None,
    iterations: int = _PBKDF2_ITERATIONS,
) -> str:
    salt = salt or os.urandom(16).hex()
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), iterations).hex()
    return f"pbkdf2_sha256${iterations}${salt}${digest}"


def _verify_password(password: str, encoded: str) -> bool:
    try:
        parts = encoded.split("$")
        if len(parts) == 3:
            _algo, salt, digest = parts
            iterations = 120_000  # legacy hashes created before iteration metadata
        elif len(parts) == 4:
            _algo, raw_iterations, salt, digest = parts
            iterations = int(raw_iterations)
        else:
            return False
    except ValueError:
        return False
    if _algo != "pbkdf2_sha256" or not 100_000 <= iterations <= 2_000_000:
        return False
    candidate = hashlib.pbkdf2_hmac(
        "sha256", password.encode(), salt.encode(), iterations
    ).hex()
    return hmac.compare_digest(candidate, digest)


def create_token(user: dict[str, Any]) -> str:
    expires = datetime.now(timezone.utc) + timedelta(seconds=_jwt_expiry_seconds())
    return jwt.encode({"sub": str(user["id"]), "email": user["email"], "name": user["name"], "exp": expires}, _jwt_secret(), algorithm="HS256")


def register_user(body: RegisterRequest) -> dict[str, Any]:
    with connect() as conn:
        try:
            conn.execute(
                "INSERT INTO users (email, name, password_hash) VALUES (?, ?, ?)",
                (body.email.lower(), body.name.strip(), _hash_password(body.password)),
            )
            conn.commit()
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail="Email already registered") from exc
        user = row_dict(conn.execute("SELECT id, email, name FROM users WHERE email = ?", (body.email.lower(),)).fetchone())
    assert user is not None
    return user


def authenticate_user(body: LoginRequest) -> dict[str, Any]:
    with connect() as conn:
        user = row_dict(conn.execute("SELECT * FROM users WHERE email = ?", (body.email.lower(),)).fetchone())
        if not user or not _verify_password(body.password, user["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid email or password")
        # Transparently upgrade legacy 120k hashes after a successful login.
        if len(str(user["password_hash"]).split("$")) == 3:
            conn.execute(
                "UPDATE users SET password_hash = ? WHERE id = ?",
                (_hash_password(body.password), user["id"]),
            )
            conn.commit()
    return {"id": user["id"], "email": user["email"], "name": user["name"]}


def update_profile(user_id: int, *, name: str) -> dict[str, Any]:
    clean = name.strip()
    if not clean or len(clean) > 80:
        raise HTTPException(status_code=422, detail="Name must be 1-80 characters")
    with connect() as conn:
        conn.execute("UPDATE users SET name = ? WHERE id = ?", (clean, user_id))
        conn.commit()
        user = row_dict(conn.execute("SELECT id, email, name FROM users WHERE id = ?", (user_id,)).fetchone())
    assert user is not None
    return user


def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_security),
) -> dict[str, Any]:
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    try:
        payload = jwt.decode(credentials.credentials, _jwt_secret(), algorithms=["HS256"])
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid bearer token") from exc
    return {"id": int(payload["sub"]), "email": payload["email"], "name": payload["name"]}
