from __future__ import annotations

import hashlib
import hmac
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr, Field

from app.storage.sqlite import connect, row_dict


JWT_SECRET = os.getenv("JWT_SECRET", "local-dev-secret")
JWT_EXPIRY_SECONDS = int(os.getenv("JWT_EXPIRY_SECONDS", "86400"))
_security = HTTPBearer(auto_error=False)


class RegisterRequest(BaseModel):
    name: str = Field(min_length=1)
    email: EmailStr
    password: str = Field(min_length=6)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1)


def _hash_password(password: str, *, salt: str | None = None) -> str:
    salt = salt or os.urandom(16).hex()
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 120_000).hex()
    return f"pbkdf2_sha256${salt}${digest}"


def _verify_password(password: str, encoded: str) -> bool:
    try:
        _algo, salt, digest = encoded.split("$", 2)
    except ValueError:
        return False
    candidate = _hash_password(password, salt=salt).split("$", 2)[2]
    return hmac.compare_digest(candidate, digest)


def create_token(user: dict[str, Any]) -> str:
    expires = datetime.now(timezone.utc) + timedelta(seconds=JWT_EXPIRY_SECONDS)
    return jwt.encode({"sub": str(user["id"]), "email": user["email"], "name": user["name"], "exp": expires}, JWT_SECRET, algorithm="HS256")


def register_user(body: RegisterRequest) -> dict[str, Any]:
    with connect() as conn:
        try:
            conn.execute(
                "INSERT INTO users (email, name, password_hash) VALUES (?, ?, ?)",
                (body.email.lower(), body.name.strip(), _hash_password(body.password)),
            )
            conn.commit()
        except Exception as exc:
            raise HTTPException(status_code=409, detail="Email already registered") from exc
        user = row_dict(conn.execute("SELECT id, email, name FROM users WHERE email = ?", (body.email.lower(),)).fetchone())
    assert user is not None
    return user


def authenticate_user(body: LoginRequest) -> dict[str, Any]:
    with connect() as conn:
        user = row_dict(conn.execute("SELECT * FROM users WHERE email = ?", (body.email.lower(),)).fetchone())
    if not user or not _verify_password(body.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return {"id": user["id"], "email": user["email"], "name": user["name"]}


def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_security),
) -> dict[str, Any]:
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=["HS256"])
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid bearer token") from exc
    return {"id": int(payload["sub"]), "email": payload["email"], "name": payload["name"]}
