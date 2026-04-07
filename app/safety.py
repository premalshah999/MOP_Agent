from __future__ import annotations

import re


BLOCKED = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "CREATE",
    "ALTER",
    "TRUNCATE",
    "ATTACH",
    "COPY",
    "EXPORT",
    "IMPORT",
    "INSTALL",
    "LOAD",
]


def is_safe(sql: str) -> bool:
    if not sql or not sql.strip():
        return False

    stripped = sql.strip()
    normalized = re.sub(r"\s+", " ", stripped).upper()

    # Single statement only.
    if ";" in stripped[:-1]:
        return False

    # Read-only statement only.
    first_token = stripped.lstrip().split(None, 1)[0].upper()
    if first_token not in {"SELECT", "WITH"}:
        return False

    for kw in BLOCKED:
        if re.search(rf"\b{re.escape(kw)}\b", normalized):
            return False

    return True
