from __future__ import annotations

import re

from app.semantic.registry import all_allowed_views

try:
    import sqlglot
    from sqlglot import exp
    from sqlglot.errors import ParseError
except ImportError:  # pragma: no cover - requirements install sqlglot in normal runtime
    sqlglot = None
    exp = None
    ParseError = Exception


FORBIDDEN_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "COPY", "EXPORT", "ATTACH", "DETACH",
    "INSTALL", "LOAD", "CALL", "PRAGMA", "READ_CSV", "READ_PARQUET", "READ_JSON",
}
_TABLE_REF_RE = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)


class SqlValidationError(ValueError):
    pass


def _parser_table_refs(sql: str) -> tuple[set[str], set[str]]:
    if sqlglot is None or exp is None:
        return set(), set()
    try:
        statements = sqlglot.parse(sql, read="duckdb")
    except ParseError as exc:
        raise SqlValidationError(f"SQL parser rejected the query: {exc}") from exc
    if len(statements) != 1:
        raise SqlValidationError("Exactly one SQL statement is allowed.")

    statement = statements[0]
    if not isinstance(statement, exp.Select):
        raise SqlValidationError("Only SELECT/WITH statements are allowed.")

    cte_names = {cte.alias_or_name for cte in statement.find_all(exp.CTE)}
    table_refs = {table.name for table in statement.find_all(exp.Table)}
    return table_refs, cte_names


def _regex_table_refs(sql: str) -> set[str]:
    return set(_TABLE_REF_RE.findall(sql))


def validate_sql(sql: str) -> None:
    stripped = sql.strip().rstrip(";")
    upper = stripped.upper()
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise SqlValidationError("Only SELECT/WITH statements are allowed.")
    if len(stripped) > 12000:
        raise SqlValidationError("SQL is too long.")
    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{re.escape(keyword)}\b", upper):
            raise SqlValidationError(f"Forbidden SQL keyword/function: {keyword}.")
    allowed_views = all_allowed_views()
    parser_refs, cte_names = _parser_table_refs(stripped)
    refs = parser_refs or _regex_table_refs(stripped)
    if not refs:
        raise SqlValidationError("SQL must reference at least one whitelisted view.")
    allowed_ctes = cte_names or {"base", "ranked"}
    disallowed = sorted(ref for ref in refs if ref not in allowed_views and ref not in allowed_ctes)
    if disallowed:
        raise SqlValidationError(f"SQL references non-whitelisted tables/views: {', '.join(disallowed)}.")
    if ".." in stripped or "information_schema" in upper or "sqlite_master" in upper:
        raise SqlValidationError("SQL may not access system metadata or path-like objects.")
