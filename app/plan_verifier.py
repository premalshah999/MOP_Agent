from __future__ import annotations

import re
from dataclasses import dataclass

from app.query_frame import QueryFrame, infer_query_frame
from app.semantic_registry import runtime_table_loaded


@dataclass(frozen=True)
class VerificationResult:
    ok: bool
    answer: str | None = None
    error: str | None = None


_TABLE_PATTERN = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)


def _normalize_sql(sql: str) -> str:
    return " ".join((sql or "").split())


def extract_sql_tables(sql: str) -> list[str]:
    tables: list[str] = []
    for match in _TABLE_PATTERN.finditer(sql or ""):
        name = match.group(1)
        if name not in tables:
            tables.append(name)
    return tables


def _actual_tables(sql: str, planned_tables: list[str] | None) -> list[str]:
    extracted = extract_sql_tables(sql)
    return extracted or list(planned_tables or [])


def _contains_any(sql: str, snippets: tuple[str, ...]) -> bool:
    lower = (sql or "").lower()
    return any(snippet.lower() in lower for snippet in snippets)


def _verify_runtime_coverage(frame: QueryFrame, tables: list[str]) -> VerificationResult | None:
    if frame.family == "agency":
        if frame.geo_level == "county" and not runtime_table_loaded("contract_county_agency"):
            return VerificationResult(
                ok=False,
                answer="The requested county-level agency spending data is not available in the current runtime yet, so I can’t answer that reliably from loaded tables.",
            )
        if frame.geo_level == "congress" and not runtime_table_loaded("contract_cd_agency"):
            return VerificationResult(
                ok=False,
                answer="The requested congressional-district agency spending data is not available in the current runtime yet, so I can’t answer that reliably from loaded tables.",
            )

    if frame.family == "breakdown" and frame.geo_level in {"county", "congress"}:
        return VerificationResult(
            ok=False,
            answer="Federal Spending Breakdown is state-only in the current runtime. County or congressional agency analysis needs the agency-granular federal spending tables instead.",
        )

    return None


def _verify_relative_metric(frame: QueryFrame, sql: str) -> VerificationResult | None:
    if not frame.wants_relative:
        return None
    if frame.metric_hint is None:
        return None
    if "per 1000" in frame.metric_hint.lower() or "_per_capita" in frame.metric_hint.lower():
        return None
    if _contains_any(sql, ('"Contracts Per 1000"', '"Grants Per 1000"', '"Resident Wage Per 1000"', '"Direct Payments Per 1000"', '"Federal Residents Per 1000"', '"Employees Per 1000"', '"Employees Wage Per 1000"', "_per_capita")):
        return None
    return VerificationResult(
        ok=False,
        error="Relative-exposure questions must use stored normalized `Per 1000` or `_per_capita` fields.",
    )


def _verify_congress_scope(frame: QueryFrame, tables: list[str], sql: str) -> VerificationResult | None:
    if frame.geo_level != "congress" or not frame.state_postal:
        return None

    congress_tables = {"gov_congress", "contract_congress", "finra_congress", "contract_cd_agency"}
    if not (congress_tables & set(tables)):
        return None

    sql_lower = sql.lower()
    if f"upper(cd_118) like '{frame.state_postal}-%'".lower() in sql_lower:
        return None
    if frame.primary_state and f"lower(state) = '{frame.primary_state}'" in sql_lower:
        return None

    return VerificationResult(
        ok=False,
        error=f"Congressional district query is missing a {frame.state_postal}-scoped filter.",
    )


def _verify_share_math(frame: QueryFrame, sql: str) -> VerificationResult | None:
    if frame.intent != "share":
        return None

    # ACS and FINRA contain many stored share / percentage / score columns.
    # Those should be allowed as direct metrics instead of being treated like
    # custom denominator math.
    if frame.family in {"acs", "finra"}:
        return None

    sql_lower = sql.lower()
    if "share_pct" in sql_lower:
        return None
    if "nullif(" in sql_lower and " over ()" in sql_lower:
        return None
    if " / " in sql_lower and ("sum(" in sql_lower or "avg(" in sql_lower):
        return None

    return VerificationResult(
        ok=False,
        error="Share-style questions need an explicit denominator-aware calculation.",
    )


def _verify_jobs_metric(frame: QueryFrame, sql: str) -> VerificationResult | None:
    if not frame.wants_jobs_metric:
        return None
    sql_lower = sql.lower()
    if "employees" in sql_lower and "spending_total" not in sql_lower:
        return None
    return VerificationResult(
        ok=False,
        error="Jobs-focused questions should be answered with the `Employees` metric, not a spending composite.",
    )


def _verify_explicit_period(frame: QueryFrame, sql: str, tables: list[str]) -> VerificationResult | None:
    if not frame.period_label:
        return None

    if any(table.startswith(("contract_", "spending_")) for table in tables):
        if f"'{frame.period_label}'" not in sql:
            return VerificationResult(
                ok=False,
                error=f"Expected explicit period filter `{frame.period_label}` in the SQL plan.",
            )
    return None


def _verify_flow_semantics(frame: QueryFrame, tables: list[str], sql: str) -> VerificationResult | None:
    if frame.family != "flow":
        return None

    sql_lower = sql.lower()
    if "state_flow" in tables and frame.primary_state:
        if frame.flow_direction == "inflow" and f"lower(subawardee_state_name) = '{frame.primary_state}'" not in sql_lower:
            return VerificationResult(ok=False, error="Inflow state-flow queries must filter on the destination state.")
        if frame.flow_direction == "outflow" and f"lower(rcpt_state_name) = '{frame.primary_state}'" not in sql_lower:
            return VerificationResult(ok=False, error="Outflow state-flow queries must filter on the origin state.")

    if "county_flow" in tables or "congress_flow" in tables:
        if frame.primary_state and frame.flow_direction == "inflow":
            if f"lower(subawardee_state) = '{frame.primary_state}'" not in sql_lower:
                return VerificationResult(ok=False, error="Inflow county/congress flow queries must filter on the destination state.")
        if frame.primary_state and frame.flow_direction == "outflow":
            if f"lower(rcpt_state) = '{frame.primary_state}'" not in sql_lower:
                return VerificationResult(ok=False, error="Outflow county/congress flow queries must filter on the origin state.")

    if frame.wants_displayed_flow and "<>" not in sql:
        return VerificationResult(ok=False, error="Displayed-flow questions must exclude internal flows.")

    if frame.wants_internal_flow and "=" not in sql_lower:
        return VerificationResult(ok=False, error="Internal-flow questions must explicitly require origin and destination to match.")

    if frame.wants_agency_dimension and "agency_name" not in sql_lower:
        return VerificationResult(ok=False, error="Agency flow breakdowns must select or group by `agency_name`.")

    if frame.wants_industry_dimension and "naics_2digit_title" not in sql_lower:
        return VerificationResult(ok=False, error="Industry flow breakdowns must use `naics_2digit_title`.")

    return None


def verify_execution_candidate(
    question: str,
    sql: str,
    table_names: list[str] | None = None,
) -> VerificationResult:
    frame = infer_query_frame(question)
    normalized_sql = _normalize_sql(sql)
    actual_tables = _actual_tables(normalized_sql, table_names)

    for check in (
        _verify_runtime_coverage,
        _verify_relative_metric,
        _verify_congress_scope,
        _verify_share_math,
        _verify_jobs_metric,
        _verify_explicit_period,
        _verify_flow_semantics,
    ):
        if check in {_verify_runtime_coverage}:
            result = check(frame, actual_tables)
        elif check in {_verify_relative_metric, _verify_share_math, _verify_jobs_metric}:
            result = check(frame, normalized_sql)
        elif check in {_verify_congress_scope, _verify_flow_semantics}:
            result = check(frame, actual_tables, normalized_sql)
        elif check in {_verify_explicit_period}:
            result = check(frame, normalized_sql, actual_tables)
        else:
            result = None
        if result:
            return result

    return VerificationResult(ok=True)
