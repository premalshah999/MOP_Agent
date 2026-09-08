"""Non-analytical responses: META, CLARIFY, UNANSWERABLE, OUT_OF_SCOPE.

None of these touch the database. CLARIFY and OUT_OF_SCOPE are templated (no LLM
needed); META and UNANSWERABLE get one grounded LLM call so the reply is helpful
and specific to this catalog.
"""

from __future__ import annotations

import json
import re
from typing import Any

from app.llm import client
from app.paths import MANIFEST_PATH
from app.semantic.discovery import build_guidance, discover_metrics
from app.semantic.registry import load_registry, table_metadata

_SCOPE_LINE = (
    "I answer questions about a fixed catalog of US public-policy data: Census "
    "demographics (ACS), state/local government finance, federal "
    "contracts/grants/spending (incl. by agency), FINRA financial-health "
    "indices, and federal subaward flows — at state, county, and congressional-"
    "district level."
)

_CATALOG_SEARCH_RE = re.compile(
    r"(could(?: not|n't) find|can(?: not|'t) find|closest (?:measure|variable)|"
    r"do you have (?:data|a (?:measure|variable))|is there .*?(?:data|measure|variable))",
    re.IGNORECASE,
)


def _facts(intent_payload: dict[str, Any]) -> str:
    reg = load_registry()
    manifest = json.loads(MANIFEST_PATH.read_text())
    requested = [
        table for table in (intent_payload.get("catalog_tables") or []) if table in reg.datasets
    ]
    datasets = (
        [reg.datasets[table] for table in requested] if requested else list(reg.datasets.values())
    )
    lines: list[str] = []
    for ds in datasets:
        metadata = table_metadata(ds.id)
        years = ", ".join(str(y) for y in ds.available_years) or "single snapshot"
        row_count = manifest.get(ds.id, {}).get("rows", "unknown")
        lines.append(
            f"### {ds.id}\n"
            f"- description: {ds.description}\n"
            f"- grain: {ds.grain}\n"
            f"- source: {metadata.get('source') or 'not documented'}; "
            f"source link: {metadata.get('source_url') or 'not documented'}\n"
            f"- physical rows: {row_count}\n"
            f"- years/periods: {years}; default: {ds.default_year or 'catalog snapshot'}"
        )
        for caveat in ds.caveats:
            lines.append(f"- runtime limitation: {caveat}")
        # Full variable facts are useful only for the referenced table(s). A
        # catalog-wide question gets compact summaries to stay within context.
        if requested:
            for column in ds.columns:
                metric = ds.metrics.get(column)
                dimension = ds.dimensions.get(column)
                if metric is not None:
                    aliases = ", ".join(metric.synonyms) or "none"
                    lines.append(
                        f"  - {column} [measure; unit={metric.unit}; "
                        f"row aggregation={metric.aggregation}; aliases={aliases}]: "
                        f"{metric.description}"
                    )
                elif dimension is not None:
                    lines.append(f"  - {column} [dimension]: {dimension.description}")
                else:
                    lines.append(f"  - {column} [structural column]")
    return "\n".join(lines)


def _llm_reply(system: str, user: str, purpose: str) -> str:
    try:
        return client.chat(
            [{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.0,
            max_tokens=600,
            purpose=purpose,
        ).strip()
    except client.LLMError:
        return ""


def _meta_critique(question: str, answer: str, facts: str) -> dict[str, Any]:
    try:
        raw = client.chat_json(
            [
                {
                    "role": "system",
                    "content": (
                        "Audit a catalog answer against the supplied authoritative facts. "
                        "Reject contradictions, unsupported claims, reversed column availability, "
                        "and any confusion between physical row count and the value or total of a "
                        "measure. Reject a causal explanation for a source definition when the "
                        "facts state only the definition or denominator. Reject labeling an "
                        "unweighted average across represented geographies as a population-weighted "
                        "national statistic. Also reject an answer "
                        "that fails to address what the user asked. "
                        'Return JSON only: {"faithful": <bool>, "complete": <bool>, '
                        '"reason": "<specific issue or empty>"}.'
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"QUESTION:\n{question}\n\nAUTHORITATIVE CATALOG FACTS:\n{facts}\n\n"
                        f"ANSWER TO AUDIT:\n{answer}"
                    ),
                },
            ],
            temperature=0.0,
            max_tokens=250,
            purpose="meta_faithfulness",
        )
    except client.LLMError:
        return {"faithful": False, "complete": False, "reason": "catalog critique unavailable"}
    return {
        "faithful": raw.get("faithful") is True,
        "complete": raw.get("complete") is True,
        "reason": str(raw.get("reason") or "").strip(),
    }


def respond(question: str, intent: str, intent_payload: dict[str, Any]) -> dict[str, Any]:
    # A user searching the data dictionary needs concept navigation regardless
    # of whether the router called the wording META, CLARIFY, or OUT_OF_SCOPE.
    # This also powers the dictionary's no-result CTA.
    if _CATALOG_SEARCH_RE.search(question):
        matches = discover_metrics(question, limit=1)
        intent_for_guidance = "CLARIFY" if matches and matches[0].score >= 0.86 else "UNANSWERABLE"
        return build_guidance(question, intent=intent_for_guidance)

    if intent == "CLARIFY":
        ask = (
            intent_payload.get("clarification_question")
            or intent_payload.get("reason")
            or ("Could you clarify which measure, geography level, and time period you mean?")
        )
        return build_guidance(question, intent="CLARIFY", clarification=str(ask))

    if intent == "OUT_OF_SCOPE":
        system = (
            "You are a friendly, sharp data assistant for US public-policy "
            "data. The user asked something outside your scope (small talk, "
            "weather, jokes, general knowledge, etc.). Reply in ONE or TWO "
            "warm, natural sentences: acknowledge the ask with a little "
            "personality (never scold, never say 'outside what I can help "
            "with'), then pivot to what you CAN do with a concrete, inviting "
            "example question they could ask. No lists, no headers.\n\n"
            "What you cover: " + _SCOPE_LINE
        )
        reply = _llm_reply(system, question, "out_of_scope") or (
            f"I'll stay in my lane on that one — my expertise is data. {_SCOPE_LINE}"
        )
        return {"answer": reply, "resolution": "unsupported", "confidence": "high"}

    if intent == "UNANSWERABLE":
        reason = str(intent_payload.get("reason") or "")
        # Router reasoning is useful evidence, but phrases about prompts,
        # examples, or internal classification should never leak into the UI.
        reason = re.sub(
            r"(?:^|[.!?]\s+)(?:the\s+)?(?:example|prompt|router|system message)\b[^.!?]*[.!?]?",
            " ",
            reason,
            flags=re.IGNORECASE,
        )
        reason = " ".join(reason.split()).strip()
        if reason and reason[-1] not in ".!?":
            reason += "."
        return build_guidance(
            question,
            intent="UNANSWERABLE",
            clarification=reason,
        )

    # META
    facts = _facts(intent_payload)
    system = (
        "You answer questions about THIS assistant and its catalog: what data "
        "exists, available years, schema, row counts, and the meaning of "
        "datasets/terms. Use only the facts below. Be concise and helpful.\n"
        "Never infer that a measure is absent unless the supplied column list "
        "shows it is absent. Never substitute physical row count for the value "
        "of a metric: rows describe table shape, while a requested total such "
        "as total employees requires an analytical query. If the user supplied "
        "a close dataset-name typo, use the canonical id shown in the facts and "
        "briefly say what you interpreted. Do not claim a numeric metric total "
        "unless that number is present in the facts. Treat runtime limitations "
        "as authoritative: broader upstream/source documentation does not make "
        "a variable queryable when it is absent from the loaded column list. "
        "Never infer a joint demographic subgroup from separate percentages. "
        "When the facts state a source-table universe or denominator but do not "
        "give a causal rationale for why the source chose it, say it is the "
        "source definition and do not speculate about the reason. Never relabel "
        "an unweighted AVG/MEDIAN across geography rows as a national population "
        "statistic; use the exact coverage language in the runtime limitations.\n\n"
        + _SCOPE_LINE
        + "\n\nDATASETS:\n"
        + facts
    )
    reply = _llm_reply(system, question, "meta") or _SCOPE_LINE
    verdict = _meta_critique(question, reply, facts)
    if verdict["faithful"] and verdict["complete"]:
        confidence = "high"
    else:
        repair_system = (
            system + "\n\nA previous draft failed catalog verification. Correct the stated issue "
            "without adding any fact not present above."
        )
        repaired = _llm_reply(
            repair_system,
            f"QUESTION:\n{question}\n\nFAILED DRAFT:\n{reply}\n\nISSUE:\n{verdict['reason']}",
            "meta_repair",
        )
        if repaired:
            repaired_verdict = _meta_critique(question, repaired, facts)
            reply = repaired
            confidence = (
                "high"
                if repaired_verdict["faithful"] and repaired_verdict["complete"]
                else "medium"
            )
        else:
            confidence = "medium"
    context_memory = None
    if intent_payload.get("catalog_tables"):
        context_memory = {
            "tables": list(intent_payload.get("catalog_tables") or []),
            "metrics": list(intent_payload.get("catalog_columns") or []),
            "operation": "catalog_question",
        }
    return {
        "answer": reply,
        "resolution": "answered",
        "confidence": confidence,
        "context_memory": context_memory,
    }
