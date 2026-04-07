from __future__ import annotations

import argparse
import asyncio
import json
import re
import statistics
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


RANK_KEYWORDS = (
    "highest",
    "lowest",
    "most",
    "least",
    "top",
    "bottom",
    "rank",
    "leading",
)


def _word_count(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text or ""))


def _is_ranking_question(question: str) -> bool:
    q = question.lower()
    return any(k in q for k in RANK_KEYWORDS)


def _explicit_n(question: str) -> int | None:
    q = question.lower()
    m = re.search(r"\b(?:top|bottom)\s+(\d{1,3})\b", q)
    if m:
        return int(m.group(1))
    words = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
        "fifteen": 15,
        "twenty": 20,
    }
    m2 = re.search(
        r"\b(?:top|bottom)\s+(one|two|three|four|five|six|seven|eight|nine|ten|fifteen|twenty)\b",
        q,
    )
    if m2:
        return words[m2.group(1)]
    return None


def _add_question(
    out: list[dict[str, Any]],
    seen: set[str],
    category: str,
    question: str,
) -> None:
    key = re.sub(r"\s+", " ", question.strip().lower())
    if not key or key in seen:
        return
    seen.add(key)
    out.append(
        {
            "id": len(out) + 1,
            "category": category,
            "question": question.strip(),
        }
    )


def build_question_set() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    states = [
        "Maryland",
        "Virginia",
        "Texas",
        "California",
        "New York",
        "Florida",
        "Pennsylvania",
        "Illinois",
        "Georgia",
        "North Carolina",
    ]

    gov_metrics = [
        "total liabilities per capita",
        "debt ratio",
        "current ratio",
        "revenue per capita",
        "net position per capita",
        "free cash flow per capita",
        "net pension liability per capita",
    ]

    for metric in gov_metrics:
        _add_question(out, seen, "government_finance", f"Which 15 states have the highest {metric}?")
        _add_question(out, seen, "government_finance", f"Which 15 states have the lowest {metric}?")

    for state in states:
        _add_question(
            out,
            seen,
            "government_finance",
            f"How does {state} compare with the national average on debt ratio and total liabilities per capita?",
        )

    county_states = ["Maryland", "Virginia", "Texas", "California", "Florida", "Georgia"]
    for state in county_states:
        _add_question(
            out,
            seen,
            "government_finance",
            f"Which 15 counties in {state} have the highest debt ratio?",
        )
        _add_question(
            out,
            seen,
            "government_finance",
            f"Which 15 counties in {state} have the highest total liabilities per capita?",
        )

    _add_question(
        out,
        seen,
        "government_finance",
        "Which congressional districts have the highest debt ratio? Show top 20.",
    )
    _add_question(
        out,
        seen,
        "government_finance",
        "Which congressional districts have the lowest current ratio? Show bottom 20.",
    )
    _add_question(
        out,
        seen,
        "government_finance",
        "Which states have revenue per capita below expenses per capita? Show 20 with largest gaps.",
    )
    _add_question(
        out,
        seen,
        "government_finance",
        "Which states have negative free cash flow and high debt ratio?",
    )
    _add_question(
        out,
        seen,
        "government_finance",
        "Rank states by net pension liability per capita and include debt ratio for context.",
    )

    spend_metrics = [
        "contracts",
        "grants",
        "direct payments",
        "resident wage",
        "federal residents",
    ]

    for metric in spend_metrics:
        _add_question(
            out,
            seen,
            "federal_spending",
            f"Which 15 states received the most federal {metric} in 2024?",
        )
        _add_question(
            out,
            seen,
            "federal_spending",
            f"Which 15 states received the most federal {metric} per 1000 residents in 2024?",
        )

    agency_states = ["Maryland", "Virginia", "Texas", "California", "Florida", "New York"]
    for state in agency_states:
        _add_question(
            out,
            seen,
            "federal_spending_agency",
            f"Which agencies provided the most grants in {state} in 2024? Show top 10.",
        )
        _add_question(
            out,
            seen,
            "federal_spending_agency",
            f"Which agencies provided the most contracts in {state} in 2024? Show top 10.",
        )

    _add_question(
        out,
        seen,
        "federal_spending_agency",
        "How much did the Department of Defense spend in Maryland in 2024? Break out contracts, grants, and direct payments.",
    )
    _add_question(
        out,
        seen,
        "federal_spending_agency",
        "How much did the Department of Defense spend in Virginia in 2024? Break out contracts, grants, and direct payments.",
    )
    _add_question(
        out,
        seen,
        "federal_spending_agency",
        "Compare Department of Defense contracts across states in 2024. Show top 15 states.",
    )
    _add_question(
        out,
        seen,
        "federal_spending_agency",
        "Compare Department of Defense grants across states in 2024. Show top 15 states.",
    )
    _add_question(
        out,
        seen,
        "federal_spending",
        "Which congressional districts received the most grants in 2024? Show top 20.",
    )
    _add_question(
        out,
        seen,
        "federal_spending",
        "Which congressional districts received the most contracts per 1000 in 2024? Show top 20.",
    )
    _add_question(
        out,
        seen,
        "federal_spending",
        "Which counties in Maryland received the most direct payments per 1000 in 2024? Show top 20.",
    )
    _add_question(
        out,
        seen,
        "federal_spending",
        "Which counties in Texas received the most grants in 2024? Show top 20.",
    )
    _add_question(
        out,
        seen,
        "federal_spending",
        "Compare contracts, grants, and direct payments for California and Texas in 2024.",
    )
    _add_question(
        out,
        seen,
        "federal_spending",
        "Which states have unusually high grants per 1000 but low contracts per 1000 in 2024?",
    )

    acs_metrics = [
        "below poverty",
        "median household income",
        "education at or above bachelor's",
        "income above $100K",
        "owner occupied housing",
    ]

    for metric in acs_metrics:
        _add_question(out, seen, "acs", f"Which 15 states rank highest on {metric} in 2023?")
        _add_question(out, seen, "acs", f"Which 15 states rank lowest on {metric} in 2023?")

    acs_states = ["Maryland", "Virginia", "Texas", "California", "Florida"]
    for state in acs_states:
        _add_question(
            out,
            seen,
            "acs",
            f"Which 15 counties in {state} have the highest poverty rates in 2023?",
        )
        _add_question(
            out,
            seen,
            "acs",
            f"Which 15 counties in {state} have the highest median household income in 2023?",
        )

    _add_question(out, seen, "acs", "Which congressional districts have the highest poverty rates in 2023? Show top 20.")
    _add_question(out, seen, "acs", "Which congressional districts have the highest median household income in 2023? Show top 20.")
    _add_question(out, seen, "acs", "Compare renter occupied versus owner occupied rates across states in 2023.")
    _add_question(out, seen, "acs", "Which states show high poverty rates despite high median household income in 2023?")
    _add_question(out, seen, "acs", "Which states have the largest share of households with income above $200K in 2023?")

    finra_metrics = [
        "financial literacy",
        "financial constraint",
        "alternative financing",
        "risk aversion",
        "satisfaction",
    ]

    for metric in finra_metrics:
        _add_question(out, seen, "finra", f"Which 15 states rank highest on {metric} in 2021?")
        _add_question(out, seen, "finra", f"Which 15 states rank lowest on {metric} in 2021?")

    finra_states = ["Maryland", "Virginia", "Texas", "California", "Florida"]
    for state in finra_states:
        _add_question(
            out,
            seen,
            "finra",
            f"How does {state} compare with the national average on financial literacy in 2021?",
        )
        _add_question(
            out,
            seen,
            "finra",
            f"How does {state} compare with the national average on financial constraint in 2021?",
        )

    _add_question(out, seen, "finra", "Which counties in Maryland have the highest financial literacy in 2021? Show top 20.")
    _add_question(out, seen, "finra", "Which counties in Texas have the highest financial constraint in 2021? Show top 20.")
    _add_question(out, seen, "finra", "Which congressional districts have high financial literacy but low financial constraint in 2021?")
    _add_question(out, seen, "finra", "Are states with high financial literacy also high in satisfaction in 2021?")
    _add_question(out, seen, "finra", "Which states combine high risk aversion and high financial constraint in 2021?")

    flow_states = ["Maryland", "Virginia", "Texas", "California", "Florida"]
    for state in flow_states:
        _add_question(
            out,
            seen,
            "fund_flow",
            f"Which states receive the most federal subaward funding from {state}? Show top 15.",
        )
        _add_question(
            out,
            seen,
            "fund_flow",
            f"Which agencies send the most subaward dollars from {state}? Show top 15.",
        )

    _add_question(out, seen, "fund_flow", "From Maryland, which NAICS 2-digit sectors receive the most subaward dollars? Show top 15.")
    _add_question(out, seen, "fund_flow", "From Maryland, what share of subaward dollars stays within Maryland versus goes to other states?")
    _add_question(out, seen, "fund_flow", "Which recipient-state and subawardee-state pairs have the largest subaward flows overall? Show top 20.")
    _add_question(out, seen, "fund_flow", "How have Maryland outbound subaward totals changed by fiscal year?")
    _add_question(out, seen, "fund_flow", "Compare Maryland and Virginia outbound subaward totals by agency.")
    _add_question(out, seen, "fund_flow", "Which agencies dominate Maryland to Virginia subaward flows?")
    _add_question(out, seen, "fund_flow", "Which states receive the smallest non-zero subaward amounts from Maryland? Show bottom 10.")
    _add_question(out, seen, "fund_flow", "Which counties receive the highest subaward amounts from Maryland recipient counties? Show top 20.")
    _add_question(out, seen, "fund_flow", "Which congressional districts receive the highest subaward amounts from Maryland recipient districts? Show top 20.")

    _add_question(
        out,
        seen,
        "cross_dataset",
        "Do states with higher financial literacy tend to have lower government debt ratios? Include correlation and sample size.",
    )
    _add_question(
        out,
        seen,
        "cross_dataset",
        "Compare poverty rates and financial constraint across states in 2021. Show states where both are high.",
    )
    _add_question(
        out,
        seen,
        "cross_dataset",
        "Do states with higher poverty rates receive higher grants per 1000 in 2024?",
    )
    _add_question(
        out,
        seen,
        "cross_dataset",
        "Do states with higher median household income have lower debt ratios?",
    )
    _add_question(
        out,
        seen,
        "cross_dataset",
        "Which states are simultaneously high in financial constraint and high in debt ratio?",
    )
    _add_question(
        out,
        seen,
        "cross_dataset",
        "Which congressional districts have high poverty rates and low federal contracts per 1000?",
    )
    _add_question(
        out,
        seen,
        "cross_dataset",
        "Are states with stronger educational attainment associated with higher financial literacy scores?",
    )
    _add_question(
        out,
        seen,
        "cross_dataset",
        "Compare states by both grants per 1000 and current ratio. Which states are high on both?",
    )
    _add_question(
        out,
        seen,
        "cross_dataset",
        "Which states combine high total liabilities per capita with high financial constraint?",
    )
    _add_question(
        out,
        seen,
        "cross_dataset",
        "Do higher federal direct payments per 1000 align with higher poverty rates across states?",
    )

    # Ensure stable IDs after any truncation.
    out = out[:150]
    for i, q in enumerate(out, start=1):
        q["id"] = i

    if len(out) != 150:
        raise RuntimeError(f"Expected 150 questions, built {len(out)}")

    return out


async def ask_one(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    item: dict[str, Any],
) -> dict[str, Any]:
    queued_started = time.perf_counter()
    payload = {"question": item["question"], "history": []}

    status_code: int | None = None
    error: str | None = None
    answer = ""
    sql: str | None = None
    row_count = 0
    data_len = 0
    api_latency_s = 0.0

    async with sem:
        queue_wait_s = round(time.perf_counter() - queued_started, 3)
        api_started = time.perf_counter()
        try:
            resp = await client.post("/api/ask", json=payload)
            status_code = resp.status_code
            if resp.status_code != 200:
                error = f"HTTP_{resp.status_code}"
            else:
                body = resp.json()
                error = body.get("error")
                answer = body.get("answer") or ""
                sql = body.get("sql")
                row_count = int(body.get("row_count") or 0)
                data = body.get("data") or []
                if isinstance(data, list):
                    data_len = len(data)
        except Exception as exc:  # noqa: BLE001
            error = f"REQUEST_FAILED: {exc}"
        finally:
            api_latency_s = round(time.perf_counter() - api_started, 3)

    total_latency_s = round(time.perf_counter() - queued_started, 3)
    words = _word_count(answer)

    flags: list[str] = []
    if error:
        flags.append("error")
        msg = error.lower()
        if "parser error" in msg:
            flags.append("parser_error")
        if "safety" in msg:
            flags.append("safety_reject")
        if "timeout" in msg:
            flags.append("timeout")
        if "rate" in msg and "limit" in msg:
            flags.append("rate_limit")

    no_data_tokens = (
        "no data found",
        "not available in the current dataset",
        "data_not_available",
    )
    if any(t in (answer or "").lower() for t in no_data_tokens):
        flags.append("no_data_answer")

    if not answer.strip() and not error:
        flags.append("empty_answer")

    if words < 80 and not error:
        flags.append("too_short")
    elif words < 140 and not error:
        flags.append("short")

    if not sql and item["category"] != "conceptual":
        flags.append("missing_sql")

    if _is_ranking_question(item["question"]) and not error:
        explicit = _explicit_n(item["question"])
        minimum_expected = 1 if explicit == 1 else min(explicit or 5, 10)
        if row_count < minimum_expected:
            flags.append("ranking_too_few_rows")

    if api_latency_s > 45:
        flags.append("slow_response")

    score = 100
    if error:
        score -= 55
    if "parser_error" in flags:
        score -= 25
    if "empty_answer" in flags:
        score -= 20
    if "too_short" in flags:
        score -= 20
    elif "short" in flags:
        score -= 10
    if "no_data_answer" in flags:
        score -= 10
    if "missing_sql" in flags:
        score -= 10
    if "ranking_too_few_rows" in flags:
        score -= 15
    if "slow_response" in flags:
        score -= 10
    score = max(0, min(100, score))

    return {
        "id": item["id"],
        "category": item["category"],
        "question": item["question"],
        "status_code": status_code,
        "latency_s": total_latency_s,
        "queue_wait_s": queue_wait_s,
        "api_latency_s": api_latency_s,
        "error": error,
        "answer": answer,
        "answer_words": words,
        "answer_chars": len(answer),
        "sql": sql,
        "row_count": row_count,
        "data_len": data_len,
        "flags": sorted(set(flags)),
        "score": score,
    }


def _fmt_pct(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "0.0%"
    return f"{(numerator / denominator) * 100:.1f}%"


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(results)
    errors = [r for r in results if r.get("error")]
    success = [r for r in results if not r.get("error")]

    latencies = [r["latency_s"] for r in results]
    scores = [r["score"] for r in results]
    word_counts = [r["answer_words"] for r in success if r["answer_words"] > 0]

    flag_counts = Counter()
    error_counts = Counter()
    for r in results:
        flag_counts.update(r.get("flags", []))
        if r.get("error"):
            error_counts.update([str(r["error"])])

    by_category: dict[str, dict[str, Any]] = {}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in results:
        grouped[r["category"]].append(r)

    for category, rows in grouped.items():
        c_total = len(rows)
        c_err = sum(1 for r in rows if r.get("error"))
        c_scores = [r["score"] for r in rows]
        c_words = [r["answer_words"] for r in rows if not r.get("error") and r["answer_words"] > 0]
        by_category[category] = {
            "count": c_total,
            "error_count": c_err,
            "error_rate": round(c_err / c_total, 4) if c_total else 0,
            "avg_score": round(statistics.mean(c_scores), 2) if c_scores else 0,
            "avg_words": round(statistics.mean(c_words), 1) if c_words else 0,
        }

    low_score_examples = sorted(results, key=lambda r: (r["score"], -r["latency_s"]))[:20]

    summary = {
        "total_questions": total,
        "success_count": len(success),
        "error_count": len(errors),
        "success_rate": round(len(success) / total, 4) if total else 0,
        "latency_avg_s": round(statistics.mean(latencies), 3) if latencies else 0,
        "latency_p50_s": round(statistics.median(latencies), 3) if latencies else 0,
        "latency_p95_s": round(sorted(latencies)[int(0.95 * (len(latencies) - 1))], 3) if latencies else 0,
        "score_avg": round(statistics.mean(scores), 2) if scores else 0,
        "score_p50": round(statistics.median(scores), 2) if scores else 0,
        "word_avg": round(statistics.mean(word_counts), 1) if word_counts else 0,
        "word_p50": round(statistics.median(word_counts), 1) if word_counts else 0,
        "flag_counts": dict(flag_counts.most_common()),
        "top_errors": [{"error": k, "count": v} for k, v in error_counts.most_common(10)],
        "by_category": by_category,
        "low_score_examples": low_score_examples,
    }
    return summary


def build_recommendations(summary: dict[str, Any]) -> list[str]:
    recs: list[str] = []
    flags = summary.get("flag_counts", {})
    error_rate = 1 - float(summary.get("success_rate", 0))

    if error_rate >= 0.12:
        recs.append(
            "Stability: increase SQL repair robustness (raise SQL_REPAIR_ATTEMPTS and add a stricter SQL-repair prompt that enforces complete CTE blocks)."
        )

    if flags.get("parser_error", 0) >= 5:
        recs.append(
            "SQL correctness: add a pre-execution SQL linter/validator step and retry with corrected SQL before failing requests."
        )

    if flags.get("ranking_too_few_rows", 0) >= 8:
        recs.append(
            "Ranking quality: strengthen prompt rules for implicit ranking to always return at least top 10 unless user explicitly asks for top 1."
        )

    if flags.get("too_short", 0) + flags.get("short", 0) >= 15:
        recs.append(
            "Answer depth: raise minimum word floor in formatter for analytical questions and enforce a section structure (finding, evidence, implication, caveat)."
        )

    if flags.get("no_data_answer", 0) >= 10:
        recs.append(
            "Coverage fallback: when a query returns no rows, auto-attempt a broader fallback query (relax filter/year/geo level) and explain the fallback used."
        )

    if flags.get("slow_response", 0) >= 12:
        recs.append(
            "Latency: route first-pass SQL generation to a faster model and reserve reasoner model only for repair attempts."
        )

    if not recs:
        recs.append("No severe systemic failures detected. Focus on iterative prompt tuning using low-score examples.")

    return recs


def write_report(
    report_path: Path,
    questions_path: Path,
    results_path: Path,
    summary: dict[str, Any],
    recommendations: list[str],
) -> None:
    lines: list[str] = []
    lines.append("# 150-Question Async Benchmark Report")
    lines.append("")
    lines.append(f"- Questions: `{questions_path}`")
    lines.append(f"- Results: `{results_path}`")
    lines.append(f"- Total questions: **{summary['total_questions']}**")
    lines.append(f"- Success rate: **{summary['success_rate'] * 100:.1f}%** ({summary['success_count']}/{summary['total_questions']})")
    lines.append(f"- Average score: **{summary['score_avg']:.1f}/100**")
    lines.append(f"- Average answer length: **{summary['word_avg']:.1f} words**")
    lines.append(f"- Latency (avg / p50 / p95): **{summary['latency_avg_s']}s / {summary['latency_p50_s']}s / {summary['latency_p95_s']}s**")
    lines.append("")

    lines.append("## Category Breakdown")
    lines.append("")
    lines.append("| Category | Count | Error Rate | Avg Score | Avg Words |")
    lines.append("|---|---:|---:|---:|---:|")
    for category, stats in sorted(summary["by_category"].items()):
        lines.append(
            f"| {category} | {stats['count']} | {stats['error_rate'] * 100:.1f}% | {stats['avg_score']:.1f} | {stats['avg_words']:.1f} |"
        )
    lines.append("")

    lines.append("## Key Problem Signals")
    lines.append("")
    for name, count in sorted(summary["flag_counts"].items(), key=lambda x: x[1], reverse=True)[:12]:
        lines.append(f"- `{name}`: {count} ({_fmt_pct(count, summary['total_questions'])})")
    lines.append("")

    lines.append("## Top Errors")
    lines.append("")
    if not summary["top_errors"]:
        lines.append("- No API/agent errors were recorded.")
    else:
        for item in summary["top_errors"]:
            lines.append(f"- {item['count']}x `{item['error']}`")
    lines.append("")

    lines.append("## Lowest-Scoring Examples")
    lines.append("")
    for row in summary["low_score_examples"][:15]:
        lines.append(
            f"- Q{row['id']} [{row['category']}] score={row['score']} latency={row['latency_s']}s flags={','.join(row['flags']) or 'none'}"
        )
        lines.append(f"  - Question: {row['question']}")
        if row.get("error"):
            lines.append(f"  - Error: {row['error']}")
        else:
            lines.append(f"  - Answer words: {row['answer_words']}, row_count: {row['row_count']}")
    lines.append("")

    lines.append("## Recommended Fixes")
    lines.append("")
    for i, rec in enumerate(recommendations, start=1):
        lines.append(f"{i}. {rec}")
    lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")


async def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    questions = build_question_set()

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    questions_path = out_dir / f"questions_150_{ts}.json"
    results_path = out_dir / f"results_150_{ts}.jsonl"
    summary_path = out_dir / f"summary_150_{ts}.json"
    report_path = out_dir / f"report_150_{ts}.md"

    questions_path.write_text(json.dumps(questions, indent=2), encoding="utf-8")

    sem = asyncio.Semaphore(args.concurrency)
    timeout = httpx.Timeout(args.timeout_s)
    limits = httpx.Limits(max_keepalive_connections=args.concurrency * 2, max_connections=args.concurrency * 3)

    print(f"Running {len(questions)} questions against {args.base_url} with concurrency={args.concurrency}")

    async with httpx.AsyncClient(base_url=args.base_url.rstrip("/"), timeout=timeout, limits=limits) as client:
        tasks = [asyncio.create_task(ask_one(client, sem, item)) for item in questions]

        done_results: list[dict[str, Any]] = []
        completed = 0
        started = time.perf_counter()

        for coro in asyncio.as_completed(tasks):
            result = await coro
            done_results.append(result)
            completed += 1

            if completed % 10 == 0 or completed == len(questions):
                elapsed = time.perf_counter() - started
                avg_per_q = elapsed / completed
                remaining = len(questions) - completed
                eta = avg_per_q * remaining
                print(f"Progress: {completed}/{len(questions)} | elapsed={elapsed:.1f}s | eta={eta:.1f}s")

    done_results.sort(key=lambda r: r["id"])

    with results_path.open("w", encoding="utf-8") as f:
        for row in done_results:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = summarize(done_results)
    recommendations = build_recommendations(summary)

    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_report(report_path, questions_path, results_path, summary, recommendations)

    print("Done.")
    print(f"Questions: {questions_path}")
    print(f"Results:   {results_path}")
    print(f"Summary:   {summary_path}")
    print(f"Report:    {report_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run 150 async benchmark questions against MOP agent.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Base URL for FastAPI service")
    parser.add_argument("--concurrency", type=int, default=4, help="Async concurrency")
    parser.add_argument("--timeout-s", type=float, default=180.0, help="Request timeout seconds")
    parser.add_argument(
        "--output-dir",
        default="reports/qa_eval",
        help="Directory for benchmark outputs",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
