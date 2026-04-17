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

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent

import sys

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from app.agent import ask_agent
from benchmark_goldset_async import build_goldset_question_set
from benchmark_qa_async import build_question_set, build_recommendations, summarize


load_dotenv(ROOT_DIR / ".env")

RANK_KEYWORDS = ("highest", "lowest", "most", "least", "top", "bottom", "rank", "leading")


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
    return None


def build_stress_question_set() -> list[dict[str, Any]]:
    return [
        {"category": "stress_routing", "question": "Which state has the highest total liabilities?"},
        {"category": "stress_routing", "question": "highest assets county in california?"},
        {"category": "stress_routing", "question": "Which counties in Maryland have the highest total liabilities?"},
        {"category": "stress_routing", "question": "Which Maryland congressional district has the highest Free_Cash_Flow?"},
        {"category": "stress_routing", "question": "Compare Maryland and Virginia on total liabilities, revenue, and debt ratio."},
        {"category": "stress_routing", "question": "Which state has the highest debt ratio in Government Finances?"},
        {"category": "stress_routing", "question": "Which state has the highest current ratio in Government Finances?"},
        {"category": "stress_metadata", "question": "What years are available in Government Finances?"},
        {"category": "stress_metadata", "question": "What years are available in FINRA county data?"},
        {"category": "stress_metadata", "question": "What does cd_118 mean in these tables?"},
        {"category": "stress_metadata", "question": "Are all year fields numeric in the project?"},
        {"category": "stress_metadata", "question": "Should the chatbot automatically recompute Per 1000 and per-capita fields?"},
        {"category": "stress_spending", "question": "Which agencies account for the most spending in Maryland?"},
        {"category": "stress_spending", "question": "Which agencies account for the most direct payments in Maryland?"},
        {"category": "stress_spending", "question": "How much federal money goes to Maryland?"},
        {"category": "stress_spending", "question": "Which states receive the most federal contracts in 2024?"},
        {"category": "stress_spending", "question": "Which states receive the most grants per 1000 in 2024?"},
        {"category": "stress_spending", "question": "Which Maryland congressional districts received the most grants in 2024?"},
        {"category": "stress_finra", "question": "Which counties in Maryland have the highest financial literacy?"},
        {"category": "stress_finra", "question": "Compare Maryland and Virginia on financial literacy in 2021."},
        {"category": "stress_finra", "question": "How has Maryland changed over time on financial literacy?"},
        {"category": "stress_flow", "question": "Which states send the most subcontract inflow into Maryland?"},
        {"category": "stress_flow", "question": "Which agencies dominate Maryland inflows?"},
        {"category": "stress_flow", "question": "Which industries dominate Maryland inflows?"},
        {"category": "stress_flow", "question": "What is the biggest displayed flow involving Maryland?"},
        {"category": "stress_flow", "question": "How much internal flow does Maryland have, and is it shown on the map?"},
        {"category": "stress_flow", "question": "What is the difference between total flows and displayed flows?"},
        {"category": "stress_cross", "question": "Do states with higher financial literacy tend to have lower debt ratios?"},
        {"category": "stress_cross", "question": "Do states with higher direct payments per 1000 also have higher poverty rates?"},
        {"category": "stress_cross", "question": "Compare Maryland and Virginia on federal contracts in 2024, median household income in 2023, and financial literacy in 2021."},
        {"category": "stress_cross", "question": "Which states are top-10 in both federal contracts and total liabilities?"},
        {"category": "stress_unsupported", "question": "What was Maryland liabilities in 2021?"},
        {"category": "stress_unsupported", "question": "What was county-level FINRA financial literacy in Maryland in 2018?"},
        {"category": "stress_unsupported", "question": "What were agency-level county spending values in 2017?"},
        {"category": "stress_ambiguity", "question": "Which state is most dependent on federal money?"},
        {"category": "stress_ambiguity", "question": "Which state is strongest economically?"},
        {"category": "stress_ambiguity", "question": "What is the biggest funding source in Maryland?"},
        {"category": "stress_ambiguity", "question": "Can you add Employees to Contracts to create a single impact score?"},
        {"category": "stress_ambiguity", "question": "Do direct payments cause lower poverty?"},
        {"category": "stress_ambiguity", "question": "Rank Maryland districts by a custom score combining grants, financial literacy, and bachelor's attainment."},
        {
            "category": "stress_followup",
            "question": "Where does Mississippi stand?",
            "history": [
                {"role": "user", "content": "Which state has the highest total liabilities?"},
                {"role": "assistant", "content": "California has the highest total liabilities in FY2023 government finance data."},
            ],
        },
        {
            "category": "stress_followup",
            "question": "How about Virginia?",
            "history": [
                {"role": "user", "content": "How much federal money goes to Maryland?"},
                {"role": "assistant", "content": "Maryland receives about $104.27B using the dashboard default of Contracts + Grants + Resident Wage."},
            ],
        },
        {
            "category": "stress_followup",
            "question": "What about direct payments instead?",
            "history": [
                {"role": "user", "content": "Which agencies account for the most spending in Maryland?"},
                {"role": "assistant", "content": "Under the default spending definition, HHS leads in Maryland."},
            ],
        },
        {
            "category": "stress_followup",
            "question": "What about outflow?",
            "history": [
                {"role": "user", "content": "Which states send the most subcontract inflow into Maryland?"},
                {"role": "assistant", "content": "Virginia sends the most subcontract inflow into Maryland."},
            ],
        },
        {
            "category": "stress_followup",
            "question": "How about Virginia?",
            "history": [
                {"role": "user", "content": "Which counties in Maryland have the highest total liabilities?"},
                {"role": "assistant", "content": "Baltimore City leads Maryland counties on total liabilities."},
            ],
        },
        {
            "category": "stress_followup",
            "question": "And Texas?",
            "history": [
                {"role": "user", "content": "Compare Maryland and Virginia on financial literacy in 2021."},
                {"role": "assistant", "content": "Virginia is slightly above Maryland on financial literacy in 2021."},
            ],
        },
        {
            "category": "stress_followup",
            "question": "And the lowest?",
            "history": [
                {"role": "user", "content": "Which state has the highest debt ratio?"},
                {"role": "assistant", "content": "Illinois has the highest debt ratio in the FY2023 government finance file."},
            ],
        },
        {
            "category": "stress_followup",
            "question": "Use per 1000 instead.",
            "history": [
                {"role": "user", "content": "Which states receive the most federal contracts in 2024?"},
                {"role": "assistant", "content": "Virginia leads states on raw federal contracts in 2024."},
            ],
        },
        {
            "category": "stress_followup",
            "question": "What about contracts?",
            "history": [
                {"role": "user", "content": "Which Maryland congressional districts received the most grants in 2024?"},
                {"role": "assistant", "content": "One Maryland district leads on grants in the processed 2024 federal spending file."},
            ],
        },
        {
            "category": "stress_followup",
            "question": "Zoom into the leader.",
            "history": [
                {"role": "user", "content": "Show county-level poverty across Texas."},
                {"role": "assistant", "content": "The county map can show poverty distribution across Texas in 2023."},
            ],
        },
    ]


def build_combined_question_set() -> list[dict[str, Any]]:
    combined: list[dict[str, Any]] = []
    for source, items in (
        ("gold300", build_goldset_question_set()),
        ("qa150", build_question_set()),
        ("stress50", build_stress_question_set()),
    ):
        for item in items:
            combined.append(
                {
                    "source": source,
                    "category": item["category"],
                    "question": item["question"],
                    "history": item.get("history", []),
                }
            )

    if len(combined) != 500:
        raise RuntimeError(f"Expected 500 questions, got {len(combined)}")

    for idx, item in enumerate(combined, start=1):
        item["id"] = idx
    return combined


def score_result(item: dict[str, Any], result: dict[str, Any], elapsed_s: float) -> dict[str, Any]:
    error = result.get("error")
    answer = result.get("answer") or ""
    sql = result.get("sql")
    row_count = int(result.get("row_count") or 0)
    data = result.get("data") or []
    data_len = len(data) if isinstance(data, list) else 0
    words = _word_count(answer)

    flags: list[str] = []
    if error:
        flags.append("error")
        msg = str(error).lower()
        if "parser error" in msg:
            flags.append("parser_error")
        if "timeout" in msg:
            flags.append("timeout")
        if "rate" in msg and "limit" in msg:
            flags.append("rate_limit")

    no_data_tokens = (
        "no data found",
        "not available in the current dataset",
        "data_not_available",
    )
    if any(t in answer.lower() for t in no_data_tokens):
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

    if elapsed_s > 45:
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
        "source": item["source"],
        "category": item["category"],
        "question": item["question"],
        "history_len": len(item.get("history", [])),
        "latency_s": round(elapsed_s, 3),
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


async def ask_one_direct(sem: asyncio.Semaphore, item: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    async with sem:
        result = await asyncio.to_thread(ask_agent, item["question"], item.get("history", []))
    elapsed = time.perf_counter() - started
    return score_result(item, result if isinstance(result, dict) else {"error": "INVALID_AGENT_RESULT"}, elapsed)


def add_extended_summary(summary: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in results:
        by_source[row["source"]].append(row)

    source_breakdown: dict[str, Any] = {}
    for source, rows in by_source.items():
        source_summary = summarize(rows)
        source_breakdown[source] = {
            "count": len(rows),
            "success_rate": source_summary["success_rate"],
            "score_avg": source_summary["score_avg"],
            "word_avg": source_summary["word_avg"],
            "latency_avg_s": source_summary["latency_avg_s"],
            "error_count": source_summary["error_count"],
        }

    long_answers = [r for r in results if r["answer_words"] >= 220 and not r.get("error")]
    medium_answers = [r for r in results if 120 <= r["answer_words"] < 220 and not r.get("error")]
    short_answers = [r for r in results if r["answer_words"] < 120 and not r.get("error")]

    def avg_score(rows: list[dict[str, Any]]) -> float:
        return round(statistics.mean(r["score"] for r in rows), 2) if rows else 0

    summary["source_breakdown"] = source_breakdown
    summary["length_bands"] = {
        "short_lt_120": {"count": len(short_answers), "avg_score": avg_score(short_answers)},
        "medium_120_219": {"count": len(medium_answers), "avg_score": avg_score(medium_answers)},
        "long_ge_220": {"count": len(long_answers), "avg_score": avg_score(long_answers)},
    }
    history_rows = [r for r in results if r["history_len"] > 0]
    summary["history_cases"] = {
        "count": len(history_rows),
        "avg_score": avg_score(history_rows),
        "error_count": sum(1 for r in history_rows if r.get("error")),
    }
    summary["top_recommendations"] = build_recommendations(summary)
    return summary


def write_report(
    report_path: Path,
    questions_path: Path,
    results_path: Path,
    summary: dict[str, Any],
    results: list[dict[str, Any]],
) -> None:
    lines: list[str] = []
    lines.append("# DeepSeek Direct 500-Query Benchmark Report")
    lines.append("")
    lines.append(f"- Questions: `{questions_path}`")
    lines.append(f"- Results: `{results_path}`")
    lines.append(f"- Total questions: **{summary['total_questions']}**")
    lines.append(f"- Success rate: **{summary['success_rate'] * 100:.1f}%** ({summary['success_count']}/{summary['total_questions']})")
    lines.append(f"- Average score: **{summary['score_avg']:.1f}/100**")
    lines.append(f"- Average answer length: **{summary['word_avg']:.1f} words**")
    lines.append(f"- Latency (avg / p50 / p95): **{summary['latency_avg_s']}s / {summary['latency_p50_s']}s / {summary['latency_p95_s']}s**")
    lines.append("")
    lines.append("## Source Breakdown")
    lines.append("")
    lines.append("| Source | Count | Success Rate | Avg Score | Avg Words | Avg Latency | Errors |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for source, stats in sorted(summary["source_breakdown"].items()):
        lines.append(
            f"| {source} | {stats['count']} | {stats['success_rate'] * 100:.1f}% | {stats['score_avg']:.1f} | {stats['word_avg']:.1f} | {stats['latency_avg_s']:.2f}s | {stats['error_count']} |"
        )
    lines.append("")
    lines.append("## Flag Counts")
    lines.append("")
    for name, count in sorted(summary["flag_counts"].items(), key=lambda x: x[1], reverse=True):
        lines.append(f"- `{name}`: {count}")
    lines.append("")
    lines.append("## Answer Length Bands")
    lines.append("")
    for band, stats in summary["length_bands"].items():
        lines.append(f"- `{band}`: {stats['count']} questions, avg score {stats['avg_score']}")
    lines.append("")
    lines.append("## History / Follow-up Slice")
    lines.append("")
    lines.append(f"- History-backed questions: **{summary['history_cases']['count']}**")
    lines.append(f"- Avg score on history-backed questions: **{summary['history_cases']['avg_score']}**")
    lines.append(f"- Errors on history-backed questions: **{summary['history_cases']['error_count']}**")
    lines.append("")
    lines.append("## Lowest-Scoring 25")
    lines.append("")
    for row in sorted(results, key=lambda r: (r["score"], -r["latency_s"]))[:25]:
        lines.append(
            f"- Q{row['id']} [{row['source']} / {row['category']}] score={row['score']} latency={row['latency_s']}s flags={','.join(row['flags']) or 'none'}"
        )
        lines.append(f"  - Question: {row['question']}")
        if row.get("error"):
            lines.append(f"  - Error: {row['error']}")
        else:
            lines.append(f"  - Answer words: {row['answer_words']}, row_count: {row['row_count']}")
    lines.append("")
    lines.append("## Recommendations")
    lines.append("")
    for idx, recommendation in enumerate(summary["top_recommendations"], start=1):
        lines.append(f"{idx}. {recommendation}")
    report_path.write_text("\n".join(lines), encoding="utf-8")


async def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    questions = build_combined_question_set()

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    questions_path = out_dir / f"questions_500_{ts}.json"
    results_path = out_dir / f"results_500_{ts}.jsonl"
    summary_path = out_dir / f"summary_500_{ts}.json"
    report_path = out_dir / f"report_500_{ts}.md"

    questions_path.write_text(json.dumps(questions, indent=2), encoding="utf-8")

    sem = asyncio.Semaphore(args.concurrency)
    tasks = [asyncio.create_task(ask_one_direct(sem, item)) for item in questions]

    done_results: list[dict[str, Any]] = []
    started = time.perf_counter()
    print(f"Running {len(questions)} DeepSeek-backed direct agent queries with concurrency={args.concurrency}", flush=True)

    for idx, coro in enumerate(asyncio.as_completed(tasks), start=1):
        result = await coro
        done_results.append(result)
        if idx % 25 == 0 or idx == len(tasks):
            elapsed = time.perf_counter() - started
            avg = elapsed / idx
            eta = avg * (len(tasks) - idx)
            print(f"Progress: {idx}/{len(tasks)} | elapsed={elapsed:.1f}s | eta={eta:.1f}s", flush=True)

    done_results.sort(key=lambda r: r["id"])

    with results_path.open("w", encoding="utf-8") as handle:
        for row in done_results:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = summarize(done_results)
    summary = add_extended_summary(summary, done_results)

    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_report(report_path, questions_path, results_path, summary, done_results)

    print("Done.", flush=True)
    print(f"Questions: {questions_path}", flush=True)
    print(f"Results:   {results_path}", flush=True)
    print(f"Summary:   {summary_path}", flush=True)
    print(f"Report:    {report_path}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a 500-query DeepSeek-backed direct benchmark against the MOP agent.")
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrent ask_agent calls")
    parser.add_argument("--output-dir", default="reports/deepseek_500_eval", help="Directory for benchmark outputs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
