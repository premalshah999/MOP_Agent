from __future__ import annotations

import argparse
import asyncio
import json
import sys
import statistics
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from benchmark_qa_async import ask_one, build_recommendations, summarize


def _add_question(out: list[dict[str, Any]], seen: set[str], category: str, question: str) -> None:
    key = " ".join(question.strip().lower().split())
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


def build_goldset_question_set() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    states_12 = [
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
        "Ohio",
        "Colorado",
    ]
    states_10 = states_12[:10]
    states_8 = states_12[:8]
    states_6 = states_12[:6]
    states_5 = states_12[:5]

    gov_metrics = [
        "total liabilities per capita",
        "debt ratio",
        "current ratio",
        "revenue per capita",
        "expenses per capita",
        "net position per capita",
        "free cash flow per capita",
        "net pension liability per capita",
        "net OPEB liability per capita",
        "long-term debt per capita",
    ]
    for metric in gov_metrics:
        _add_question(out, seen, "government_finance", f"Which 15 states have the highest {metric}?")
        _add_question(out, seen, "government_finance", f"Which 15 states have the lowest {metric}?")

    for state in states_10:
        _add_question(
            out,
            seen,
            "government_finance",
            f"Compare {state}'s total liabilities per capita to the national average and median.",
        )
        _add_question(
            out,
            seen,
            "government_finance",
            f"How does {state} compare with the national average on debt ratio?",
        )

    for state in states_5:
        _add_question(
            out,
            seen,
            "government_finance",
            f"Where does {state} rank nationally for debt ratio, and what are the top 10 and bottom 10?",
        )
        _add_question(
            out,
            seen,
            "government_finance",
            f"Where does {state} rank nationally for total liabilities per capita, and what are the top 10 and bottom 10?",
        )

    for state in states_6:
        for metric in ["debt ratio", "current ratio", "total liabilities per capita"]:
            _add_question(
                out,
                seen,
                "government_finance",
                f"Which 15 counties in {state} have the highest {metric}?",
            )

    for metric in ["debt ratio", "current ratio", "total liabilities per capita"]:
        _add_question(
            out,
            seen,
            "government_finance",
            f"Which 20 congressional districts have the highest {metric}?",
        )
        _add_question(
            out,
            seen,
            "government_finance",
            f"Which 20 congressional districts have the lowest {metric}?",
        )

    for state in states_5:
        _add_question(
            out,
            seen,
            "government_finance",
            f"Compare {state}'s 3 highest-liability counties to the state average (liabilities per capita).",
        )
        _add_question(
            out,
            seen,
            "government_finance",
            f"Compare {state}'s 3 highest-liability congressional districts to the state average (liabilities per capita).",
        )

    _add_question(
        out,
        seen,
        "government_finance",
        "Which 10 states have the largest gap between revenue per capita and expenditure per capita?",
    )
    _add_question(
        out,
        seen,
        "government_finance",
        "Which 10 states have the smallest gap between revenue per capita and expenditure per capita?",
    )
    _add_question(
        out,
        seen,
        "government_finance",
        "Which 10 Texas counties have the largest gap between revenue per capita and expenditure per capita?",
    )
    _add_question(
        out,
        seen,
        "government_finance",
        "Which 10 California counties have the largest gap between revenue per capita and expenditure per capita?",
    )
    _add_question(
        out,
        seen,
        "government_finance",
        "Which 10 congressional districts have the largest gap between revenue per capita and expenditure per capita?",
    )
    _add_question(
        out,
        seen,
        "government_finance",
        "Which 10 congressional districts have the smallest gap between revenue per capita and expenditure per capita?",
    )

    for metric in ["poverty rates", "median household income"]:
        plain_metric = "below poverty" if metric == "poverty rates" else metric
        _add_question(out, seen, "acs", f"Which 15 states have the highest {metric} in 2023?")
        _add_question(out, seen, "acs", f"Which 15 states have the lowest {metric} in 2023?")
        for state in states_10:
            _add_question(
                out,
                seen,
                "acs",
                f"Compare {state}'s {plain_metric} to the national average and median in 2023.",
            )

    for state in states_12:
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

    _add_question(out, seen, "acs", "Which 20 congressional districts have the highest poverty rates in 2023?")
    _add_question(out, seen, "acs", "Which 20 congressional districts have the lowest poverty rates in 2023?")
    _add_question(out, seen, "acs", "Which 20 congressional districts have the highest median household income in 2023?")
    _add_question(out, seen, "acs", "Which 20 congressional districts have the lowest median household income in 2023?")

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

    for state in states_10:
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

    for state in states_8:
        _add_question(
            out,
            seen,
            "finra",
            f"Which 20 counties in {state} have the highest financial literacy in 2021?",
        )
        _add_question(
            out,
            seen,
            "finra",
            f"Which 20 counties in {state} have the highest financial constraint in 2021?",
        )

    _add_question(
        out,
        seen,
        "finra",
        "Which congressional districts have high financial literacy but low financial constraint in 2021?",
    )
    _add_question(
        out,
        seen,
        "finra",
        "Is financial literacy positively related to satisfaction across states in 2021?",
    )
    _add_question(out, seen, "finra", "Which 20 congressional districts have the highest financial literacy in 2021?")
    _add_question(out, seen, "finra", "Which 20 congressional districts have the highest financial constraint in 2021?")

    spending_metrics = ["contracts", "grants", "direct payments", "resident wage"]
    for metric in spending_metrics:
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
            f"Which 15 states received the least federal {metric} in 2024?",
        )
        _add_question(
            out,
            seen,
            "federal_spending",
            f"Which 15 states received the most federal {metric} per 1000 in 2024?",
        )
        _add_question(
            out,
            seen,
            "federal_spending",
            f"Which 15 states received the least federal {metric} per 1000 in 2024?",
        )

    for state in states_8:
        _add_question(
            out,
            seen,
            "federal_spending",
            f"How does {state} compare with the national average on grants per 1000 in 2024?",
        )
        _add_question(
            out,
            seen,
            "federal_spending",
            f"How does {state} compare with the national average on contracts in 2024?",
        )

    for state in states_8:
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

    for state in ["Maryland", "Virginia", "Texas", "California"]:
        _add_question(
            out,
            seen,
            "federal_spending_agency",
            f"How much did the Department of Defense spend in {state} in 2024? Break out contracts, grants, and direct payments.",
        )

    _add_question(out, seen, "federal_spending", "Which 20 congressional districts received the most grants in 2024?")
    _add_question(out, seen, "federal_spending", "Which 20 congressional districts received the most contracts per 1000 in 2024?")
    _add_question(out, seen, "federal_spending", "Which 20 Maryland counties received the most direct payments per 1000 in 2024?")
    _add_question(out, seen, "federal_spending", "Which 20 Texas counties received the most grants in 2024?")

    flow_origins = [
        "Maryland",
        "Virginia",
        "Texas",
        "California",
        "Florida",
        "New York",
        "Pennsylvania",
        "Illinois",
        "Georgia",
        "North Carolina",
    ]
    for state in flow_origins:
        _add_question(
            out,
            seen,
            "fund_flow",
            f"Which states receive the most federal subaward funding from {state}? Show top 15.",
        )
    for state in ["Maryland", "Virginia", "Texas", "California", "Florida", "New York"]:
        _add_question(
            out,
            seen,
            "fund_flow",
            f"Which states receive the smallest non-zero subaward amounts from {state}? Show bottom 10.",
        )
    for state in ["Maryland", "Virginia", "Texas", "California"]:
        _add_question(
            out,
            seen,
            "fund_flow",
            f"How have {state} outbound subaward totals changed by fiscal year?",
        )
    for state in ["Maryland", "Texas"]:
        _add_question(
            out,
            seen,
            "fund_flow",
            f"Which 20 counties receive the highest subaward amounts from {state} recipient counties?",
        )
        _add_question(
            out,
            seen,
            "fund_flow",
            f"Which 20 congressional districts receive the highest subaward amounts from {state} recipient districts?",
        )

    cross_questions = [
        "Do states with higher financial literacy tend to have lower government debt ratios? Include correlation and sample size.",
        "Do states with higher median household income have lower debt ratios?",
        "Do states with higher poverty rates receive higher grants per 1000 in 2024?",
        "Do higher federal direct payments per 1000 align with higher poverty rates across states?",
        "Do states with higher total liabilities per capita also have higher financial constraint?",
        "Compare financial literacy and poverty rates across states in 2021.",
        "Compare poverty rates and financial constraint across states in 2021. Show states where both are high.",
        "Which states are simultaneously high in grants per 1000 and current ratio?",
        "Which states are simultaneously high in total liabilities per capita and financial constraint?",
        "Which congressional districts have high poverty rates and low federal contracts per 1000?",
        "Which congressional districts have high financial literacy but low financial constraint in 2021?",
        "Do states with higher financial literacy also show lower poverty rates?",
        "Do states with higher grants per 1000 also have higher current ratios?",
        "Compare direct payments per 1000 and poverty rates across states.",
        "Are states with high debt ratios also high in financial constraint?",
        "Are states with high total liabilities per capita also high in grants per 1000?",
        "Which states have above-average poverty and above-average financial constraint?",
        "Which states have high median household income but low debt ratios?",
        "Which congressional districts combine high poverty and low grants per 1000?",
        "Do states with higher current ratios also have lower poverty rates?",
    ]
    for question in cross_questions:
        _add_question(out, seen, "cross_dataset", question)

    for state in ["Maryland", "Texas", "California", "Florida", "New York"]:
        _add_question(
            out,
            seen,
            "government_finance",
            f"How does {state} compare with the national average on current ratio?",
        )

    for state in ["Pennsylvania", "Illinois", "Georgia", "North Carolina"]:
        _add_question(
            out,
            seen,
            "federal_spending",
            f"How does {state} compare with the national average on grants per 1000 in 2024?",
        )
    for state in ["Ohio", "Colorado"]:
        _add_question(
            out,
            seen,
            "federal_spending",
            f"How does {state} compare with the national average on contracts in 2024?",
        )

    if len(out) != 300:
        raise RuntimeError(f"Expected 300 questions, built {len(out)}")

    for idx, item in enumerate(out, start=1):
        item["id"] = idx
    return out


def is_optimal(result: dict[str, Any]) -> bool:
    hard_flags = {
        "error",
        "parser_error",
        "empty_answer",
        "no_data_answer",
        "missing_sql",
        "ranking_too_few_rows",
        "too_short",
    }
    flags = set(result.get("flags", []))
    return result.get("score", 0) >= 90 and not (flags & hard_flags)


def is_strict_optimal(result: dict[str, Any]) -> bool:
    flags = set(result.get("flags", []))
    return is_optimal(result) and "short" not in flags and "slow_response" not in flags


def summarize_goldset(results: list[dict[str, Any]]) -> dict[str, Any]:
    summary = summarize(results)
    optimal_count = sum(1 for row in results if is_optimal(row))
    strict_optimal_count = sum(1 for row in results if is_strict_optimal(row))
    summary["optimal_count"] = optimal_count
    summary["optimal_rate"] = round(optimal_count / len(results), 4) if results else 0
    summary["strict_optimal_count"] = strict_optimal_count
    summary["strict_optimal_rate"] = round(strict_optimal_count / len(results), 4) if results else 0
    summary["meets_bar_280"] = optimal_count >= 280
    return summary


def write_goldset_report(
    report_path: Path,
    questions_path: Path,
    results_path: Path,
    summary: dict[str, Any],
    recommendations: list[str],
) -> None:
    lines: list[str] = []
    lines.append("# 300-Question Goldset Async Benchmark Report")
    lines.append("")
    lines.append(f"- Questions: `{questions_path}`")
    lines.append(f"- Results: `{results_path}`")
    lines.append(f"- Total questions: **{summary['total_questions']}**")
    lines.append(f"- Success rate: **{summary['success_rate'] * 100:.1f}%** ({summary['success_count']}/{summary['total_questions']})")
    lines.append(f"- Optimal count: **{summary['optimal_count']} / {summary['total_questions']}**")
    lines.append(f"- Strict optimal count: **{summary['strict_optimal_count']} / {summary['total_questions']}**")
    lines.append(f"- Meets 280 bar: **{'yes' if summary['meets_bar_280'] else 'no'}**")
    lines.append(f"- Average score: **{summary['score_avg']:.1f}/100**")
    lines.append(f"- Latency (avg / p50 / p95): **{summary['latency_avg_s']}s / {summary['latency_p50_s']}s / {summary['latency_p95_s']}s**")
    lines.append("")
    lines.append("## Flag Counts")
    lines.append("")
    for name, count in sorted(summary["flag_counts"].items(), key=lambda x: x[1], reverse=True):
        lines.append(f"- `{name}`: {count}")
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
    lines.append("## Lowest-Scoring Examples")
    lines.append("")
    for row in summary["low_score_examples"][:20]:
        lines.append(
            f"- Q{row['id']} [{row['category']}] score={row['score']} api_latency={row.get('api_latency_s')}s flags={','.join(row['flags']) or 'none'}"
        )
        lines.append(f"  - Question: {row['question']}")
        if row.get("error"):
            lines.append(f"  - Error: {row['error']}")
        else:
            lines.append(f"  - Row count: {row['row_count']}, answer words: {row['answer_words']}")
    lines.append("")
    lines.append("## Recommendations")
    lines.append("")
    for idx, recommendation in enumerate(recommendations, start=1):
        lines.append(f"{idx}. {recommendation}")
    report_path.write_text("\n".join(lines), encoding="utf-8")


async def run(args: argparse.Namespace) -> None:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    questions = build_goldset_question_set()

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    questions_path = out_dir / f"goldset_questions_300_{ts}.json"
    results_path = out_dir / f"goldset_results_300_{ts}.jsonl"
    summary_path = out_dir / f"goldset_summary_300_{ts}.json"
    report_path = out_dir / f"goldset_report_300_{ts}.md"

    questions_path.write_text(json.dumps(questions, indent=2), encoding="utf-8")

    sem = asyncio.Semaphore(args.concurrency)
    timeout = httpx.Timeout(args.timeout_s)
    limits = httpx.Limits(max_keepalive_connections=args.concurrency * 2, max_connections=args.concurrency * 3)

    print(f"Running {len(questions)} goldset questions against {args.base_url} with concurrency={args.concurrency}")

    async with httpx.AsyncClient(base_url=args.base_url.rstrip("/"), timeout=timeout, limits=limits) as client:
        tasks = [asyncio.create_task(ask_one(client, sem, item)) for item in questions]

        results: list[dict[str, Any]] = []
        completed = 0
        started = time.perf_counter()

        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            completed += 1
            if completed % 10 == 0 or completed == len(questions):
                elapsed = time.perf_counter() - started
                avg_per = elapsed / completed
                eta = avg_per * (len(questions) - completed)
                optimal_so_far = sum(1 for row in results if is_optimal(row))
                print(
                    f"Progress: {completed}/{len(questions)} | elapsed={elapsed:.1f}s | "
                    f"eta={eta:.1f}s | optimal_so_far={optimal_so_far}"
                )

    results.sort(key=lambda row: row["id"])
    with results_path.open("w", encoding="utf-8") as handle:
        for row in results:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = summarize_goldset(results)
    recommendations = build_recommendations(summary)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_goldset_report(report_path, questions_path, results_path, summary, recommendations)

    print("Done.")
    print(f"Questions: {questions_path}")
    print(f"Results:   {results_path}")
    print(f"Summary:   {summary_path}")
    print(f"Report:    {report_path}")
    print(
        f"Optimal: {summary['optimal_count']}/{summary['total_questions']} | "
        f"Strict optimal: {summary['strict_optimal_count']}/{summary['total_questions']} | "
        f"Meets 280 bar: {summary['meets_bar_280']}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a 300-question synthetic goldset benchmark against MOP agent.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Base URL for FastAPI service")
    parser.add_argument("--concurrency", type=int, default=6, help="Async concurrency")
    parser.add_argument("--timeout-s", type=float, default=180.0, help="Request timeout seconds")
    parser.add_argument("--output-dir", default="reports/qa_eval", help="Directory for benchmark outputs")
    return parser.parse_args()


def main() -> None:
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
