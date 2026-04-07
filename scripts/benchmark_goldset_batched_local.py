from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from benchmark_goldset_async import (
    build_goldset_question_set,
    is_optimal,
    summarize_goldset,
    write_goldset_report,
)
from benchmark_qa_async import ask_one, build_recommendations


def _wait_for_health(base_url: str, timeout_s: float) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with httpx.Client(base_url=base_url, timeout=1.0) as client:
                response = client.get("/health")
                if response.status_code == 200:
                    body = response.json()
                    if body.get("status") == "ok" and body.get("checks", {}).get("registered_table_count", 0) > 0:
                        return True
        except Exception:
            time.sleep(0.2)
    return False


def _start_server(host: str, port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    return subprocess.Popen(
        [
            "/usr/bin/python3",
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            host,
            "--port",
            str(port),
        ],
        cwd=ROOT_DIR,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def _stop_server(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


async def _run_batch(
    batch: list[dict[str, object]],
    base_url: str,
    concurrency: int,
    timeout_s: float,
) -> list[dict[str, object]]:
    sem = asyncio.Semaphore(concurrency)
    timeout = httpx.Timeout(timeout_s)
    limits = httpx.Limits(max_keepalive_connections=concurrency * 2, max_connections=concurrency * 3)
    async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=timeout, limits=limits) as client:
        tasks = [asyncio.create_task(ask_one(client, sem, item)) for item in batch]
        results: list[dict[str, object]] = []
        for coro in asyncio.as_completed(tasks):
            results.append(await coro)
        return results


def _chunks(items: list[dict[str, object]], batch_size: int) -> list[list[dict[str, object]]]:
    return [items[idx : idx + batch_size] for idx in range(0, len(items), batch_size)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the 300-question goldset in local batched API mode.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8010)
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--timeout-s", type=float, default=180.0)
    parser.add_argument("--startup-timeout-s", type=float, default=20.0)
    parser.add_argument("--output-dir", default="reports/qa_eval")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    questions = build_goldset_question_set()
    batches = _chunks(questions, args.batch_size)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    questions_path = out_dir / f"goldset_questions_300_{ts}.json"
    results_path = out_dir / f"goldset_results_300_batched_{ts}.jsonl"
    summary_path = out_dir / f"goldset_summary_300_batched_{ts}.json"
    report_path = out_dir / f"goldset_report_300_batched_{ts}.md"

    questions_path.write_text(json.dumps(questions, indent=2), encoding="utf-8")

    base_url = f"http://{args.host}:{args.port}"
    all_results: list[dict[str, object]] = []
    started = time.perf_counter()

    for idx, batch in enumerate(batches, start=1):
        proc = _start_server(args.host, args.port)
        try:
            if not _wait_for_health(base_url, args.startup_timeout_s):
                raise RuntimeError(f"Batch {idx}: server did not start on {base_url}")
            batch_results = asyncio.run(_run_batch(batch, base_url, args.concurrency, args.timeout_s))
            all_results.extend(batch_results)
            optimal_so_far = sum(1 for row in all_results if is_optimal(row))
            elapsed = time.perf_counter() - started
            print(
                f"Batch {idx}/{len(batches)} done | questions={len(batch)} | "
                f"cumulative={len(all_results)}/{len(questions)} | optimal_so_far={optimal_so_far} | elapsed={elapsed:.1f}s"
            )
        finally:
            _stop_server(proc)

    all_results.sort(key=lambda row: int(row["id"]))
    with results_path.open("w", encoding="utf-8") as handle:
        for row in all_results:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = summarize_goldset(all_results)
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


if __name__ == "__main__":
    main()
