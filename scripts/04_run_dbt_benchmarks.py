#!/usr/bin/env python
"""
Step 4 — Run dbt models against each format and record elapsed time per model.

This script shells out to `dbt run` for each format target, captures per-model
timing from dbt's JSON output, and writes results to results/dbt_benchmark_results.csv.

Usage:
    uv run python scripts/04_run_dbt_benchmarks.py
    uv run python scripts/04_run_dbt_benchmarks.py --formats ducklake delta
"""

import csv
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from benchmarks.config import DBT_RESULTS_CSV, RESULTS_DIR, STORAGE_DIR

console = Console()
app = typer.Typer(add_completion=False)

FORMAT_CHOICES = ["ducklake", "delta", "iceberg"]
DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_project"


@app.command()
def main(
    formats: Annotated[list[str], typer.Option("--formats")] = FORMAT_CHOICES,
    runs: int = typer.Option(2, "--runs", help="Number of dbt run repetitions per format"),
    fresh: bool = typer.Option(False, "--fresh", help="dbt build --full-refresh first"),
) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    all_rows = []

    for fmt in formats:
        console.rule(f"[bold]dbt — {fmt}[/bold]")

        env = _make_env(fmt)

        if fresh:
            _run_dbt(["dbt", "run", "--full-refresh", "--target", fmt], env)

        for run_id in range(1, runs + 1):
            console.print(f"  Run {run_id}/{runs}…")
            rows = _timed_dbt_run(fmt, run_id, env)
            all_rows.extend(rows)

    _save(all_rows)
    _print_summary(all_rows)


def _make_env(fmt: str) -> dict:
    """Inject format-specific env vars that the dbt profiles.yml reads."""
    env = os.environ.copy()
    env["STORAGE_PATH"] = str(STORAGE_DIR)
    env["DUCKLAKE_CATALOG_PATH"] = str(STORAGE_DIR / "ducklake" / "catalog.db")
    env["DUCKLAKE_DATA_PATH"] = str(STORAGE_DIR / "ducklake" / "data")
    env["DBT_FORMAT"] = fmt
    return env


def _timed_dbt_run(fmt: str, run_id: int, env: dict) -> list[dict]:
    cmd = [
        "dbt", "run",
        "--target", fmt,
        "--profiles-dir", str(DBT_PROJECT_DIR),
        "--project-dir", str(DBT_PROJECT_DIR),
        "--log-format", "json",
    ]

    t0 = time.perf_counter()
    proc = subprocess.run(
        cmd, capture_output=True, text=True, env=env, cwd=str(DBT_PROJECT_DIR)
    )
    total_elapsed = (time.perf_counter() - t0) * 1000

    rows = []
    ts = datetime.now(timezone.utc).isoformat()

    # dbt 1.9+ writes JSON log events to stderr; stdout may be empty.
    # Parse per-model timings from LogModelResult events (code Q012).
    model_times: dict[str, float] = {}
    for line in (proc.stdout + proc.stderr).splitlines():
        try:
            event = json.loads(line)
            data = event.get("data", {})
            if "execution_time" in data and "node_info" in data:
                node_name = data["node_info"].get("node_name", "")
                exec_time = data["execution_time"]
                if node_name:
                    model_times[node_name] = exec_time * 1000  # s → ms
        except (json.JSONDecodeError, KeyError):
            continue

    if model_times:
        for model_name, elapsed_ms in model_times.items():
            rows.append({
                "format": fmt,
                "model": model_name,
                "run_id": run_id,
                "elapsed_ms": round(elapsed_ms, 2),
                "total_run_ms": round(total_elapsed, 2),
                "status": "success" if proc.returncode == 0 else "error",
                "timestamp": ts,
            })
    else:
        # Fallback: record total run time only
        rows.append({
            "format": fmt,
            "model": "__total__",
            "run_id": run_id,
            "elapsed_ms": round(total_elapsed, 2),
            "total_run_ms": round(total_elapsed, 2),
            "status": "success" if proc.returncode == 0 else "error",
            "timestamp": ts,
        })

    if proc.returncode != 0:
        console.print(f"  [red]dbt run failed for {fmt}:[/red]")
        console.print(proc.stderr[-2000:])
    else:
        console.print(
            f"  [green]✓[/green] {fmt} run {run_id} — "
            f"total={total_elapsed:,.0f} ms  models={len(model_times)}"
        )

    return rows


def _run_dbt(cmd: list[str], env: dict) -> None:
    subprocess.run(cmd, env=env, cwd=str(DBT_PROJECT_DIR), check=False)


def _save(rows: list[dict]) -> None:
    if not rows:
        return
    write_header = not DBT_RESULTS_CSV.exists()
    with DBT_RESULTS_CSV.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    console.print(f"\n[dim]dbt results saved to {DBT_RESULTS_CSV}[/dim]")


def _print_summary(rows: list[dict]) -> None:
    from collections import defaultdict
    from rich.table import Table

    totals: dict[tuple, list[float]] = defaultdict(list)
    for r in rows:
        totals[(r["format"], r["model"])].append(r["elapsed_ms"])

    tbl = Table(title="dbt Benchmark Summary (avg ms)", show_lines=True)
    formats = sorted({r["format"] for r in rows})
    models = sorted({r["model"] for r in rows})

    tbl.add_column("model")
    for fmt in formats:
        tbl.add_column(fmt, justify="right")

    for model in models:
        times = [
            sum(totals.get((fmt, model), [])) / len(totals.get((fmt, model), [1]))
            if totals.get((fmt, model)) else None
            for fmt in formats
        ]
        valid = [t for t in times if t]
        best = min(valid) if valid else None
        cells = []
        for t in times:
            if t is None:
                cells.append("[dim]n/a[/dim]")
            elif best and abs(t - best) < 1:
                cells.append(f"[bold green]{t:,.1f}[/bold green]")
            else:
                cells.append(f"{t:,.1f}")
        tbl.add_row(model, *cells)

    console.print(tbl)


if __name__ == "__main__":
    app()
