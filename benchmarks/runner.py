"""
Benchmark runner.

For each format × category × query:
  1. Run WARMUP_RUNS times (not recorded)
  2. Run TIMED_RUNS times (record each)
Results are appended to a CSV file in results/.
"""

import csv
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from rich.console import Console
from rich.table import Table

from benchmarks.config import WARMUP_RUNS, TIMED_RUNS, RESULTS_CSV, RESULTS_DIR
from benchmarks.formats.base import FormatAdapter, QueryResult
from benchmarks.queries import ALL_BENCHMARKS

console = Console()


@dataclass
class BenchmarkRecord:
    format: str
    category: str
    query_name: str
    description: str
    run_id: int
    elapsed_ms: float
    rows: int
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def run_suite(adapter: FormatAdapter, categories: list[str] | None = None) -> list[BenchmarkRecord]:
    """
    Run all benchmarks (or a subset) for one adapter.
    Returns a flat list of BenchmarkRecord objects.
    """
    records: list[BenchmarkRecord] = []
    cats = categories or list(ALL_BENCHMARKS.keys())

    console.rule(f"[bold]{adapter.name.upper()}[/bold]")

    for cat in cats:
        benchmarks = ALL_BENCHMARKS[cat]
        console.print(f"\n[bold underline]{cat}[/bold underline]")

        for name, description, fn in benchmarks:
            # Warmup
            for _ in range(WARMUP_RUNS):
                try:
                    fn(adapter)
                except Exception as exc:
                    console.print(f"  [red]WARMUP ERROR[/red] {name}: {exc}")
                    break

            # Timed runs
            run_times: list[float] = []
            run_rows: list[int] = []
            for run_id in range(1, TIMED_RUNS + 1):
                try:
                    result: QueryResult = fn(adapter)
                    run_times.append(result.elapsed_ms)
                    run_rows.append(result.rows)
                    records.append(BenchmarkRecord(
                        format=adapter.name,
                        category=cat,
                        query_name=name,
                        description=description,
                        run_id=run_id,
                        elapsed_ms=result.elapsed_ms,
                        rows=result.rows,
                    ))
                except Exception as exc:
                    console.print(f"  [red]ERROR[/red] {name} run {run_id}: {exc}")

            if run_times:
                avg = sum(run_times) / len(run_times)
                console.print(
                    f"  [green]✓[/green] {name:<35} "
                    f"avg={avg:>8.1f} ms   rows={run_rows[0]:>10,}"
                )

    return records


def save_records(records: list[BenchmarkRecord]) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    write_header = not RESULTS_CSV.exists()

    with RESULTS_CSV.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=BenchmarkRecord.__dataclass_fields__.keys())
        if write_header:
            writer.writeheader()
        for r in records:
            writer.writerow(asdict(r))

    console.print(f"\n[dim]Results appended to {RESULTS_CSV}[/dim]")


def print_summary(records: list[BenchmarkRecord]) -> None:
    """Print a quick comparison table: formats as columns, queries as rows."""
    # Collect average per (category, query_name, format)
    from collections import defaultdict

    totals: dict[tuple, list[float]] = defaultdict(list)
    for r in records:
        totals[(r.category, r.query_name, r.format)].append(r.elapsed_ms)

    averages: dict[tuple, float] = {k: sum(v) / len(v) for k, v in totals.items()}

    formats = sorted({r.format for r in records})
    queries = sorted({(r.category, r.query_name) for r in records})

    tbl = Table(title="Benchmark Summary (avg ms)", show_lines=True)
    tbl.add_column("category", style="dim")
    tbl.add_column("query")
    for fmt in formats:
        tbl.add_column(fmt, justify="right")

    for cat, qname in queries:
        cells = []
        times = [averages.get((cat, qname, fmt)) for fmt in formats]
        valid = [t for t in times if t is not None]
        best = min(valid) if valid else None

        for t in times:
            if t is None:
                cells.append("[dim]n/a[/dim]")
            elif best and t == best:
                cells.append(f"[bold green]{t:,.1f}[/bold green]")
            else:
                cells.append(f"{t:,.1f}")

        tbl.add_row(cat, qname, *cells)

    console.print(tbl)
