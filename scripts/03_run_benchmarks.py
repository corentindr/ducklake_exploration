#!/usr/bin/env python
"""
Step 3 — Run all benchmarks and save results to results/benchmark_results.csv.

Usage:
    uv run python scripts/03_run_benchmarks.py
    uv run python scripts/03_run_benchmarks.py --formats ducklake delta
    uv run python scripts/03_run_benchmarks.py --categories reads aggregations
    uv run python scripts/03_run_benchmarks.py --fresh   # clear previous results first
"""

import typer
from typing import Annotated
from rich.console import Console

from benchmarks.config import RESULTS_CSV
from benchmarks.runner import run_suite, save_records, print_summary

console = Console()
app = typer.Typer(add_completion=False)

FORMAT_CHOICES = ["ducklake", "delta", "iceberg"]
CATEGORY_CHOICES = ["reads", "aggregations", "updates", "schema_evolution"]


@app.command()
def main(
    formats: Annotated[
        list[str],
        typer.Option("--formats"),
    ] = FORMAT_CHOICES,
    categories: Annotated[
        list[str],
        typer.Option("--categories"),
    ] = CATEGORY_CHOICES,
    fresh: bool = typer.Option(False, "--fresh", help="Delete previous results before running"),
) -> None:
    if fresh and RESULTS_CSV.exists():
        RESULTS_CSV.unlink()
        console.print(f"[dim]Cleared {RESULTS_CSV}[/dim]")

    all_records = []

    for fmt in formats:
        adapter = _make_adapter(fmt)
        try:
            adapter.setup()
            records = run_suite(adapter, categories=categories)
            all_records.extend(records)
            save_records(records)
        except Exception as exc:
            console.print(f"[red]Error in {fmt}: {exc}[/red]")
            raise
        finally:
            adapter.teardown()

    print_summary(all_records)


def _make_adapter(fmt: str):
    if fmt == "ducklake":
        from benchmarks.formats.ducklake import DuckLakeAdapter
        return DuckLakeAdapter()
    if fmt == "delta":
        from benchmarks.formats.delta import DeltaAdapter
        return DeltaAdapter()
    if fmt == "iceberg":
        from benchmarks.formats.iceberg import IcebergAdapter
        return IcebergAdapter()
    raise ValueError(fmt)


if __name__ == "__main__":
    app()
