#!/usr/bin/env python
"""
Step 2 — Load source Parquet data into each table format.

This is run once (or after --reset). It can take several minutes because it
writes 50M rows into each of the three format directories.

Usage:
    uv run python scripts/02_setup_formats.py
    uv run python scripts/02_setup_formats.py --formats ducklake delta  # subset
    uv run python scripts/02_setup_formats.py --reset                   # wipe & reload
"""

import shutil
import typer
from typing import Annotated
from rich.console import Console

from benchmarks.config import DUCKLAKE_DIR, DELTA_DIR, ICEBERG_DIR, ORDERS_PARQUET

console = Console()
app = typer.Typer(add_completion=False)

FORMAT_CHOICES = ["ducklake", "delta", "iceberg"]


@app.command()
def main(
    formats: Annotated[
        list[str],
        typer.Option("--formats", help=f"Formats to set up: {FORMAT_CHOICES}"),
    ] = FORMAT_CHOICES,
    reset: bool = typer.Option(False, "--reset", help="Delete existing format data first"),
) -> None:
    if not ORDERS_PARQUET.exists():
        console.print("[red]Source data not found. Run scripts/01_generate_data.py first.[/red]")
        raise typer.Exit(1)

    for fmt in formats:
        if fmt not in FORMAT_CHOICES:
            console.print(f"[red]Unknown format: {fmt}[/red]")
            raise typer.Exit(1)

    if reset:
        _dirs = {"ducklake": DUCKLAKE_DIR, "delta": DELTA_DIR, "iceberg": ICEBERG_DIR}
        for fmt in formats:
            d = _dirs[fmt]
            if d.exists():
                console.print(f"[dim]Removing {d}…[/dim]")
                shutil.rmtree(d)

    for fmt in formats:
        console.print(f"\n[bold]Setting up {fmt}…[/bold]")
        adapter = _make_adapter(fmt)
        try:
            adapter.setup()
            adapter.teardown()
            console.print(f"[green]✓ {fmt} ready[/green]")
        except Exception as exc:
            console.print(f"[red]✗ {fmt} failed: {exc}[/red]")
            raise


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
