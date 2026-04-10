#!/usr/bin/env python
"""
Step 1 — Generate synthetic source data.
Run once; all three formats load from the same Parquet files.

Usage:
    uv run python scripts/01_generate_data.py
    uv run python scripts/01_generate_data.py --force   # regenerate even if files exist
"""

import typer
from benchmarks.data_gen import generate_all

app = typer.Typer(add_completion=False)


@app.command()
def main(force: bool = typer.Option(False, "--force", help="Overwrite existing files")) -> None:
    generate_all(force=force)


if __name__ == "__main__":
    app()
