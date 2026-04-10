#!/usr/bin/env bash
# ============================================================
# run_all.sh  —  One-command setup + full benchmark pipeline
#
# Usage:
#   ./run_all.sh              # full run (skips data gen if files exist)
#   ./run_all.sh --fresh      # wipe everything and start over
#   ./run_all.sh --no-dbt     # skip dbt benchmarks
# ============================================================

set -euo pipefail

FRESH=false
SKIP_DBT=false

for arg in "$@"; do
  case $arg in
    --fresh)  FRESH=true ;;
    --no-dbt) SKIP_DBT=true ;;
  esac
done

echo "=========================================="
echo " DuckLake vs Delta vs Iceberg Benchmark"
echo "=========================================="
echo ""

# -- Environment setup --
if ! command -v uv &> /dev/null; then
  echo "ERROR: uv not found. Install with: curl -Lsf https://astral.sh/uv/install.sh | sh"
  exit 1
fi

echo "► Installing dependencies (uv sync)…"
uv sync --quiet

# -- Step 1: Data generation --
echo ""
echo "► [1/4] Generating synthetic data…"
if [ "$FRESH" = true ]; then
  uv run python scripts/01_generate_data.py --force
else
  uv run python scripts/01_generate_data.py
fi

# -- Step 2: Format setup --
echo ""
echo "► [2/4] Loading data into all three formats…"
if [ "$FRESH" = true ]; then
  uv run python scripts/02_setup_formats.py --reset
else
  uv run python scripts/02_setup_formats.py
fi

# -- Step 3: Benchmarks --
echo ""
echo "► [3/4] Running benchmarks…"
if [ "$FRESH" = true ]; then
  uv run python scripts/03_run_benchmarks.py --fresh
else
  uv run python scripts/03_run_benchmarks.py
fi

# -- Step 4: dbt benchmarks --
if [ "$SKIP_DBT" = false ]; then
  echo ""
  echo "► [4/4] Running dbt benchmarks…"
  (cd dbt_project && uv run dbt deps --quiet 2>/dev/null || true)
  uv run python scripts/04_run_dbt_benchmarks.py
else
  echo ""
  echo "► [4/4] dbt benchmarks skipped (--no-dbt)"
fi

# -- Launch dashboard --
echo ""
echo "=========================================="
echo " All done!  Launch the dashboard with:"
echo "   uv run streamlit run dashboard/app.py"
echo "=========================================="
