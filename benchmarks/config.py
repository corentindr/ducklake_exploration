from pathlib import Path

ROOT = Path(__file__).parent.parent

DATA_DIR = ROOT / "data"
STORAGE_DIR = ROOT / "storage"
RESULTS_DIR = ROOT / "results"

# Per-format storage
DUCKLAKE_DIR = STORAGE_DIR / "ducklake"
DUCKLAKE_CATALOG = DUCKLAKE_DIR / "catalog.db"
DUCKLAKE_DATA = DUCKLAKE_DIR / "data"

DELTA_DIR = STORAGE_DIR / "delta"
DELTA_ORDERS = DELTA_DIR / "orders"
DELTA_CUSTOMERS = DELTA_DIR / "customers"
DELTA_PRODUCTS = DELTA_DIR / "products"
DELTA_ORDERS_MERGE = DELTA_DIR / "orders_merge_batch"  # staging for merge benchmark

ICEBERG_DIR = STORAGE_DIR / "iceberg"
ICEBERG_CATALOG = ICEBERG_DIR / "catalog.db"
ICEBERG_WAREHOUSE = ICEBERG_DIR / "warehouse"

# Source Parquet (generated once, shared by all formats)
ORDERS_PARQUET = DATA_DIR / "orders.parquet"
CUSTOMERS_PARQUET = DATA_DIR / "customers.parquet"
PRODUCTS_PARQUET = DATA_DIR / "products.parquet"
MERGE_BATCH_PARQUET = DATA_DIR / "orders_merge_batch.parquet"

# Scale
N_ORDERS = 50_000_000
N_CUSTOMERS = 500_000
N_PRODUCTS = 50_000
N_MERGE_BATCH = 500_000   # rows in the MERGE benchmark batch

# Benchmark execution
WARMUP_RUNS = 1
TIMED_RUNS = 3

# Update benchmark parameters
UPDATE_CUTOFF_DATE = "2020-02-01"   # UPDATE pending orders before this date
DELETE_CUTOFF_DATE = "2020-06-01"   # DELETE cancelled orders before this date

# Results
RESULTS_CSV = RESULTS_DIR / "benchmark_results.csv"
DBT_RESULTS_CSV = RESULTS_DIR / "dbt_benchmark_results.csv"
