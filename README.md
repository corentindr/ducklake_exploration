# DuckLake vs Delta Lake vs Apache Iceberg — Local Benchmark

A performance comparison of three open table formats on a **local filesystem**.
Built to understand each format's developer experience and query behaviour before
committing to one in a cloud data stack.

---

## What this benchmark measures (and what it does not)

### What it measures

| Category | What the numbers actually reflect |
|---|---|
| **Reads & Aggregations** | DuckDB scan performance over each format's Parquet layout. All three use the same engine (`delta_scan`, `iceberg_scan`, `lake.*`), so this is a fair apples-to-apples comparison. Differences come from file layout, compression, and extension overhead. |
| **Writes / Mutations** | Each format uses a different write library: DuckDB SQL (DuckLake), delta-rs Rust (Delta), pyiceberg Python (Iceberg). Results reflect **library performance**, not format design. |
| **Schema evolution** | Metadata-only `ADD COLUMN` cost: a SQLite write (DuckLake), a JSON log entry (Delta), a new `metadata.json` file (Iceberg). |
| **dbt models** | dbt run times using the same DuckDB engine for all three formats — comparable to the reads/aggregations results. |

### What it does not measure

DuckLake's primary architectural differentiator is its **SQL-native catalog**
(SQLite locally, Postgres/MySQL in production). On local disk this advantage is
invisible because filesystem reads are fast regardless of format. In a cloud
object-storage environment, the following would show meaningful gaps:

- **S3 LIST elimination** — DuckLake resolves file lists with a single SQL index
  lookup. Delta replays a JSON transaction log and Iceberg chains
  `metadata.json → manifest-list.avro → manifest.avro` (3+ round trips per query).
- **Metadata latency at scale** — With thousands of snapshots or partitions the
  log replay / manifest chain cost compounds. DuckLake scales the catalog with
  SQL indexes.
- **Concurrent write correctness** — DuckLake uses database-level transactions
  for atomic catalog + data commits. Hard to test locally with a single writer.
- **Object-store rate limiting** — S3 LIST calls count against API quotas;
  DuckLake makes zero LIST calls for reads.

---

## Dataset

| Table | Rows | Compressed size |
|---|---|---|
| `orders` | 50,000,000 | ~928 MB |
| `customers` | 500,000 | ~2 MB |
| `products` | 50,000 | ~0.5 MB |
| `orders_merge_batch` | 500,000 | ~5 MB |

All tables are generated as ZSTD-compressed Parquet files and loaded once into
each format. The merge batch is 80% updates to existing orders + 20% new inserts.

---

## Setup

### Requirements

- Python >= 3.11
- [uv](https://docs.astral.sh/uv/) (`curl -Lsf https://astral.sh/uv/install.sh | sh`)
- ~10 GB free disk space
- Internet access on first run (DuckDB auto-installs `ducklake`, `delta`, `iceberg` extensions)

### Install

```bash
uv sync
uv run dbt deps --project-dir dbt_project --profiles-dir dbt_project
```

### Run the full pipeline

```bash
# 1. Generate source Parquet data (run once, ~1 min)
uv run python scripts/01_generate_data.py

# 2. Load data into each format (run once, ~5-10 min)
uv run python scripts/02_setup_formats.py

# 3. Run query benchmarks (3 warmup + timed runs per query)
uv run python scripts/03_run_benchmarks.py --fresh

# 4. Run dbt benchmarks
uv run python scripts/04_run_dbt_benchmarks.py

# 5. Open the dashboard
uv run streamlit run dashboard/app.py
```

To reset and re-run everything from scratch:

```bash
uv run python scripts/02_setup_formats.py --reset
uv run python scripts/03_run_benchmarks.py --fresh
```

---

## Project structure

```
.
├── benchmarks/
│   ├── config.py               # Row counts, paths, benchmark parameters
│   ├── data_gen.py             # Synthetic data generator (DuckDB)
│   ├── runner.py               # Warmup + timed execution engine
│   ├── formats/
│   │   ├── base.py             # FormatAdapter interface
│   │   ├── ducklake.py         # DuckLake via duckdb + ducklake extension
│   │   ├── delta.py            # Delta Lake via delta-rs + DuckDB delta_scan
│   │   └── iceberg.py          # Iceberg via pyiceberg + DuckDB iceberg_scan
│   └── queries/
│       ├── reads.py            # Full scan, filtered, point lookup
│       ├── aggregations.py     # GROUP BY, window functions, joins
│       ├── updates.py          # UPDATE, DELETE, MERGE
│       └── schema_evolution.py # ADD COLUMN + query over new column
├── scripts/
│   ├── 01_generate_data.py
│   ├── 02_setup_formats.py
│   ├── 03_run_benchmarks.py
│   └── 04_run_dbt_benchmarks.py
├── dbt_project/                # dbt models (staging + marts)
├── dashboard/app.py            # Streamlit results viewer
├── results/
│   ├── benchmark_results.csv
│   └── dbt_benchmark_results.csv
└── data/                       # Generated Parquet files (gitignored)
```

---

## Dependencies

| Package | Version | Role |
|---|---|---|
| `duckdb` | >=1.2.0 | Query engine + DuckLake extension host |
| `deltalake` | >=0.24.0 | Delta write/update/merge (delta-rs) |
| `pyiceberg[duckdb,sql-sqlite]` | >=0.8.0 | Iceberg write/update/merge |
| `pyarrow` | >=17.0.0 | Arrow interchange for Delta and Iceberg writes |
| `pandas` | >=2.0.0 | Result DataFrames |
| `streamlit` | >=1.35.0 | Dashboard |
| `plotly` | >=5.22.0 | Charts |
| `dbt-duckdb` | >=1.9.0 | dbt adapter |
| `rich` | >=13.7.0 | Terminal output |
| `typer` | >=0.12.0 | CLI |

DuckDB extensions (`ducklake`, `delta`, `iceberg`) are installed automatically
on first run by each format adapter.

---

## Known version-specific behaviour

Tested with **duckdb 1.5.1**, **pyiceberg 0.11.1**, **deltalake 0.24+**, **dbt 1.11**.

- **DuckLake ATTACH syntax**: `TYPE DUCKLAKE` in the attach options breaks
  `DATA_PATH` resolution in this extension version. Use
  `ATTACH 'ducklake:sqlite:...' AS lake (DATA_PATH '...')` without `TYPE`.
- **pyiceberg schema conversion**: `_convert_schema_to_iceberg` was removed in
  0.11. Use `_pyarrow_to_schema_without_ids` for Parquet files without embedded
  Iceberg field-ids.
- **DuckDB iceberg_scan**: pass the table directory (not the `metadata.json`
  path) and set `unsafe_enable_version_guessing = true` on the connection.
- **dbt JSON logs**: dbt 1.9+ writes JSON log events to stderr, not stdout.
  The benchmark script reads both.

---

## Future work: measuring what matters in the cloud

To properly benchmark DuckLake's catalog advantages, the next step is a cloud
deployment. Concrete things to measure:

| Experiment | What it isolates |
|---|---|
| **Cold read latency** on S3 (no caching) | Metadata hop count: 1 (DuckLake) vs 3+ (Iceberg) vs log-replay (Delta) |
| **Listing cost** on large partitioned tables | DuckLake makes zero S3 LIST calls; Delta and Iceberg scale LIST calls with partition count |
| **Time-travel query latency** | SQL `AS OF` in DuckLake vs snapshot-id resolution in Iceberg/Delta |
| **Concurrent writer throughput** | DuckLake uses DB transactions; Delta uses optimistic concurrency; Iceberg requires external locking |
| **Metadata query performance** at 1000+ snapshots | DuckLake catalog scales with SQL indexes; Iceberg manifest chain grows linearly |
| **Catalog cold-start** (first query after idle) | Number of object-store round trips before any data is read |

A minimal cloud setup would be:
1. Replace local SQLite with a Postgres catalog for DuckLake
2. Use S3 (or MinIO) as the data path for all three formats
3. Disable OS page cache between runs (`echo 3 > /proc/sys/vm/drop_caches` on Linux)
4. Run benchmarks from a separate EC2/VM to include real network latency
