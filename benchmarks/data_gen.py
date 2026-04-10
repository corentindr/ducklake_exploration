"""
Generate synthetic e-commerce data using DuckDB's vectorized engine.
All data is written as Parquet files that are then loaded into each format.
"""

from pathlib import Path
import duckdb
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from benchmarks.config import (
    DATA_DIR,
    N_ORDERS,
    N_CUSTOMERS,
    N_PRODUCTS,
    N_MERGE_BATCH,
    ORDERS_PARQUET,
    CUSTOMERS_PARQUET,
    PRODUCTS_PARQUET,
    MERGE_BATCH_PARQUET,
)

console = Console()


def generate_all(force: bool = False) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    files = {
        ORDERS_PARQUET: _gen_orders,
        CUSTOMERS_PARQUET: _gen_customers,
        PRODUCTS_PARQUET: _gen_products,
        MERGE_BATCH_PARQUET: _gen_merge_batch,
    }

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        for path, fn in files.items():
            if path.exists() and not force:
                console.print(f"[dim]Skipping {path.name} (already exists)[/dim]")
                continue
            task = progress.add_task(f"Generating {path.name}...", total=None)
            fn(path)
            progress.update(task, completed=True, description=f"[green]Done {path.name}[/green]")

    console.print(f"[bold green]Data generation complete.[/bold green]")
    _print_stats()


def _gen_orders(out: Path) -> None:
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT
                (range + 1)::BIGINT                             AS order_id,
                (random() * {N_CUSTOMERS - 1} + 1)::BIGINT     AS customer_id,
                (random() * {N_PRODUCTS - 1} + 1)::BIGINT      AS product_id,
                (DATE '2020-01-01' + (random() * 1460)::INT * INTERVAL '1' DAY)::DATE AS order_date,
                ROUND((random() * 999 + 0.01)::DOUBLE, 2)      AS amount,
                CASE (random() * 4.99)::INT
                    WHEN 0 THEN 'pending'
                    WHEN 1 THEN 'processing'
                    WHEN 2 THEN 'shipped'
                    WHEN 3 THEN 'delivered'
                    ELSE       'cancelled'
                END                                             AS status,
                CASE (random() * 4.99)::INT
                    WHEN 0 THEN 'north'
                    WHEN 1 THEN 'south'
                    WHEN 2 THEN 'east'
                    WHEN 3 THEN 'west'
                    ELSE       'central'
                END                                             AS region,
                (random() * 9 + 1)::INT                        AS quantity,
                ROUND((random() * 0.5)::DOUBLE, 2)             AS discount,
                (TIMESTAMPTZ '2020-01-01' + (random() * 126230400)::INT * INTERVAL '1' SECOND)
                                                                AS created_at
            FROM generate_series(0, {N_ORDERS - 1}) AS t(range)
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
    """)
    con.close()


def _gen_customers(out: Path) -> None:
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT
                (range + 1)::BIGINT                             AS customer_id,
                'Customer_' || (range + 1)::VARCHAR             AS name,
                'customer_' || (range + 1)::VARCHAR || '@example.com' AS email,
                (DATE '2015-01-01' + (random() * 3285)::INT * INTERVAL '1' DAY)::DATE AS signup_date,
                CASE (random() * 9.99)::INT
                    WHEN 0 THEN 'US'  WHEN 1 THEN 'UK'  WHEN 2 THEN 'DE'
                    WHEN 3 THEN 'FR'  WHEN 4 THEN 'CA'  WHEN 5 THEN 'AU'
                    WHEN 6 THEN 'JP'  WHEN 7 THEN 'BR'  WHEN 8 THEN 'IN'
                    ELSE 'MX'
                END                                             AS country,
                CASE (random() * 3.99)::INT
                    WHEN 0 THEN 'bronze'
                    WHEN 1 THEN 'silver'
                    WHEN 2 THEN 'gold'
                    ELSE       'platinum'
                END                                             AS tier
            FROM generate_series(0, {N_CUSTOMERS - 1}) AS t(range)
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()


def _gen_products(out: Path) -> None:
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT
                (range + 1)::BIGINT                             AS product_id,
                'Product_' || (range + 1)::VARCHAR             AS name,
                CASE (random() * 7.99)::INT
                    WHEN 0 THEN 'Electronics'  WHEN 1 THEN 'Clothing'
                    WHEN 2 THEN 'Food'         WHEN 3 THEN 'Books'
                    WHEN 4 THEN 'Home'         WHEN 5 THEN 'Sports'
                    WHEN 6 THEN 'Beauty'       ELSE       'Toys'
                END                                             AS category,
                ROUND((random() * 499.99 + 0.01)::DOUBLE, 2)  AS price,
                (random() * 999 + 1)::BIGINT                   AS supplier_id,
                ROUND((random() * 10 + 0.1)::DOUBLE, 3)       AS weight_kg
            FROM generate_series(0, {N_PRODUCTS - 1}) AS t(range)
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()


def _gen_merge_batch(out: Path) -> None:
    """
    Generate a MERGE benchmark batch: 80% existing order IDs (updates) +
    20% brand-new order IDs (inserts), with modified amounts/statuses.
    """
    con = duckdb.connect()
    # 400K rows that update existing orders (IDs 1..400000)
    # 100K rows that are new orders (IDs N_ORDERS+1..N_ORDERS+100001)
    con.execute(f"""
        COPY (
            SELECT * FROM (
                -- existing order updates (80%)
                SELECT
                    (range + 1)::BIGINT                         AS order_id,
                    (random() * {N_CUSTOMERS - 1} + 1)::BIGINT AS customer_id,
                    (random() * {N_PRODUCTS - 1} + 1)::BIGINT  AS product_id,
                    DATE '2020-01-15'                           AS order_date,
                    ROUND((random() * 999 + 0.01)::DOUBLE, 2)  AS amount,
                    'processing'                                AS status,
                    'north'                                     AS region,
                    1::INT                                      AS quantity,
                    0.0::DOUBLE                                 AS discount,
                    NOW()                                       AS created_at
                FROM generate_series(0, 399999) AS t(range)
                UNION ALL
                -- brand-new orders (20%)
                SELECT
                    ({N_ORDERS} + range + 1)::BIGINT            AS order_id,
                    (random() * {N_CUSTOMERS - 1} + 1)::BIGINT AS customer_id,
                    (random() * {N_PRODUCTS - 1} + 1)::BIGINT  AS product_id,
                    DATE '2024-06-01'                           AS order_date,
                    ROUND((random() * 999 + 0.01)::DOUBLE, 2)  AS amount,
                    'pending'                                   AS status,
                    'east'                                      AS region,
                    2::INT                                      AS quantity,
                    0.1::DOUBLE                                 AS discount,
                    NOW()                                       AS created_at
                FROM generate_series(0, 99999) AS t(range)
            )
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()


def _print_stats() -> None:
    con = duckdb.connect()
    for label, path in [
        ("orders", ORDERS_PARQUET),
        ("customers", CUSTOMERS_PARQUET),
        ("products", PRODUCTS_PARQUET),
        ("merge_batch", MERGE_BATCH_PARQUET),
    ]:
        if not path.exists():
            continue
        row = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{path}')"
        ).fetchone()
        size_mb = path.stat().st_size / 1_048_576
        console.print(
            f"  [cyan]{label}[/cyan]: "
            f"row_count={row[0]:,}  file_size={size_mb:.1f} MB"
        )
    con.close()
