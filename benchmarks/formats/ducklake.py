"""
DuckLake adapter.

DuckLake stores metadata in a SQLite catalog and data as Parquet files on disk.
All DML (INSERT, UPDATE, DELETE, MERGE) is handled natively by DuckDB via the
`ducklake` extension — no separate Python library needed.
"""

import time
import duckdb
from rich.console import Console

from benchmarks.config import (
    DUCKLAKE_CATALOG,
    DUCKLAKE_DATA,
    DUCKLAKE_DIR,
    ORDERS_PARQUET,
    CUSTOMERS_PARQUET,
    PRODUCTS_PARQUET,
    MERGE_BATCH_PARQUET,
    UPDATE_CUTOFF_DATE,
    DELETE_CUTOFF_DATE,
)
from benchmarks.formats.base import FormatAdapter, QueryResult

console = Console()


class DuckLakeAdapter(FormatAdapter):
    name = "ducklake"

    def __init__(self) -> None:
        self.con: duckdb.DuckDBPyConnection | None = None

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def setup(self) -> None:
        DUCKLAKE_DIR.mkdir(parents=True, exist_ok=True)
        DUCKLAKE_DATA.mkdir(parents=True, exist_ok=True)

        # Remove stale catalog so the extension always creates a fresh one.
        # A partial/empty catalog.db causes DuckLake to fail on DATA_PATH resolution.
        if DUCKLAKE_CATALOG.exists():
            DUCKLAKE_CATALOG.unlink()

        self.con = duckdb.connect()
        self._load_extension()

        attach_sql = (
            f"ATTACH 'ducklake:sqlite:{DUCKLAKE_CATALOG}' AS lake "
            f"(DATA_PATH '{DUCKLAKE_DATA}')"
        )
        self.con.execute(attach_sql)

        console.print("  [cyan]DuckLake[/cyan] loading orders…")
        self.con.execute(f"""
            CREATE OR REPLACE TABLE lake.orders AS
            SELECT * FROM read_parquet('{ORDERS_PARQUET}')
        """)
        console.print("  [cyan]DuckLake[/cyan] loading customers…")
        self.con.execute(f"""
            CREATE OR REPLACE TABLE lake.customers AS
            SELECT * FROM read_parquet('{CUSTOMERS_PARQUET}')
        """)
        console.print("  [cyan]DuckLake[/cyan] loading products…")
        self.con.execute(f"""
            CREATE OR REPLACE TABLE lake.products AS
            SELECT * FROM read_parquet('{PRODUCTS_PARQUET}')
        """)
        console.print("  [cyan]DuckLake[/cyan] setup complete.")

    def teardown(self) -> None:
        if self.con:
            self.con.close()
            self.con = None

    # -------------------------------------------------------------------------
    # Table references
    # -------------------------------------------------------------------------

    @property
    def orders_ref(self) -> str:
        return "lake.orders"

    @property
    def customers_ref(self) -> str:
        return "lake.customers"

    @property
    def products_ref(self) -> str:
        return "lake.products"

    # -------------------------------------------------------------------------
    # Query execution
    # -------------------------------------------------------------------------

    def query(self, sql: str) -> QueryResult:
        t0 = time.perf_counter()
        rel = self.con.execute(sql)
        rows = len(rel.fetchdf())
        return QueryResult(rows=rows, elapsed_ms=(time.perf_counter() - t0) * 1000)

    # -------------------------------------------------------------------------
    # Mutations
    # -------------------------------------------------------------------------

    def update_pending_orders(self) -> QueryResult:
        sql = f"""
            UPDATE lake.orders
            SET    status = 'processing'
            WHERE  status = 'pending'
              AND  order_date < DATE '{UPDATE_CUTOFF_DATE}'
        """
        t0 = time.perf_counter()
        self.con.execute(sql)
        elapsed = (time.perf_counter() - t0) * 1000
        # DuckDB EXPLAIN doesn't expose rows_changed easily; query count separately
        cnt = self.con.execute(f"""
            SELECT COUNT(*) FROM lake.orders
            WHERE status = 'processing' AND order_date < DATE '{UPDATE_CUTOFF_DATE}'
        """).fetchone()[0]
        return QueryResult(rows=cnt, elapsed_ms=elapsed)

    def delete_old_cancelled_orders(self) -> QueryResult:
        sql = f"""
            DELETE FROM lake.orders
            WHERE  status = 'cancelled'
              AND  order_date < DATE '{DELETE_CUTOFF_DATE}'
        """
        t0 = time.perf_counter()
        self.con.execute(sql)
        return QueryResult(rows=0, elapsed_ms=(time.perf_counter() - t0) * 1000)

    def merge_new_orders_batch(self) -> QueryResult:
        self.con.execute(f"""
            CREATE OR REPLACE TEMP VIEW merge_source AS
            SELECT * FROM read_parquet('{MERGE_BATCH_PARQUET}')
        """)
        sql = """
            MERGE INTO lake.orders AS tgt
            USING merge_source AS src
            ON tgt.order_id = src.order_id
            WHEN MATCHED THEN
                UPDATE SET
                    customer_id = src.customer_id, product_id  = src.product_id,
                    order_date  = src.order_date,  amount      = src.amount,
                    status      = src.status,      region      = src.region,
                    quantity    = src.quantity,    discount    = src.discount,
                    created_at  = src.created_at
            WHEN NOT MATCHED THEN
                INSERT (order_id, customer_id, product_id, order_date, amount,
                        status, region, quantity, discount, created_at)
                VALUES (src.order_id, src.customer_id, src.product_id, src.order_date,
                        src.amount, src.status, src.region, src.quantity,
                        src.discount, src.created_at)
        """
        t0 = time.perf_counter()
        self.con.execute(sql)
        elapsed = (time.perf_counter() - t0) * 1000
        return QueryResult(rows=500_000, elapsed_ms=elapsed)

    # -------------------------------------------------------------------------
    # Schema evolution
    # -------------------------------------------------------------------------

    def add_column(self, table: str, column_name: str, column_type: str) -> QueryResult:
        sql = f"ALTER TABLE lake.{table} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        t0 = time.perf_counter()
        self.con.execute(sql)
        return QueryResult(rows=0, elapsed_ms=(time.perf_counter() - t0) * 1000)

    def query_after_schema_change(self) -> QueryResult:
        sql = """
            SELECT
                region,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN is_prime_customer THEN 1 ELSE 0 END) AS prime_orders
            FROM lake.orders
            GROUP BY region
        """
        return self.query(sql)

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _load_extension(self) -> None:
        try:
            self.con.execute("LOAD ducklake")
        except Exception:
            self.con.execute("INSTALL ducklake")
            self.con.execute("LOAD ducklake")
