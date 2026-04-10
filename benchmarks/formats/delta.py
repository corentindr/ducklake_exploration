"""
Delta Lake adapter.

Writes/reads use delta-rs (the `deltalake` Python package).
Reads for SELECT benchmarks go through DuckDB's `delta` extension so all
three formats are read by the same query engine — keeping the comparison fair.
Updates/deletes/merges use delta-rs's native Rust-backed operations.
"""

import time
import duckdb
import pyarrow.parquet as pq
from rich.console import Console

from benchmarks.config import (
    DELTA_DIR,
    DELTA_ORDERS,
    DELTA_CUSTOMERS,
    DELTA_PRODUCTS,
    DELTA_ORDERS_MERGE,
    ORDERS_PARQUET,
    CUSTOMERS_PARQUET,
    PRODUCTS_PARQUET,
    MERGE_BATCH_PARQUET,
    UPDATE_CUTOFF_DATE,
    DELETE_CUTOFF_DATE,
)
from benchmarks.formats.base import FormatAdapter, QueryResult

console = Console()


class DeltaAdapter(FormatAdapter):
    name = "delta"

    def __init__(self) -> None:
        self.con: duckdb.DuckDBPyConnection | None = None

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def setup(self) -> None:
        from deltalake import write_deltalake

        DELTA_DIR.mkdir(parents=True, exist_ok=True)

        console.print("  [magenta]Delta[/magenta] loading orders…")
        orders = pq.read_table(str(ORDERS_PARQUET))
        write_deltalake(str(DELTA_ORDERS), orders, mode="overwrite")
        del orders

        console.print("  [magenta]Delta[/magenta] loading customers…")
        customers = pq.read_table(str(CUSTOMERS_PARQUET))
        write_deltalake(str(DELTA_CUSTOMERS), customers, mode="overwrite")
        del customers

        console.print("  [magenta]Delta[/magenta] loading products…")
        products = pq.read_table(str(PRODUCTS_PARQUET))
        write_deltalake(str(DELTA_PRODUCTS), products, mode="overwrite")
        del products

        # Pre-stage merge batch as a Delta table for merge_new_orders_batch
        console.print("  [magenta]Delta[/magenta] staging merge batch…")
        batch = pq.read_table(str(MERGE_BATCH_PARQUET))
        write_deltalake(str(DELTA_ORDERS_MERGE), batch, mode="overwrite")
        del batch

        self.con = duckdb.connect()
        self._load_extension()
        console.print("  [magenta]Delta[/magenta] setup complete.")

    def teardown(self) -> None:
        if self.con:
            self.con.close()
            self.con = None

    # -------------------------------------------------------------------------
    # Table references
    # -------------------------------------------------------------------------

    @property
    def orders_ref(self) -> str:
        return f"delta_scan('{DELTA_ORDERS}')"

    @property
    def customers_ref(self) -> str:
        return f"delta_scan('{DELTA_CUSTOMERS}')"

    @property
    def products_ref(self) -> str:
        return f"delta_scan('{DELTA_PRODUCTS}')"

    # -------------------------------------------------------------------------
    # Query execution (via DuckDB delta_scan)
    # -------------------------------------------------------------------------

    def query(self, sql: str) -> QueryResult:
        t0 = time.perf_counter()
        rel = self.con.execute(sql)
        rows = len(rel.fetchdf())
        return QueryResult(rows=rows, elapsed_ms=(time.perf_counter() - t0) * 1000)

    # -------------------------------------------------------------------------
    # Mutations (via delta-rs Rust engine)
    # -------------------------------------------------------------------------

    def update_pending_orders(self) -> QueryResult:
        from deltalake import DeltaTable

        dt = DeltaTable(str(DELTA_ORDERS))
        t0 = time.perf_counter()
        result = dt.update(
            updates={"status": "'processing'"},
            predicate=f"status = 'pending' AND order_date < '{UPDATE_CUTOFF_DATE}'",
        )
        elapsed = (time.perf_counter() - t0) * 1000
        rows = result.get("num_updated_rows", 0)
        return QueryResult(rows=rows, elapsed_ms=elapsed)

    def delete_old_cancelled_orders(self) -> QueryResult:
        from deltalake import DeltaTable

        dt = DeltaTable(str(DELTA_ORDERS))
        t0 = time.perf_counter()
        result = dt.delete(
            predicate=f"status = 'cancelled' AND order_date < '{DELETE_CUTOFF_DATE}'"
        )
        elapsed = (time.perf_counter() - t0) * 1000
        rows = result.get("num_deleted_rows", 0)
        return QueryResult(rows=rows, elapsed_ms=elapsed)

    def merge_new_orders_batch(self) -> QueryResult:
        from deltalake import DeltaTable
        import pyarrow.parquet as pq

        dt = DeltaTable(str(DELTA_ORDERS))
        batch = pq.read_table(str(MERGE_BATCH_PARQUET))

        t0 = time.perf_counter()
        (
            dt.merge(
                source=batch,
                predicate="target.order_id = source.order_id",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        elapsed = (time.perf_counter() - t0) * 1000
        return QueryResult(rows=500_000, elapsed_ms=elapsed)

    # -------------------------------------------------------------------------
    # Schema evolution (delta-rs alter table)
    # -------------------------------------------------------------------------

    def add_column(self, table: str, column_name: str, column_type: str) -> QueryResult:
        from deltalake import DeltaTable
        from deltalake.schema import Field, PrimitiveType

        # Map simple type strings to delta-rs types
        _type_map = {
            "BOOLEAN": "boolean",
            "BIGINT": "long",
            "DOUBLE": "double",
            "VARCHAR": "string",
        }
        dt_type = _type_map.get(column_type.upper(), "string")
        path_map = {"orders": DELTA_ORDERS, "customers": DELTA_CUSTOMERS, "products": DELTA_PRODUCTS}
        dt = DeltaTable(str(path_map[table]))

        t0 = time.perf_counter()
        dt.alter.add_columns(
            [Field(column_name, PrimitiveType(dt_type), nullable=True)]
        )
        return QueryResult(rows=0, elapsed_ms=(time.perf_counter() - t0) * 1000)

    def query_after_schema_change(self) -> QueryResult:
        sql = f"""
            SELECT
                region,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN is_prime_customer THEN 1 ELSE 0 END) AS prime_orders
            FROM {self.orders_ref}
            GROUP BY region
        """
        return self.query(sql)

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _load_extension(self) -> None:
        try:
            self.con.execute("LOAD delta")
        except Exception:
            self.con.execute("INSTALL delta")
            self.con.execute("LOAD delta")
