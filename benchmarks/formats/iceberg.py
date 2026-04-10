"""
Apache Iceberg adapter.

Catalog  : SQLite (via pyiceberg sql catalog) — fully local, no Hive/REST server needed.
Writes   : pyiceberg Python API (append / overwrite / delete).
Reads    : DuckDB `iceberg` extension via iceberg_scan() — same engine as the other formats.

Notes on UPDATE semantics in Iceberg:
  Iceberg doesn't have a single-call UPDATE like DuckLake/Delta.
  The standard pattern is: delete matching rows then append the modified rows.
  This is what pyiceberg exposes and what we benchmark, which accurately
  reflects the operational cost of Iceberg updates.
"""

import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from rich.console import Console

from benchmarks.config import (
    ICEBERG_CATALOG,
    ICEBERG_DIR,
    ICEBERG_WAREHOUSE,
    ORDERS_PARQUET,
    CUSTOMERS_PARQUET,
    PRODUCTS_PARQUET,
    MERGE_BATCH_PARQUET,
    UPDATE_CUTOFF_DATE,
    DELETE_CUTOFF_DATE,
)
from benchmarks.formats.base import FormatAdapter, QueryResult

console = Console()

_NAMESPACE = "benchmark"
_CATALOG_NAME = "local"


class IcebergAdapter(FormatAdapter):
    name = "iceberg"

    def __init__(self) -> None:
        self.con: duckdb.DuckDBPyConnection | None = None
        self._catalog = None
        # Cache metadata locations so we always point to the latest snapshot
        self._metadata: dict[str, str] = {}

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def setup(self) -> None:
        ICEBERG_DIR.mkdir(parents=True, exist_ok=True)
        ICEBERG_WAREHOUSE.mkdir(parents=True, exist_ok=True)

        self._catalog = self._make_catalog()

        try:
            self._catalog.create_namespace(_NAMESPACE)
        except Exception:
            pass  # already exists on re-runs

        console.print("  [yellow]Iceberg[/yellow] loading orders…")
        self._load_table("orders", pq.read_table(str(ORDERS_PARQUET)))
        console.print("  [yellow]Iceberg[/yellow] loading customers…")
        self._load_table("customers", pq.read_table(str(CUSTOMERS_PARQUET)))
        console.print("  [yellow]Iceberg[/yellow] loading products…")
        self._load_table("products", pq.read_table(str(PRODUCTS_PARQUET)))

        self.con = duckdb.connect()
        self._load_extension()
        console.print("  [yellow]Iceberg[/yellow] setup complete.")

    def teardown(self) -> None:
        if self.con:
            self.con.close()
            self.con = None

    # -------------------------------------------------------------------------
    # Table references
    # -------------------------------------------------------------------------

    @property
    def orders_ref(self) -> str:
        return self._iceberg_scan("orders")

    @property
    def customers_ref(self) -> str:
        return self._iceberg_scan("customers")

    @property
    def products_ref(self) -> str:
        return self._iceberg_scan("products")

    def _iceberg_scan(self, table: str) -> str:
        # DuckDB iceberg_scan expects the table directory, not the metadata.json path.
        table_dir = ICEBERG_WAREHOUSE / _NAMESPACE / table
        return f"iceberg_scan('{table_dir}', allow_moved_paths=true)"

    # -------------------------------------------------------------------------
    # Query execution (via DuckDB iceberg_scan)
    # -------------------------------------------------------------------------

    def query(self, sql: str) -> QueryResult:
        t0 = time.perf_counter()
        rel = self.con.execute(sql)
        rows = len(rel.fetchdf())
        return QueryResult(rows=rows, elapsed_ms=(time.perf_counter() - t0) * 1000)

    # -------------------------------------------------------------------------
    # Mutations (via pyiceberg)
    # -------------------------------------------------------------------------

    def update_pending_orders(self) -> QueryResult:
        """
        UPDATE orders SET status='processing'
        WHERE status='pending' AND order_date < UPDATE_CUTOFF_DATE.

        Iceberg pattern: delete matching rows, then append modified rows.
        """
        from pyiceberg.expressions import And, EqualTo, LessThan

        table = self._catalog.load_table(f"{_NAMESPACE}.orders")

        # 1. Read rows we intend to update
        scan = table.scan(
            row_filter=And(
                EqualTo("status", "pending"),
                LessThan("order_date", UPDATE_CUTOFF_DATE),
            )
        )
        affected = scan.to_arrow()

        # 2. Build the modified version
        new_status = pa.array(["processing"] * len(affected), type=pa.string())
        idx = affected.schema.get_field_index("status")
        modified = affected.set_column(idx, "status", new_status)

        t0 = time.perf_counter()
        # 3. Delete originals
        table.delete(
            delete_filter=And(
                EqualTo("status", "pending"),
                LessThan("order_date", UPDATE_CUTOFF_DATE),
            )
        )
        # 4. Append modified
        table.append(modified)
        elapsed = (time.perf_counter() - t0) * 1000

        self._refresh_metadata("orders")
        return QueryResult(rows=len(affected), elapsed_ms=elapsed)

    def delete_old_cancelled_orders(self) -> QueryResult:
        from pyiceberg.expressions import And, EqualTo, LessThan

        table = self._catalog.load_table(f"{_NAMESPACE}.orders")
        t0 = time.perf_counter()
        table.delete(
            delete_filter=And(
                EqualTo("status", "cancelled"),
                LessThan("order_date", DELETE_CUTOFF_DATE),
            )
        )
        elapsed = (time.perf_counter() - t0) * 1000
        self._refresh_metadata("orders")
        return QueryResult(rows=0, elapsed_ms=elapsed)

    def merge_new_orders_batch(self) -> QueryResult:
        """
        UPSERT merge batch: update matching order_ids, insert new ones.
        Iceberg MERGE pattern: delete-then-append for matched rows + append for new.
        """
        from pyiceberg.expressions import In

        batch = pq.read_table(str(MERGE_BATCH_PARQUET))
        existing_ids = batch["order_id"].to_pylist()[:400_000]  # the update half

        table = self._catalog.load_table(f"{_NAMESPACE}.orders")

        t0 = time.perf_counter()
        # Delete rows that will be replaced
        table.delete(delete_filter=In("order_id", existing_ids))
        # Append entire batch (updates + inserts in one go)
        table.append(batch)
        elapsed = (time.perf_counter() - t0) * 1000

        self._refresh_metadata("orders")
        return QueryResult(rows=len(batch), elapsed_ms=elapsed)

    # -------------------------------------------------------------------------
    # Schema evolution
    # -------------------------------------------------------------------------

    def add_column(self, table: str, column_name: str, column_type: str) -> QueryResult:
        from pyiceberg.types import BooleanType, LongType, DoubleType, StringType

        _type_map = {
            "BOOLEAN": BooleanType(),
            "BIGINT": LongType(),
            "DOUBLE": DoubleType(),
            "VARCHAR": StringType(),
        }
        ice_type = _type_map.get(column_type.upper(), StringType())
        tbl = self._catalog.load_table(f"{_NAMESPACE}.{table}")

        if column_name in {f.name for f in tbl.schema().fields}:
            return QueryResult(rows=0, elapsed_ms=0.0)

        t0 = time.perf_counter()
        with tbl.update_schema() as upd:
            upd.add_column(path=column_name, field_type=ice_type, required=False)
        elapsed = (time.perf_counter() - t0) * 1000

        self._refresh_metadata(table)
        return QueryResult(rows=0, elapsed_ms=elapsed)

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

    def _make_catalog(self):
        from pyiceberg.catalog.sql import SqlCatalog

        return SqlCatalog(
            _CATALOG_NAME,
            **{
                "uri": f"sqlite:///{ICEBERG_CATALOG}",
                "warehouse": f"file://{ICEBERG_WAREHOUSE}",
            },
        )

    def _load_table(self, name: str, arrow_table: pa.Table) -> None:
        from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids

        ice_schema = _pyarrow_to_schema_without_ids(arrow_table.schema)
        full_name = f"{_NAMESPACE}.{name}"

        try:
            tbl = self._catalog.load_table(full_name)
            tbl.overwrite(arrow_table)
        except Exception:
            tbl = self._catalog.create_table(full_name, schema=ice_schema)
            tbl.append(arrow_table)

        self._refresh_metadata(name)

    def _get_metadata_location(self, table: str) -> str:
        if table not in self._metadata:
            self._refresh_metadata(table)
        return self._metadata[table]

    def _refresh_metadata(self, table: str) -> None:
        tbl = self._catalog.load_table(f"{_NAMESPACE}.{table}")
        self._metadata[table] = tbl.metadata_location

    def _load_extension(self) -> None:
        try:
            self.con.execute("LOAD iceberg")
        except Exception:
            self.con.execute("INSTALL iceberg")
            self.con.execute("LOAD iceberg")
        # Required when pointing iceberg_scan at a table directory (local FS).
        self.con.execute("SET unsafe_enable_version_guessing = true")
