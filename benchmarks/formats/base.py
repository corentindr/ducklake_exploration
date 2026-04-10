from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class QueryResult:
    rows: int
    elapsed_ms: float


class FormatAdapter(ABC):
    """
    Common interface for all three table formats.
    Each adapter manages its own DuckDB connection(s) and format-specific
    Python libraries (deltalake, pyiceberg).
    """

    name: str  # "ducklake" | "delta" | "iceberg"

    # --- Lifecycle ---

    @abstractmethod
    def setup(self) -> None:
        """
        Load source Parquet files into this format.
        Called once before benchmarking.
        """

    @abstractmethod
    def teardown(self) -> None:
        """Release connections and other resources."""

    # --- Table references (used to build query strings) ---

    @property
    @abstractmethod
    def orders_ref(self) -> str:
        """SQL expression that resolves to the orders table."""

    @property
    @abstractmethod
    def customers_ref(self) -> str:
        """SQL expression that resolves to the customers table."""

    @property
    @abstractmethod
    def products_ref(self) -> str:
        """SQL expression that resolves to the products table."""

    # --- Query execution ---

    @abstractmethod
    def query(self, sql: str) -> QueryResult:
        """Execute a SELECT query and return row count + elapsed ms."""

    # --- Mutation benchmarks ---

    @abstractmethod
    def update_pending_orders(self) -> QueryResult:
        """
        Set status='processing' for pending orders placed before UPDATE_CUTOFF_DATE.
        Returns rows_affected + elapsed_ms.
        """

    @abstractmethod
    def delete_old_cancelled_orders(self) -> QueryResult:
        """
        Delete cancelled orders placed before DELETE_CUTOFF_DATE.
        Returns rows_affected + elapsed_ms.
        """

    @abstractmethod
    def merge_new_orders_batch(self) -> QueryResult:
        """
        UPSERT 500K orders (80% updates, 20% inserts) from MERGE_BATCH_PARQUET.
        Returns total rows touched + elapsed_ms.
        """

    # --- Schema evolution ---

    @abstractmethod
    def add_column(self, table: str, column_name: str, column_type: str) -> QueryResult:
        """
        ALTER TABLE to add a new column.
        elapsed_ms is for the DDL itself; rows is 0.
        """

    @abstractmethod
    def query_after_schema_change(self) -> QueryResult:
        """
        Run a query referencing the newly-added column.
        Verifies schema evolution worked and tests null-handling performance.
        """
