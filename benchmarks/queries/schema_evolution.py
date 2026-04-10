"""
Schema evolution benchmarks.

Tests how each format handles DDL changes (ADD COLUMN) without rewriting data,
and how quickly subsequent queries handle the new nullable column.
"""

from benchmarks.formats.base import FormatAdapter, QueryResult


def add_boolean_column(adapter: FormatAdapter) -> QueryResult:
    """ADD COLUMN is_prime_customer BOOLEAN to the orders table."""
    return adapter.add_column("orders", "is_prime_customer", "BOOLEAN")


def query_with_new_column(adapter: FormatAdapter) -> QueryResult:
    """
    Query referencing the newly added column.
    All existing rows will have NULL for is_prime_customer — tests
    how each format handles null-backfill in aggregate queries.
    """
    return adapter.query_after_schema_change()


SCHEMA_EVOLUTION_BENCHMARKS = [
    ("add_column_boolean",      "ALTER TABLE ADD COLUMN is_prime_customer BOOLEAN", add_boolean_column),
    ("query_after_add_column",  "GROUP BY query using new nullable column",          query_with_new_column),
]
