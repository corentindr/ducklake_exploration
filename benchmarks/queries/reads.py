"""
Read / scan benchmarks.
Each benchmark is a (name, description, callable(adapter) -> QueryResult) tuple.
"""

from benchmarks.formats.base import FormatAdapter, QueryResult


def full_table_scan(adapter: FormatAdapter) -> QueryResult:
    return adapter.query(f"SELECT COUNT(*) FROM {adapter.orders_ref}")


def filtered_scan_date_region(adapter: FormatAdapter) -> QueryResult:
    return adapter.query(f"""
        SELECT *
        FROM   {adapter.orders_ref}
        WHERE  order_date BETWEEN DATE '2022-01-01' AND DATE '2022-12-31'
          AND  region = 'north'
    """)


def column_pruning(adapter: FormatAdapter) -> QueryResult:
    return adapter.query(f"""
        SELECT order_id, amount, status
        FROM   {adapter.orders_ref}
        WHERE  status = 'pending'
    """)


def point_lookup(adapter: FormatAdapter) -> QueryResult:
    return adapter.query(f"""
        SELECT *
        FROM   {adapter.orders_ref}
        WHERE  order_id = 12345678
    """)


def multi_predicate_scan(adapter: FormatAdapter) -> QueryResult:
    return adapter.query(f"""
        SELECT order_id, customer_id, amount
        FROM   {adapter.orders_ref}
        WHERE  amount > 900
          AND  status = 'delivered'
          AND  region IN ('east', 'west')
    """)


READ_BENCHMARKS = [
    ("full_table_scan",       "COUNT(*) full table scan",             full_table_scan),
    ("filtered_date_region",  "Filter by year + region (predicate pushdown)", filtered_scan_date_region),
    ("column_pruning",        "Select 3 columns, filter by status (column pruning)", column_pruning),
    ("point_lookup",          "Single-row lookup by primary key",     point_lookup),
    ("multi_predicate_scan",  "Multi-predicate high-value delivered orders", multi_predicate_scan),
]
