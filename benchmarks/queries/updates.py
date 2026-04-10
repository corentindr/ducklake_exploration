"""
Update / delete / merge benchmarks.

These are the most format-differentiating operations:
  - DuckLake : native SQL UPDATE / DELETE / INSERT OR REPLACE
  - Delta    : delta-rs Rust UPDATE / DELETE / MERGE API
  - Iceberg  : pyiceberg delete-then-append (no native UPDATE primitive)

All three operations target the same logical rows so timing is comparable.
"""

from benchmarks.formats.base import FormatAdapter, QueryResult


def update_pending_orders(adapter: FormatAdapter) -> QueryResult:
    """
    Set status='processing' for all pending orders placed before UPDATE_CUTOFF_DATE.
    Targets ~200K rows (~0.4% of the table).
    """
    return adapter.update_pending_orders()


def delete_cancelled_orders(adapter: FormatAdapter) -> QueryResult:
    """
    Remove cancelled orders placed before DELETE_CUTOFF_DATE.
    Targets ~1.2M rows (~2.4% of the table).
    """
    return adapter.delete_old_cancelled_orders()


def merge_batch_upsert(adapter: FormatAdapter) -> QueryResult:
    """
    UPSERT 500K rows:  400K updates to existing orders + 100K brand-new inserts.
    This is the critical real-world workload for CDC / incremental pipelines.
    """
    return adapter.merge_new_orders_batch()


UPDATE_BENCHMARKS = [
    ("update_pending_orders",  "UPDATE ~200K pending → processing (0.4% of table)", update_pending_orders),
    ("delete_cancelled",       "DELETE ~1.2M cancelled orders older than cutoff",   delete_cancelled_orders),
    ("merge_upsert_500k",      "MERGE / UPSERT 500K rows (80% updates + 20% inserts)", merge_batch_upsert),
]
