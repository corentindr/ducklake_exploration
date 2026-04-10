"""
Aggregation benchmarks — GROUP BY, window functions, multi-table joins.
"""

from benchmarks.formats.base import FormatAdapter, QueryResult


def revenue_by_region_month(adapter: FormatAdapter) -> QueryResult:
    return adapter.query(f"""
        SELECT
            region,
            DATE_TRUNC('month', order_date) AS month,
            SUM(amount)                     AS total_revenue,
            COUNT(*)                        AS order_count
        FROM   {adapter.orders_ref}
        GROUP  BY region, DATE_TRUNC('month', order_date)
        ORDER  BY month, region
    """)


def customer_lifetime_value(adapter: FormatAdapter) -> QueryResult:
    return adapter.query(f"""
        SELECT
            c.tier,
            c.country,
            COUNT(DISTINCT o.customer_id)   AS num_customers,
            SUM(o.amount)                   AS total_spend,
            AVG(o.amount)                   AS avg_order_value
        FROM   {adapter.orders_ref}      AS o
        JOIN   {adapter.customers_ref}   AS c USING (customer_id)
        GROUP  BY c.tier, c.country
        ORDER  BY total_spend DESC
        LIMIT  50
    """)


def top_product_categories(adapter: FormatAdapter) -> QueryResult:
    return adapter.query(f"""
        SELECT
            p.category,
            COUNT(*)                        AS order_count,
            SUM(o.quantity * p.price)       AS gross_revenue,
            AVG(o.discount)                 AS avg_discount
        FROM   {adapter.orders_ref}      AS o
        JOIN   {adapter.products_ref}    AS p USING (product_id)
        GROUP  BY p.category
        ORDER  BY gross_revenue DESC
    """)


def running_revenue_window(adapter: FormatAdapter) -> QueryResult:
    """Window function: running total per region ordered by month."""
    return adapter.query(f"""
        WITH monthly AS (
            SELECT
                region,
                DATE_TRUNC('month', order_date) AS month,
                SUM(amount)                     AS monthly_revenue
            FROM   {adapter.orders_ref}
            GROUP  BY region, DATE_TRUNC('month', order_date)
        )
        SELECT
            region,
            month,
            monthly_revenue,
            SUM(monthly_revenue) OVER (
                PARTITION BY region
                ORDER BY month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_revenue
        FROM   monthly
        ORDER  BY region, month
    """)


def heavy_aggregation_all_dimensions(adapter: FormatAdapter) -> QueryResult:
    """Stress test: GROUP BY three dimensions simultaneously."""
    return adapter.query(f"""
        SELECT
            region,
            status,
            EXTRACT(year FROM order_date) AS year,
            COUNT(*)                      AS orders,
            SUM(amount)                   AS revenue,
            AVG(amount)                   AS avg_amount,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount
        FROM   {adapter.orders_ref}
        GROUP  BY region, status, EXTRACT(year FROM order_date)
    """)


AGGREGATION_BENCHMARKS = [
    ("revenue_by_region_month",   "Revenue grouped by region × month",      revenue_by_region_month),
    ("customer_ltv",              "Customer LTV joined with customers dim",  customer_lifetime_value),
    ("top_product_categories",    "Top categories joined with products dim", top_product_categories),
    ("running_revenue_window",    "Cumulative revenue window function",      running_revenue_window),
    ("heavy_aggregation",         "GROUP BY region × status × year + P50",  heavy_aggregation_all_dimensions),
]
