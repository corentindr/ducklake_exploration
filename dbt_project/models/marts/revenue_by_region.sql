-- Mart: monthly revenue broken down by region and order status.
-- Tests GROUP BY + date truncation performance across formats.

SELECT
    region,
    order_year,
    order_month,
    status,
    COUNT(*)       AS order_count,
    SUM(amount)    AS total_revenue,
    AVG(amount)    AS avg_order_value,
    SUM(quantity)  AS total_units
FROM   {{ ref('stg_orders') }}
GROUP  BY region, order_year, order_month, status
ORDER  BY order_year, order_month, region
