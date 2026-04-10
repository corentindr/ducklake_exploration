-- Mart: customer lifetime value — join orders × customers, aggregate.
-- Tests multi-table join + aggregation across formats.

SELECT
    c.customer_id,
    c.name,
    c.tier,
    c.country,
    c.signup_year,
    COUNT(o.order_id)               AS total_orders,
    SUM(o.amount)                   AS lifetime_revenue,
    AVG(o.amount)                   AS avg_order_value,
    MAX(o.order_date)               AS last_order_date,
    MIN(o.order_date)               AS first_order_date,
    SUM(o.quantity)                 AS total_units_purchased
FROM   {{ ref('stg_orders') }}     AS o
JOIN   {{ ref('stg_customers') }}  AS c USING (customer_id)
GROUP  BY
    c.customer_id, c.name, c.tier, c.country, c.signup_year
