-- Mart: product-level performance — join orders × products.
-- Tests three-way join + window function across formats.

WITH base AS (
    SELECT
        p.product_id,
        p.name                              AS product_name,
        p.category,
        p.price                             AS list_price,
        o.region,
        o.order_year,
        COUNT(o.order_id)                   AS order_count,
        SUM(o.quantity)                     AS units_sold,
        SUM(o.quantity * p.price)           AS gross_revenue,
        SUM(o.quantity * p.price * (1 - o.discount)) AS net_revenue
    FROM   {{ ref('stg_orders') }}    AS o
    JOIN   {{ ref('stg_products') }}  AS p USING (product_id)
    GROUP  BY p.product_id, p.name, p.category, p.price, o.region, o.order_year
)
SELECT
    *,
    SUM(gross_revenue) OVER (PARTITION BY category, order_year)  AS category_year_revenue,
    RANK() OVER (PARTITION BY category, order_year ORDER BY gross_revenue DESC) AS rank_in_category
FROM   base
