-- Mart: daily revenue — incremental materialization test.
-- On the first run this builds the full table; subsequent runs only process
-- new order dates.  Measures how each format handles incremental dbt models.

{{ config(
    materialized='incremental',
    unique_key='day',
    incremental_strategy='delete+insert'
) }}

SELECT
    order_date                      AS day,
    region,
    COUNT(*)                        AS order_count,
    SUM(amount)                     AS daily_revenue,
    AVG(amount)                     AS avg_order_value,
    SUM(quantity)                   AS units_sold
FROM   {{ ref('stg_orders') }}

{% if is_incremental() %}
    -- Only process dates not yet in the target
    WHERE order_date > (SELECT MAX(day) FROM {{ this }})
{% endif %}

GROUP  BY order_date, region
ORDER  BY day, region
