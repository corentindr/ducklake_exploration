-- Staging model: orders
-- Reads from whatever format the current dbt target uses (DuckLake / Delta / Iceberg).

SELECT
    order_id,
    customer_id,
    product_id,
    order_date,
    amount,
    status,
    region,
    quantity,
    discount,
    created_at,
    EXTRACT(year  FROM order_date)::INT AS order_year,
    EXTRACT(month FROM order_date)::INT AS order_month
FROM {{ get_source_table('orders') }}
