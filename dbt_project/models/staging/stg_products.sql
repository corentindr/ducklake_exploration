-- Staging model: products

SELECT
    product_id,
    name,
    category,
    price,
    supplier_id,
    weight_kg
FROM {{ get_source_table('products') }}
