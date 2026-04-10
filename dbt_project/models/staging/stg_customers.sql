-- Staging model: customers

SELECT
    customer_id,
    name,
    email,
    signup_date,
    country,
    tier,
    EXTRACT(year FROM signup_date)::INT AS signup_year
FROM {{ get_source_table('customers') }}
