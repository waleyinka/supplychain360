-- models/marts/dimensions/dim_products.sql

{{ config(materialized='table', tags=['marts','dim']) }}

WITH snapshot AS (
    SELECT * 
    FROM {{ ref('products_snapshot') }}
    WHERE dbt_valid_to is NULL
)

SELECT
    -- surrogate key
    {{ generate_surrogate_key(['product_id']) }} AS product_sk,

    product_id,
    product_name,
    category,
    brand,
    supplier_id,
    unit_price,
    dbt_valid_from,
    dbt_valid_to

FROM snapshot
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY dbt_valid_from DESC) = 1