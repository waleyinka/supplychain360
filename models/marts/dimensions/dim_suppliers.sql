-- models/marts/dimensions/dim_suppliers.sql

{{ config(materialized='table', tags=['marts','dim']) }}

WITH snapshot AS (
    SELECT * FROM {{ ref('suppliers_snapshot') }}
    WHERE dbt_valid_to IS NULL
)

SELECT
    -- surrogate key
    {{ generate_surrogate_key(['supplier_id']) }} AS supplier_sk,

    supplier_id,
    supplier_name,
    country,
    category,
    dbt_valid_from,
    dbt_valid_to

FROM snapshot

WHERE dbt_valid_to IS NULL