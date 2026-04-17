-- models/marts/dimensions/dim_warehouses.sql

{{ config(materialized='table', tags=['marts','dim']) }}

WITH snapshot AS (
    SELECT * 
    FROM {{ ref('warehouses_snapshot') }}
    WHERE dbt_valid_to IS NULL
)

SELECT
    -- surrogate key
    {{ generate_surrogate_key(['warehouse_id']) }} AS warehouse_sk,

    warehouse_id,
    city,
    state,
    dbt_valid_from,
    dbt_valid_to

FROM snapshot