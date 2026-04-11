-- models/marts/dimensions/dim_stores.sql

{{ config(materialized='table', tags=['marts','dim']) }}

WITH snapshot AS (
    SELECT * FROM {{ ref('stores_snapshot') }}
    WHERE dbt_valid_to IS NULL
),

deduped AS (
    SELECT
        *
    FROM snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_id
        ORDER BY dbt_valid_from DESC
    ) = 1
)

SELECT
    -- surrogate key
    {{ generate_surrogate_key(['store_id']) }} AS store_sk,

    store_id,
    store_name,
    region,
    city,
    state,
    store_open_date,
    dbt_valid_from,
    dbt_valid_to

FROM deduped