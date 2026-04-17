-- models/intermediate/base/int_dedup_products.sql

{{ config(materialized='table', tags=['intermediate', 'dedup']) }}

WITH deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY product_id
            ORDER BY ingestion_timestamp DESC
    ) AS rn
    
    FROM {{ ref('stg_products') }}
)

SELECT * 
FROM deduped
WHERE rn = 1