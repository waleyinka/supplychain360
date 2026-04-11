-- models/intermediate/base/int_dedup_store_sales.sql

{{ config(materialized='table', tags=['intermediate', 'dedup']) }}

WITH deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY transaction_id
            ORDER BY ingestion_timestamp DESC
    ) AS rn
    
    FROM {{ ref('stg_store_sales') }}
)

SELECT * 
FROM deduped
WHERE rn = 1