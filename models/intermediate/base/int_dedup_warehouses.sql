-- models/intermediate/base/int_dedup_warehouses.sql

{{ config(materialized='table', tags=['intermediate', 'dedup']) }}

WITH deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY warehouse_id
            ORDER BY ingestion_timestamp DESC
    ) AS rn
    
    FROM {{ ref('stg_warehouses') }}
)

SELECT * 
FROM deduped
WHERE rn = 1