-- models/intermediate/base/int_dedup_inventory.sql

{{ config(materialized='table', tags=['intermediate', 'dedup']) }}

WITH deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY warehouse_id, product_id, snapshot_date
            ORDER BY ingestion_timestamp DESC
    ) AS rn
    
    FROM {{ ref('stg_inventory') }}
)

SELECT * 
FROM deduped
WHERE rn = 1