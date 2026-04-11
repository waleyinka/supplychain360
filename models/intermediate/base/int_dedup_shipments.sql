-- models/intermediate/base/int_dedup_shipments.sql

{{ config(materialized='table', tags=['intermediate', 'dedup']) }}

WITH deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY shipment_id
            ORDER BY ingestion_timestamp DESC
    ) AS rn
    
    FROM {{ ref('stg_shipments') }}
)

SELECT * 
FROM deduped
WHERE rn = 1