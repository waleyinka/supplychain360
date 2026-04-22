-- models/intermediate/base/int_dedup_suppliers.sql

{{ config(materialized='table', tags=['intermediate', 'dedup']) }}

WITH deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY supplier_id
            ORDER BY ingestion_timestamp DESC
    ) AS rn
    
    FROM {{ ref('stg_suppliers') }}
)

SELECT * 
FROM deduped
WHERE rn = 1