-- models/intermediate/warehouse/int_store_warehouse_map.sql

{{ config(materialized='view', tags=['intermediate','warehouse']) }}

WITH shipment AS (
  SELECT
    store_id,
    warehouse_id,
    shipment_date
  FROM {{ ref('int_dedup_shipments') }}
),

ranked AS (
  SELECT
    *,
    
    ROW_NUMBER() OVER (
        PARTITION BY store_id ORDER BY shipment_date DESC
    ) AS rn
  
  FROM shipment
)

SELECT
    store_id,
    warehouse_id,
    shipment_date AS last_shipment_date
FROM ranked
WHERE rn = 1