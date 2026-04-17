-- models/staging/stg_inventory.sql

{{ config(materialized='view', tags=['staging']) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'inventory') }}
),

cleaned as (
    SELECT
        TRIM("warehouse_id")                        AS warehouse_id,
        TRIM("product_id")                          AS product_id,
        CAST("quantity_available" AS INTEGER)       AS quantity_available,
        CAST("reorder_threshold" AS INTEGER)        AS reorder_threshold,
        
        COALESCE(
            TRY_TO_DATE("snapshot_date", 'DD/MM/YYYY'),
            TRY_TO_DATE("snapshot_date", 'YYYY-MM-DD')
        ) AS snapshot_date,
        
        "_ingestion_timestamp"                      AS ingestion_timestamp,
        current_timestamp                           AS dbt_loaded_at,
        'raw.inventory'                             AS source_table

    FROM source
)

SELECT * FROM cleaned