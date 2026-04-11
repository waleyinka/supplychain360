-- models/staging/stg_shipments.sql

{{ config(materialized='view', tags=['staging']) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'shipments') }}
),

cleaned AS (
    SELECT
        TRIM("shipment_id")                                 AS shipment_id,
        TRIM("warehouse_id")                                AS warehouse_id,
        TRIM("store_id")                                    AS store_id,
        TRIM("product_id")                                  AS product_id,
        CAST("quantity_shipped" AS INTEGER)                 AS quantity_shipped,
        
        TRY_TO_DATE("shipment_date", 'DD/MM/YYYY')          AS shipment_date,
        TRY_TO_DATE("expected_delivery_date", 'DD/MM/YYYY') AS expected_delivery_date,
        TRY_TO_DATE("actual_delivery_date", 'DD/MM/YYYY')   AS actual_delivery_date,
        TRIM("carrier")                                     AS carrier,
        
        "_ingestion_timestamp"                              AS ingestion_timestamp,
        CURRENT_TIMESTAMP                                   AS dbt_loaded_at,
        'raw.shipments'                                     AS source_table

    FROM source
)

SELECT * FROM cleaned