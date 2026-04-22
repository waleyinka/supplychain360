-- models/staging/stg_store_sales.sql

{{ config(materialized='view', tags=['staging']) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'store_sales') }}
),
 
cleaned as (
    SELECT
        TRIM("transaction_id")                      AS transaction_id,
        TRIM("store_id")                            AS store_id,
        TRIM("product_id")                          AS product_id,
        
        CAST("quantity_sold" AS INTEGER)            AS quantity_sold,
        CAST("unit_price" AS NUMBER(12,2))          AS unit_price,
        CAST("discount_pct" AS NUMBER(12,2))        AS discount_pct,
        CAST("sale_amount" AS NUMBER(12,2))         AS sale_amount,
        
        CAST("transaction_timestamp" AS TIMESTAMP)  AS transaction_timestamp,
        
        "_ingestion_timestamp"                      AS ingestion_timestamp,
        
        current_timestamp                           AS dbt_loaded_at,
        'raw.store_sales'                           AS source_table

    FROM source
)

SELECT * FROM cleaned