-- models/staging/stg_products.sql

{{ config(materialized='view', tags=['staging']) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'products') }}
),

cleaned AS (
    SELECT
        TRIM("product_id")                 AS product_id,
        TRIM("product_name")               AS product_name,
        TRIM("category")                   AS category,
        TRIM("brand")                      AS brand,
        TRIM("supplier_id")                AS supplier_id,
        CAST("unit_price" AS NUMBER(12,2)) AS unit_price,
        
        "_ingestion_timestamp"      AS ingestion_timestamp,
        current_timestamp           AS dbt_loaded_at,
        'raw.products'              AS source_table
    
    FROM source
)

SELECT * FROM cleaned