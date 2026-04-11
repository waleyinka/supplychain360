-- models/staging/stg_suppliers.sql

{{ config(materialized='view', tags=['staging']) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'suppliers') }}
),

cleaned as (
    SELECT
        TRIM("supplier_id")     AS supplier_id,
        TRIM("supplier_name")   AS supplier_name,
        TRIM("category")        AS category,
        TRIM("country")         AS country,
        
        "_ingestion_timestamp"  AS ingestion_timestamp,
        current_timestamp       AS dbt_loaded_at,
        'raw.suppliers'         AS source_table
        
    FROM source
)

SELECT * FROM cleaned