-- models/staging/stg_warehouses.sql

{{ config(materialized='view', tags=['staging']) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'warehouses') }}
),

cleaned as (
    SELECT
        TRIM("warehouse_id")    AS warehouse_id,
        TRIM("city")            AS city,
        TRIM("state")           AS state,
        
        "_ingestion_timestamp"  AS ingestion_timestamp,
        current_timestamp       AS dbt_loaded_at,
        'raw.warehouses'        AS source_table

    FROM source
)

SELECT * FROM cleaned