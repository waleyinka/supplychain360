-- models/staging/stg_store_locations.sql

{{ config(materialized='view', tags=['staging']) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'store_locations') }}
),

cleaned as (
    SELECT
        TRIM("store_id")                              AS store_id,
        TRIM("store_name")                            AS store_name,
        TRIM("city")                                  AS city,
        TRIM("state")                                 AS state,
        TRIM("region")                                AS region,
        TRY_TO_DATE("store_open_date", 'DD/MM/YYYY')  AS store_open_date,
        
        "_ingestion_timestamp"                        AS ingestion_timestamp,
        current_timestamp                             AS dbt_loaded_at,
        'raw.store_locations'                         AS source_table

    FROM source
)

SELECT * FROM cleaned