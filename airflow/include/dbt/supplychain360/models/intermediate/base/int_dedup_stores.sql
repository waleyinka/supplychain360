-- models/intermediate/base/int_dedup_stores.sql

{{ config(materialized='table', tags=['intermediate', 'dedup']) }}

WITH deduped AS (
    SELECT * FROM {{ ref('stg_store_locations') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY store_id ORDER BY ingestion_timestamp DESC) = 1
)

SELECT
    MD5(store_id) AS store_sk,
    *
FROM deduped