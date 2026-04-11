-- models/marts/facts/fct_supplier_performance.sql

{{ config(materialized='table', tags=['marts','fact']) }}

WITH performance AS (
    SELECT *
    FROM {{ ref('int_shipments_enriched') }}
)

SELECT
    p.date_sk,
    p.shipment_date AS date,
    p.supplier_id,
    p.supplier_name,
    p.supplier_country,
    p.supplier_category,

    COUNT(*) AS total_shipments,
    SUM(IFF(p.is_on_time=1,1,0)) AS on_time_shipments,
    SUM(IFF(p.is_on_time=0,1,0)) AS delayed_shipments,

    (AVG(p.is_on_time))::number(10,4) AS on_time_delivery_rate,
    (AVG(p.delivery_delay_days))::number(10,2) AS avg_delivery_delay_days,
    PERCENTILE_CONT(0.5) WITHIN group (ORDER BY p.delivery_delay_days) as p50_delay_days,
    PERCENTILE_CONT(0.9) WITHIN group (ORDER BY p.delivery_delay_days) as p90_delay_days


FROM performance p

GROUP BY 1,2,3,4,5,6