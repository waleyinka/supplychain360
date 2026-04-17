-- models/marts/facts/fct_product_stockouts.sql

{{ config(materialized='table', tags=['marts','fact']) }}

WITH stockouts AS (
    SELECT *
    FROM {{ ref('int_inventory_stock_status') }}
)

SELECT
    s.date_sk,
    s.snapshot_date AS date,
    s.warehouse_id,
    s.warehouse_city,
    s.warehouse_state,
    s.product_id,
    s.product_name,
    s.product_category,
    s.brand,

    COUNT(*) AS total_days,

    SUM(s.is_stockout) AS stockout_days,
    
    (SUM(s.is_stockout) / nullif(count(*),0))::number(10,4) AS stockout_rate,

    SUM(s.is_below_reorder_threshold) AS days_below_reorder_threshold,

    (SUM(s.is_below_reorder_threshold) / nullif(count(*),0))::number(10,4) AS below_order_rate,

    SUM(s.inventory_value) AS total_inventory_value

FROM stockouts s

GROUP BY 1,2,3,4,5,6,7,8,9