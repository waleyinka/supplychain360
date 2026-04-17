-- models/intermediate/inventory/int_inventory_stock_status.sql

{{ config(
    materialized='table',
    tags=['intermediate', 'inventory', 'stock_status']
) }}


WITH inventory AS (
    SELECT * FROM {{ ref('int_dedup_inventory') }}
),

products AS (
    SELECT * FROM {{ ref('products_snapshot') }}
),

warehouses AS (
    SELECT * FROM {{ ref('warehouses_snapshot') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
)

SELECT
    d.date_sk,
    inv.snapshot_date    AS snapshot_date,

    inv.warehouse_id,
    wh.city              AS warehouse_city,
    wh.state             AS warehouse_state,

    inv.product_id,
    p.product_name,
    p.category           AS product_category,
    p.brand,
    p.unit_price,

    inv.quantity_available,
    inv.reorder_threshold,

    iff(inv.quantity_available=0,1,0) AS is_stockout,
    iff(inv.quantity_available < inv.reorder_threshold,1,0) AS is_below_reorder_threshold,

    (inv.quantity_available * p.unit_price) AS inventory_value,
    
FROM inventory inv
    
LEFT JOIN dates d
    ON inv.snapshot_date = d.date
    
LEFT JOIN products p
    ON inv.product_id = p.product_id
    AND inv.snapshot_date BETWEEN p.dbt_valid_from AND COALESCE(p.dbt_valid_to, '9999-12-31')
    
LEFT JOIN warehouses wh
    ON inv.warehouse_id = wh.warehouse_id
    AND inv.snapshot_date BETWEEN wh.dbt_valid_from AND COALESCE(wh.dbt_valid_to, '9999-12-31')