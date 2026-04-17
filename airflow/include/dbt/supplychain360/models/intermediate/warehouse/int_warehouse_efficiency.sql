-- models/intermediate/warehouse/int_warehouse_efficiency.sql


{{
  config(
    materialized='view',
    tags=['intermediate', 'warehouse']
  )
}}


WITH inventory AS (
    SELECT * FROM {{ ref('int_inventory_stock_status') }}
),

sales AS (
    SELECT * FROM {{ ref('int_store_sales_enriched') }}
),

map AS (
    SELECT * FROM {{ ref('int_store_warehouse_map') }}
),

warehouses AS (
    SELECT * FROM {{ ref('warehouses_snapshot') }}
)

SELECT
    wh.warehouse_id,
    wh.city,
    wh.state,

    AVG(inv.inventory_value)    AS avg_inventory_value,
    AVG(inv.quantity_available) AS avg_inventory_units,

    SUM(sales.net_sale_amount)  AS total_net_sales,
    SUM(sales.quantity_sold)    AS total_units_sold,

    CASE
        WHEN AVG(inv.inventory_value) = 0 THEN NULL
        ELSE SUM(sales.net_sale_amount) / AVG(inv.inventory_value)
    END AS inventory_turnover_ratio

FROM warehouses wh

LEFT JOIN inventory inv ON wh.warehouse_id = inv.warehouse_id

LEFT JOIN map ON wh.warehouse_id = map.warehouse_id

LEFT JOIN sales ON map.store_id = sales.store_id

GROUP BY 1,2,3