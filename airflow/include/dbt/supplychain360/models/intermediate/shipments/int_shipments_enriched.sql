-- models/intermediate/shipments/int_shipments_enriched.sql

{{ config(
    materialized='view',
    tags=['intermediate', 'shipments']
) }}

WITH shipments AS (
    SELECT * FROM {{ ref('int_dedup_shipments') }}
),

products AS (
    SELECT * FROM {{ ref('products_snapshot') }}
),

suppliers AS (
    SELECT * FROM {{ ref('suppliers_snapshot') }}
),

stores AS (
    SELECT * FROM {{ ref('stores_snapshot') }}
),

warehouse AS (
    SELECT * FROM {{ ref('warehouses_snapshot') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
)

SELECT
    d.date_sk,
    sh.shipment_id,
    sh.shipment_date,
    sh.expected_delivery_date,
    sh.actual_delivery_date,

    sh.warehouse_id,
    wh.city              AS warehouse_city,
    wh.state             AS warehouse_state,

    sh.store_id,
    st.store_name,
    st.region,

    sh.product_id,
    p.product_name,
    p.category            AS product_category,
    p.brand,

    p.supplier_id,
    su.supplier_name,
    su.country            AS supplier_country,
    su.category           AS supplier_category,

    sh.quantity_shipped,
    sh.carrier,

    DATEDIFF('day', sh.expected_delivery_date, sh.actual_delivery_date) AS delivery_delay_days,
    IFF(sh.actual_delivery_date <= sh.expected_delivery_date, 1, 0) AS is_on_time,
    DATEDIFF('day', sh.shipment_date, sh.actual_delivery_date) AS actual_lead_time_days

FROM shipments sh
    
LEFT JOIN dates d
    ON sh.shipment_date = d.date
    
LEFT JOIN products p
    ON sh.product_id = p.product_id
    AND sh.shipment_date BETWEEN p.dbt_valid_from AND COALESCE(p.dbt_valid_to, '9999-12-31')
    
LEFT JOIN suppliers su
    ON p.supplier_id = su.supplier_id
    AND sh.shipment_date BETWEEN su.dbt_valid_from AND coalesce(su.dbt_valid_to,'9999-12-31')

LEFT JOIN stores st
    ON sh.store_id = st.store_id
    AND sh.shipment_date BETWEEN st.dbt_valid_from AND coalesce(st.dbt_valid_to,'9999-12-31')

LEFT JOIN warehouse wh
    ON sh.warehouse_id = wh.warehouse_id
    AND sh.shipment_date BETWEEN wh.dbt_valid_from AND coalesce(wh.dbt_valid_to,'9999-12-31')