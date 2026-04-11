-- models/intermediate/suppliers/int_supplier_delivery_performance.sql

{{
  config(
    materialized='view',
    tags=['intermediate', 'suppliers', 'delivery_performance'],
  )
}}

WITH shipments AS (
    SELECT *
    FROM {{ ref('int_dedup_shipments') }}
),

suppliers AS (
    SELECT *
    FROM {{ ref('suppliers_snapshot') }}
),

dates AS (
    SELECT *
    FROM {{ ref('dim_dates') }}
),

products AS (
    SELECT *
    FROM {{ ref('products_snapshot') }}
),

suppliers_enriched AS (
    SELECT
        d.date_sk,
        DATE(sh.shipment_date) AS date,

        sh.shipment_id,
        sh.warehouse_id,
        sh.store_id,
        sh.product_id,
        sh.carrier,
        
        -- Dates
        DATE(sh.shipment_date)                  AS shipment_date,
        DATE(sh.expected_delivery_date)         AS expected_delivery_date,
        DATE(sh.actual_delivery_date)           AS actual_delivery_date,

        -- Delivery delay
        DATEDIFF(
            'day',
            DATE(sh.expected_delivery_date),
            DATE(sh.actual_delivery_date)
        )                                       AS delivery_delay_days,

        -- On-time flag
        CASE
            WHEN DATE(sh.actual_delivery_date) <= DATE(sh.expected_delivery_date)
                THEN 1
            ELSE 0
        END                                     AS is_on_time,

        -- Actual lead time
        DATEDIFF(
            'day',
            DATE(sh.shipment_date),
            DATE(sh.actual_delivery_date)
        )                                       AS actual_lead_time_days,

         -- Supplier enrichment
        p.supplier_id,
        su.supplier_name,
        su.country                              AS supplier_country,
        su.category                             AS supplier_category

    FROM shipments sh
    
    LEFT JOIN dates d
        ON DATE(sh.shipment_date) = d.date
        
    LEFT JOIN products p
        ON sh.product_id = p.product_id
        AND sh.shipment_date BETWEEN p.dbt_valid_from AND COALESCE(p.dbt_valid_to, '9999-12-31')
    
    LEFT JOIN suppliers su
        ON p.supplier_id = su.supplier_id
        AND sh.shipment_date BETWEEN su.dbt_valid_from AND COALESCE(su.dbt_valid_to, '9999-12-31')

)

SELECT *
FROM suppliers_enriched