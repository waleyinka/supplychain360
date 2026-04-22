-- models/intermediate/sales/int_store_sales_enriched.sql

{{
  config(
    materialized='view',
    tags=['intermediate', 'sales']
  )
}}

WITH sales AS (
    SELECT * FROM {{ ref('int_dedup_store_sales') }}
),

products AS (
    SELECT * FROM {{ ref('products_snapshot') }}
),

stores AS (
    SELECT * FROM {{ ref('stores_snapshot') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
)

SELECT
    d.date_sk,
    DATE(s.transaction_timestamp) AS sales_date,

    s.transaction_id,
    s.store_id,
    st.store_name,
    st.region,
    st.city         AS store_city,
    st.state        AS store_state,

    s.product_id,
    p.product_name,
    p.category       AS product_category,
    p.brand,
    
    s.quantity_sold,    
    s.unit_price,
    s.discount_pct,
    s.sale_amount,
    s.sale_amount * (1 - COALESCE(s.discount_pct, 0)) AS net_sale_amount,

    s.transaction_timestamp
    
FROM sales s
    
LEFT JOIN dates d
    ON DATE(s.transaction_timestamp) = d.date
        
LEFT JOIN products p
    ON s.product_id = p.product_id
    AND s.transaction_timestamp BETWEEN p.dbt_valid_from AND COALESCE(p.dbt_valid_to, '9999-12-31')
    
LEFT JOIN stores st
    ON s.store_id = st.store_id
    AND s.transaction_timestamp BETWEEN st.dbt_valid_from AND COALESCE(st.dbt_valid_to, '9999-12-31')