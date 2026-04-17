-- models/intermediate/sales/int_product_demand_trends.sql

{{
  config(
    materialized='table',
    tags=['intermediate', 'sales']
  )
}}

WITH sales_enriched AS (
    SELECT *
    FROM {{ ref('int_store_sales_enriched') }}
),

daily AS (
    SELECT
        date_sk,
        DATE(transaction_timestamp) AS sales_date,
        product_id,
        product_name,
        product_category,
        brand,
        store_id,
        region,
        SUM(quantity_sold)      AS daily_units_sold,
        SUM(net_sale_amount)    AS daily_net_sales
    
    FROM sales_enriched
    
    GROUP BY 1,2,3,4,5,6,7,8
),

rolled AS (
    SELECT
        *,

        AVG(daily_units_sold) OVER (
            PARTITION BY product_id, store_id
            ORDER BY sales_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_units_avg,

        STDDEV(daily_units_sold) OVER (
            PARTITION BY product_id, store_id
            ORDER BY sales_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_units_stddev
    
    FROM daily
)

SELECT
    date_sk,
    sales_date,
    product_id,
    product_name,
    product_category,
    brand,
    store_id,
    region,
    daily_units_sold,
    daily_net_sales,
    rolling_7d_units_avg,
    rolling_7d_units_stddev,
    CASE
        WHEN rolling_7d_units_avg IS NULL OR rolling_7d_units_avg = 0 THEN NULL
        ELSE rolling_7d_units_stddev / rolling_7d_units_avg
    END AS demand_volatility_index
    
    FROM rolled
