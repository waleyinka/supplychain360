-- models/marts/facts/fct_regional_sales_demand.sql

{{ config(materialized='table', tags=['marts','fact']) }}

WITH demand AS (
    SELECT *
    FROM {{ ref('int_product_demand_trends') }}
)

SELECT
    d.date_sk,
    d.sales_date            AS date,
    d.region,
    d.product_id,
    d.product_name,
    d.product_category,
    d.brand,

    SUM(d.daily_units_sold)        AS daily_units_sold,
    SUM(d.daily_net_sales)         AS daily_net_sales,

    AVG(d.rolling_7d_units_avg)   AS avg_rolling_7d_units_sold,
    
    (
    SUM(d.demand_volatility_index * d.daily_units_sold)
    / nullif(SUM(d.daily_units_sold),0)
    )::number(10,4) AS weighted_demand_volatility_index

FROM demand d

GROUP BY 1,2,3,4,5,6,7