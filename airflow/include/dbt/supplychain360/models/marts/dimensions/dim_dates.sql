-- models/marts/dimensions/dim_dates.sql

{{ config(materialized='table', tags=['marts','dim']) }}

WITH date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('2020-01-01')",
        end_date="to_date('2030-12-31')"
    ) }}

)

SELECT
    -- surrogate key
    {{ generate_surrogate_key(['date_day']) }} AS date_sk,
    
    date_day AS date,

    EXTRACT(YEAR FROM date_day)        AS year,
    EXTRACT(QUARTER FROM date_day)     AS quarter,
    EXTRACT(MONTH FROM date_day)       AS month,
    EXTRACT(DAY FROM date_day)         AS day,

    EXTRACT(DAYOFWEEK FROM date_day)   AS day_of_week,
    EXTRACT(WEEK FROM date_day)        AS week_of_year,

    TRIM(TO_CHAR(date_day, 'Day'))     AS day_name,
    TRIM(TO_CHAR(date_day, 'Month'))   AS month_name,

    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (6,7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    CASE 
        WHEN EXTRACT(MONTH FROM date_day) IN (12,1,2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM date_day) IN (3,4,5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date_day) IN (6,7,8) THEN 'Summer'
        ELSE 'Autumn'
    END AS season

FROM date_spine