-- models/marts/facts/fct_warehouse_efficiency.sql

{{ config(materialized='table', tags=['marts','fact']) }}

SELECT * FROM {{ ref('int_warehouse_efficiency') }}