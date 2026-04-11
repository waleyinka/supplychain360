-- snapshots/products_snapshot.sql

{% snapshot products_snapshot %}

{{
    config(
      unique_key='product_id',
      strategy='check',
      check_cols=['product_name', 'category', 'brand', 'supplier_id', 'unit_price',]
    )
}}

SELECT
    product_id,
    product_name,
    category,
    brand,
    supplier_id,
    unit_price

FROM {{ ref('int_dedup_products') }}

{% endsnapshot %}