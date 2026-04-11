-- snapshots/suppliers_snapshot.sql

{% snapshot suppliers_snapshot %}

{{
    config(
      unique_key='supplier_id',
      strategy='check',
      check_cols=['supplier_name', 'country', 'category',]
    )
}}

SELECT
    supplier_id,
    supplier_name,
    country,
    category

FROM {{ ref('int_dedup_suppliers') }}

{% endsnapshot %}