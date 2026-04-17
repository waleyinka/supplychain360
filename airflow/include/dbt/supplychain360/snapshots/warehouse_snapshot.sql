{% snapshot warehouses_snapshot %}

{{
    config(
      unique_key='warehouse_id',
      strategy='check',
      check_cols=['city', 'state']
    )
}}

SELECT
    warehouse_id,
    city,
    state
    
FROM {{ ref('int_dedup_warehouses') }}

{% endsnapshot %}
