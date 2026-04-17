-- snapshots/stores_snapshot.sql

{% snapshot stores_snapshot %}

{{
    config(
      unique_key='store_id',
      strategy='check',
      check_cols=['store_name', 'region', 'city', 'state', 'store_open_date']
    )
}}

SELECT
    store_id,
    store_name,
    region,
    city,
    state,
    store_open_date

FROM {{ ref('int_dedup_stores') }}

{% endsnapshot %}