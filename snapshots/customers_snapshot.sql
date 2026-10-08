{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_name', 'email', 'country']
    )
}}

select
    customer_id,
    customer_name,
    email,
    country,
    created_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
