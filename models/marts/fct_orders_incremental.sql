{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='sync_all_columns'
    )
}}

with source as (
    select * from {{ source('raw_parquet', 'raw_orders') }}
),

transformed as (
    select
        order_id,
        customer_id,
        cast(order_date as timestamp) as order_date,
        status,
        cast(total_amount as decimal(10, 2)) as total_amount
    from source
)

select * from transformed

{% if is_incremental() %}
    -- High-watermark filter: only process new or updated orders
    where order_date > (select coalesce(max(order_date), '1970-01-01'::timestamp) from {{ this }})
{% endif %}
