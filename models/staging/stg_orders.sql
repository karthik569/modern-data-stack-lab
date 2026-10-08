with source as (
    select * from {{ source('raw_parquet', 'raw_orders') }}
)

select
    order_id,
    customer_id,
    cast(order_date as timestamp) as order_date,
    status,
    cast(total_amount as decimal(10, 2)) as total_amount
from source
