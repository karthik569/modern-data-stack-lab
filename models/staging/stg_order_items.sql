with source as (
    select * from {{ source('raw_parquet', 'raw_order_items') }}
)

select
    order_item_id,
    order_id,
    product_id,
    product_name,
    category,
    cast(unit_price as decimal(10, 2)) as unit_price,
    quantity,
    cast(subtotal as decimal(10, 2)) as subtotal
from source
