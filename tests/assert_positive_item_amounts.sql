/*
Business Invariant Test:
Order item unit prices and subtotals must always be strictly positive (> 0).
A dbt test fails if any records are returned.
*/

select
    order_item_id,
    order_id,
    product_name,
    unit_price,
    quantity,
    subtotal
from {{ ref('stg_order_items') }}
where unit_price <= 0 or subtotal <= 0 or quantity <= 0
