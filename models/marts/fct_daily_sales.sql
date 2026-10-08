with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
)

select
    date_trunc('day', o.order_date) as order_day,
    oi.category,
    count(distinct o.order_id) as total_orders,
    sum(oi.quantity) as items_sold,
    round(sum(oi.subtotal), 2) as gross_revenue,
    round(sum(case when o.status = 'completed' then oi.subtotal else 0 end), 2) as net_revenue
from orders o
join order_items oi
    on o.order_id = oi.order_id
group by 1, 2
order by 1 desc, 2
