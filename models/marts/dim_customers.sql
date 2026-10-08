with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        coalesce(sum(case when status = 'completed' then total_amount else 0 end), 0) as lifetime_spend
    from orders
    group by customer_id
)

select
    c.customer_id,
    c.customer_name,
    c.email,
    c.country,
    c.created_at as registered_at,
    coalesce(co.number_of_orders, 0) as total_orders,
    coalesce(co.lifetime_spend, 0) as lifetime_spend,
    co.first_order_date,
    co.most_recent_order_date
from customers c
left join customer_orders co
    on c.customer_id = co.customer_id
