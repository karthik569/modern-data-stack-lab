/*
Business Invariant Test:
Net revenue must NEVER exceed gross revenue (net = gross minus cancellations/returns).
A dbt test fails if any records are returned.
*/

select
    order_day,
    category,
    gross_revenue,
    net_revenue
from {{ ref('fct_daily_sales') }}
where net_revenue > gross_revenue
