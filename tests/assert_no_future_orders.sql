/*
Business Invariant Test:
Order timestamps must NEVER be in the future relative to the current timestamp.
A dbt test fails if any records are returned.
*/

select
    order_id,
    customer_id,
    order_date
from {{ ref('stg_orders') }}
where order_date > current_timestamp + interval 1 hour
