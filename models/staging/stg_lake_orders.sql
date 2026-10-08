/*
Staging model reading from Apache Hive-partitioned Parquet files
demonstrating partition pruning (year, month extracted directly from directory path).
*/

with source as (
    select * from {{ source('raw_parquet', 'lake_partitioned_orders') }}
)

select
    order_id,
    customer_id,
    cast(order_date as timestamp) as order_date,
    status,
    cast(total_amount as decimal(10, 2)) as total_amount,
    cast(year as integer) as order_year,
    cast(month as integer) as order_month
from source
