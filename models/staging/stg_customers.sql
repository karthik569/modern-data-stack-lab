with source as (
    select * from {{ source('raw_parquet', 'raw_customers') }}
)

select
    customer_id,
    name as customer_name,
    lower(email) as email,
    country,
    cast(created_at as timestamp) as created_at
from source
