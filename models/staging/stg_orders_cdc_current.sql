/*
Reconstructs current state of orders from an append-only CDC event log
(demonstrating the Outbox / Event Sourcing pattern using SQL Window Functions).
*/

with cdc_events as (
    select * from {{ source('raw_parquet', 'raw_cdc_events') }}
),

ranked_events as (
    select
        event_id,
        entity,
        op,
        order_id,
        customer_id,
        status,
        total_amount,
        cast(event_timestamp as timestamp) as event_timestamp,
        ts_ms,
        row_number() over (
            partition by order_id
            order by ts_ms desc
        ) as deduplication_rank
    from cdc_events
)

select
    order_id,
    customer_id,
    status as current_status,
    total_amount,
    event_timestamp as last_modified_at,
    op as last_operation
from ranked_events
where deduplication_rank = 1
