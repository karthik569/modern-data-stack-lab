import os
import json
import uuid
import random
from datetime import datetime, timedelta
import polars as pl

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EVENTS_DIR = os.path.join(BASE_DIR, "data", "events")
os.makedirs(EVENTS_DIR, exist_ok=True)

def simulate_cdc_stream(num_orders=100):
    """
    Simulates transactional database Change Data Capture (CDC) events
    as produced by Debezium / Kafka Connect from a transactional Java/Spring Boot app.
    
    Event schema follows standard Debezium envelope:
      - event_id: unique UUID
      - op: 'c' (create/insert), 'u' (update), 'd' (delete)
      - entity: table/aggregate name ('orders')
      - payload: JSON snapshot or fields
      - ts_ms: event timestamp in epoch milliseconds
    """
    events = []
    base_time = datetime.now() - timedelta(hours=6)
    
    for order_id in range(1001, 1001 + num_orders):
        customer_id = random.randint(1, 250)
        total_amount = round(random.uniform(25.0, 450.0), 2)
        
        # 1. Event: Insert (Order Created) - op = 'c'
        t_create = base_time + timedelta(minutes=random.randint(0, 120))
        events.append({
            "event_id": str(uuid.uuid4()),
            "entity": "orders",
            "op": "c",
            "key": str(order_id),
            "order_id": order_id,
            "customer_id": customer_id,
            "status": "pending",
            "total_amount": total_amount,
            "event_timestamp": t_create.isoformat(),
            "ts_ms": int(t_create.timestamp() * 1000)
        })
        
        # 2. Event: Update (Payment Completed or Cancelled) - op = 'u'
        t_update = t_create + timedelta(minutes=random.randint(5, 60))
        next_status = random.choices(["completed", "cancelled"], weights=[0.85, 0.15])[0]
        events.append({
            "event_id": str(uuid.uuid4()),
            "entity": "orders",
            "op": "u",
            "key": str(order_id),
            "order_id": order_id,
            "customer_id": customer_id,
            "status": next_status,
            "total_amount": total_amount,
            "event_timestamp": t_update.isoformat(),
            "ts_ms": int(t_update.timestamp() * 1000)
        })
        
        # 3. Occasional Refund / Return update
        if next_status == "completed" and random.random() < 0.1:
            t_refund = t_update + timedelta(hours=random.randint(1, 4))
            events.append({
                "event_id": str(uuid.uuid4()),
                "entity": "orders",
                "op": "u",
                "key": str(order_id),
                "order_id": order_id,
                "customer_id": customer_id,
                "status": "returned",
                "total_amount": total_amount,
                "event_timestamp": t_refund.isoformat(),
                "ts_ms": int(t_refund.timestamp() * 1000)
            })

    # Sort events chronologically to mimic streaming journal
    events.sort(key=lambda x: x["ts_ms"])
    
    df_events = pl.DataFrame(events)
    output_path = os.path.join(EVENTS_DIR, "order_cdc_events.parquet")
    df_events.write_parquet(output_path)
    print(f"Generated {len(df_events)} CDC events across {num_orders} orders.")
    print(f"Saved append-only event stream to: {output_path}")
    return df_events

if __name__ == "__main__":
    simulate_cdc_stream()
