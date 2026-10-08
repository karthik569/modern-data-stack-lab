import os
import json
import random
from datetime import datetime, timedelta
from faker import Faker
import polars as pl

fake = Faker()
Faker.seed(42)
random.seed(42)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

def generate_customers(n=250):
    customers = []
    for cid in range(1, n + 1):
        customers.append({
            "customer_id": cid,
            "name": fake.name(),
            "email": fake.email(),
            "country": fake.country_code(),
            "created_at": (datetime.now() - timedelta(days=random.randint(10, 365))).isoformat()
        })
    return pl.DataFrame(customers)

def generate_orders_and_items(customers_df, num_orders=1000):
    cids = customers_df["customer_id"].to_list()
    orders = []
    items = []
    item_id = 1
    
    statuses = ["completed", "completed", "completed", "returned", "cancelled"]
    products = [
        {"id": 101, "name": "Mechanical Keyboard", "category": "Electronics", "price": 89.99},
        {"id": 102, "name": "Wireless Mouse", "category": "Electronics", "price": 39.50},
        {"id": 103, "name": "USB-C Hub", "category": "Accessories", "price": 24.99},
        {"id": 104, "name": "Standing Desk Mat", "category": "Office", "price": 49.00},
        {"id": 105, "name": "Noise Cancelling Headphones", "category": "Audio", "price": 199.99},
    ]

    for oid in range(1, num_orders + 1):
        order_date = datetime.now() - timedelta(days=random.randint(0, 90), hours=random.randint(0, 23))
        cid = random.choice(cids)
        num_items = random.randint(1, 4)
        order_total = 0.0

        for _ in range(num_items):
            prod = random.choice(products)
            qty = random.randint(1, 3)
            subtotal = round(prod["price"] * qty, 2)
            order_total += subtotal
            items.append({
                "order_item_id": item_id,
                "order_id": oid,
                "product_id": prod["id"],
                "product_name": prod["name"],
                "category": prod["category"],
                "unit_price": prod["price"],
                "quantity": qty,
                "subtotal": subtotal
            })
            item_id += 1

        orders.append({
            "order_id": oid,
            "customer_id": cid,
            "order_date": order_date.isoformat(),
            "status": random.choice(statuses),
            "total_amount": round(order_total, 2)
        })

    return pl.DataFrame(orders), pl.DataFrame(items)

def run():
    print("Generating synthetic e-commerce dataset...")
    df_customers = generate_customers()
    df_orders, df_items = generate_orders_and_items(df_customers)

    cust_path = os.path.join(DATA_DIR, "customers.parquet")
    ord_path = os.path.join(DATA_DIR, "orders.parquet")
    items_path = os.path.join(DATA_DIR, "order_items.parquet")

    df_customers.write_parquet(cust_path)
    df_orders.write_parquet(ord_path)
    df_items.write_parquet(items_path)

    print(f"Saved: {cust_path} ({len(df_customers)} rows)")
    print(f"Saved: {ord_path} ({len(df_orders)} rows)")
    print(f"Saved: {items_path} ({len(df_items)} rows)")

if __name__ == "__main__":
    run()
