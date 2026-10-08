import os
import duckdb
import polars as pl
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "warehouse.duckdb"

def run_elt():
    print(f"Connecting to DuckDB analytical warehouse: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))
    
    # 1. Staging Schema & Views
    con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    con.execute(f"""
        CREATE OR REPLACE VIEW staging.stg_customers AS
        SELECT
            customer_id,
            name AS customer_name,
            lower(email) AS email,
            country,
            CAST(created_at AS TIMESTAMP) AS created_at
        FROM read_parquet('{RAW_DIR}/customers.parquet');
    """)
    print("✓ Staging view created: staging.stg_customers")
    
    con.execute(f"""
        CREATE OR REPLACE VIEW staging.stg_orders AS
        SELECT
            order_id,
            customer_id,
            CAST(order_date AS TIMESTAMP) AS order_date,
            status,
            CAST(total_amount AS DECIMAL(10, 2)) AS total_amount
        FROM read_parquet('{RAW_DIR}/orders.parquet');
    """)
    print("✓ Staging view created: staging.stg_orders")
    
    con.execute(f"""
        CREATE OR REPLACE VIEW staging.stg_order_items AS
        SELECT
            order_item_id,
            order_id,
            product_id,
            product_name,
            category,
            CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
            quantity,
            CAST(subtotal AS DECIMAL(10, 2)) AS subtotal
        FROM read_parquet('{RAW_DIR}/order_items.parquet');
    """)
    print("✓ Staging view created: staging.stg_order_items")
    
    # 2. Marts Schema & Materialized Analytical Tables
    con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
    
    con.execute("""
        CREATE OR REPLACE TABLE marts.dim_customers AS
        WITH customer_orders AS (
            SELECT
                customer_id,
                MIN(order_date) AS first_order_date,
                MAX(order_date) AS most_recent_order_date,
                COUNT(order_id) AS total_orders,
                COALESCE(SUM(CASE WHEN status = 'completed' THEN total_amount ELSE 0 END), 0) AS lifetime_spend
            FROM staging.stg_orders
            GROUP BY customer_id
        )
        SELECT
            c.customer_id,
            c.customer_name,
            c.email,
            c.country,
            c.created_at AS registered_at,
            COALESCE(co.total_orders, 0) AS total_orders,
            COALESCE(co.lifetime_spend, 0) AS lifetime_spend,
            co.first_order_date,
            co.most_recent_order_date
        FROM staging.stg_customers c
        LEFT JOIN customer_orders co
            ON c.customer_id = co.customer_id;
    """)
    print("✓ Analytical mart built: marts.dim_customers")
    
    con.execute("""
        CREATE OR REPLACE TABLE marts.fct_daily_sales AS
        SELECT
            date_trunc('day', o.order_date) AS order_day,
            oi.category,
            COUNT(DISTINCT o.order_id) AS total_orders,
            SUM(oi.quantity) AS items_sold,
            ROUND(SUM(oi.subtotal), 2) AS gross_revenue,
            ROUND(SUM(CASE WHEN o.status = 'completed' THEN oi.subtotal ELSE 0 END), 2) AS net_revenue
        FROM staging.stg_orders o
        JOIN staging.stg_order_items oi
            ON o.order_id = oi.order_id
        GROUP BY 1, 2
        ORDER BY 1 DESC, 2;
    """)
    print("✓ Analytical mart built: marts.fct_daily_sales")
    
    # Sample Query verification
    print("\n--- Analytics Sample: Top 5 High-Value Customers ---")
    res = con.execute("""
        SELECT customer_id, customer_name, country, total_orders, lifetime_spend 
        FROM marts.dim_customers 
        ORDER BY lifetime_spend DESC 
        LIMIT 5;
    """).pl()
    print(res)
    
    print("\n--- Analytics Sample: Recent Daily Revenue by Category ---")
    res2 = con.execute("""
        SELECT order_day, category, total_orders, items_sold, net_revenue 
        FROM marts.fct_daily_sales 
        ORDER BY order_day DESC, net_revenue DESC 
        LIMIT 6;
    """).pl()
    print(res2)
    
    con.close()
    print("\nPipeline execution complete! Data safely written to DuckDB warehouse.")

if __name__ == "__main__":
    run_elt()
