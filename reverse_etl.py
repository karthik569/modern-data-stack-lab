"""
Reverse-ETL Sync Tool
--------------------
Syncs analytical dimensional marts from the DuckDB analytical warehouse
into operational databases (SQLite / PostgreSQL) where transactional applications
(e.g., Spring Boot, JPA/Hibernate, Node.js) consume them for operational workflows
(such as customer dashboards, CRM enrichment, and marketing segmentations).
"""

import os
import sys
import duckdb
import sqlite3
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
DUCKDB_PATH = BASE_DIR / "data" / "warehouse.duckdb"
DEFAULT_SQLITE_PATH = BASE_DIR / "data" / "operational.db"

def sync_to_sqlite(sqlite_path: Path = DEFAULT_SQLITE_PATH):
    """
    Syncs marts from DuckDB into an operational SQLite database.
    Zero server daemons needed — perfect for local testing and lightweight services.
    """
    print(f"[*] Reading analytical marts from DuckDB: {DUCKDB_PATH}")
    con_duck = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    print(f"[*] Connecting to operational SQLite database: {sqlite_path}")
    con_sqlite = sqlite3.connect(str(sqlite_path))
    cursor = con_sqlite.cursor()

    # 1. Sync dim_customers
    df_customers = con_duck.execute("""
        SELECT 
            customer_id,
            customer_name,
            email,
            country,
            CAST(registered_at AS TEXT) AS registered_at,
            total_orders,
            CAST(lifetime_spend AS FLOAT) AS lifetime_spend,
            CAST(first_order_date AS TEXT) AS first_order_date,
            CAST(most_recent_order_date AS TEXT) AS most_recent_order_date
        FROM marts.dim_customers
    """).pl()

    cursor.execute("DROP TABLE IF EXISTS operational_customer_profiles;")
    cursor.execute("""
        CREATE TABLE operational_customer_profiles (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            email TEXT,
            country TEXT,
            registered_at TEXT,
            total_orders INTEGER,
            lifetime_spend REAL,
            first_order_date TEXT,
            most_recent_order_date TEXT,
            synced_at TEXT
        );
    """)

    synced_at = datetime.now().isoformat()
    rows_customers = [
        (
            row["customer_id"],
            row["customer_name"],
            row["email"],
            row["country"],
            row["registered_at"],
            row["total_orders"],
            float(row["lifetime_spend"]) if row["lifetime_spend"] is not None else 0.0,
            row["first_order_date"],
            row["most_recent_order_date"],
            synced_at
        )
        for row in df_customers.iter_rows(named=True)
    ]

    cursor.executemany("""
        INSERT INTO operational_customer_profiles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows_customers)
    print(f"  ✓ Synced {len(rows_customers)} rows into SQLite table: operational_customer_profiles")

    # 2. Sync fct_daily_sales
    df_sales = con_duck.execute("""
        SELECT 
            CAST(order_day AS TEXT) AS order_day,
            category,
            total_orders,
            CAST(items_sold AS INTEGER) AS items_sold,
            CAST(gross_revenue AS FLOAT) AS gross_revenue,
            CAST(net_revenue AS FLOAT) AS net_revenue
        FROM marts.fct_daily_sales
    """).pl()

    cursor.execute("DROP TABLE IF EXISTS operational_daily_category_metrics;")
    cursor.execute("""
        CREATE TABLE operational_daily_category_metrics (
            order_day TEXT,
            category TEXT,
            total_orders INTEGER,
            items_sold INTEGER,
            gross_revenue REAL,
            net_revenue REAL,
            synced_at TEXT,
            PRIMARY KEY (order_day, category)
        );
    """)

    rows_sales = [
        (
            row["order_day"],
            row["category"],
            row["total_orders"],
            row["items_sold"],
            float(row["gross_revenue"]),
            float(row["net_revenue"]),
            synced_at
        )
        for row in df_sales.iter_rows(named=True)
    ]

    cursor.executemany("""
        INSERT INTO operational_daily_category_metrics VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows_sales)
    print(f"  ✓ Synced {len(rows_sales)} rows into SQLite table: operational_daily_category_metrics")

    con_sqlite.commit()
    con_sqlite.close()
    con_duck.close()
    print("✓ SQLite Reverse-ETL sync completed successfully!")

def sync_to_postgres(pg_url: str):
    """
    Syncs marts from DuckDB into an operational PostgreSQL database using SQLAlchemy.
    """
    try:
        from sqlalchemy import create_engine
    except ImportError:
        print("[!] SQLAlchemy not installed. Run `uv pip install sqlalchemy`", file=sys.stderr)
        sys.exit(1)

    print(f"[*] Connecting to operational PostgreSQL warehouse target: {pg_url}")
    engine = create_engine(pg_url)
    con_duck = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    df_customers = con_duck.execute("SELECT * FROM marts.dim_customers").arrow()
    import pyarrow.dataset as ds
    import pandas as pd
    df_pd = df_customers.to_pandas()
    df_pd["synced_at"] = datetime.now()
    df_pd.to_sql("operational_customer_profiles", engine, if_exists="replace", index=False)
    print(f"  ✓ Synced {len(df_pd)} rows to PostgreSQL: operational_customer_profiles")

    df_sales = con_duck.execute("SELECT * FROM marts.fct_daily_sales").arrow().to_pandas()
    df_sales["synced_at"] = datetime.now()
    df_sales.to_sql("operational_daily_category_metrics", engine, if_exists="replace", index=False)
    print(f"  ✓ Synced {len(df_sales)} rows to PostgreSQL: operational_daily_category_metrics")

    con_duck.close()
    print("✓ PostgreSQL Reverse-ETL sync completed successfully!")

def main():
    pg_url = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
    if pg_url:
        sync_to_postgres(pg_url)
    else:
        sync_to_sqlite()

if __name__ == "__main__":
    main()
