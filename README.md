# Modern Data Stack Lab (Ubuntu PRoot on Termux)

A lightweight Modern Data Stack (MDS) pipeline built for ARM64 Linux inside PRoot.

> [!TIP]
> **New to Data Engineering from Java / Spring / Backend development?** Check out our dedicated guide: [JAVA_TO_DATA_ENGINEERING.md](file:///sdcard/Download/termux/modern-data-stack-lab/JAVA_TO_DATA_ENGINEERING.md) which maps core concepts (OLTP vs OLAP, JPA vs dbt, Flyway vs dbt DAGs, 3NF vs Kimball Star Schema) directly to Java patterns.


## Overview & Motivation

Traditional cloud data stacks (Snowflake, BigQuery, Databricks) carry significant infrastructure overhead, cost, and complexity for local prototyping, edge computing, and resource-constrained environments. 

**This repository demonstrates how to build and operate a full-featured, zero-cost Modern Data Stack locally on mobile/ARM64 Linux hardware (Android Termux + Ubuntu PRoot)** without sacrificing industry-standard data engineering patterns:

- **Zero-Cloud & Embedded OLAP**: Uses **DuckDB** and **Polars/Arrow** for columnar analytics with zero server processes or external daemons.
- **Modern ELT Pattern**: Follows standard Extract-Load-Transform semantics — data is ingested into immutable columnar storage (`.parquet`) and modeled into dimensional layers.
- **Production Transformation Tooling**: Integrates **dbt Core** (`dbt-duckdb`) with structured staging views and dimensional marts (`dim_customers`, `fct_daily_sales`).
- **Edge & Mobile Portability**: Engineered to run reliably on resource-constrained ARM64 devices under memory and CPU constraints.
- **Extensible Export Layer**: Built to easily syndicate modeled analytical marts downstream into operational databases (PostgreSQL / MariaDB) or BI dashboards.

### End-to-End Data Flow

```text
[ Synthetic Generator (Faker + Polars) ]
                   │
                   ▼ (Extract & Load)
      [ Columnar Raw Parquet Files ]
      - customers.parquet
      - orders.parquet
      - order_items.parquet
                   │
                   ▼ (Transform via DuckDB / dbt)
      ┌────────────────────────────┐
      │     DuckDB Warehouse       │
      │  ┌──────────────────────┐  │
      │  │ Staging Layer (Views)│  │
      │  └──────────┬───────────┘  │
      │             │              │
      │  ┌──────────▼───────────┐  │
      │  │ Marts Layer (Tables) │  │
      │  │ - dim_customers      │  │
      │  │ - fct_daily_sales    │  │
      │  └──────────────────────┘  │
      └─────────────┬──────────────┘
                    ▼ (Consume)
      [ OLAP Queries / Dashboards / Operational Sync ]
```

## Stack Architecture
- **Language & Runtime**: Python 3.12 (managed via `uv`)
- **Ingestion & Data Format**: Faker + Apache Arrow / Polars (`.parquet`)
- **Storage & Analytical Engine**: DuckDB (Zero-overhead embedded OLAP warehouse)
- **Transformation Layer**: Staging views + Marts dimensional models (`dim_customers`, `fct_daily_sales`)
- **Hybrid Target**: Ready for reverse-ETL / export to PostgreSQL or MariaDB

## Directory Structure
- `data/raw/`: Partitioned/raw Parquet files (`customers.parquet`, `orders.parquet`, `order_items.parquet`)
- `data/warehouse.duckdb`: Local OLAP DuckDB database file
- `generate_raw_data.py`: Raw synthetic e-commerce data generation
- `pipeline_runner.py`: ELT pipeline running DuckDB staging views and analytical marts
- `models/`: dbt-style SQL transformation models

## Data Modeling & Dimensional Architecture

The pipeline implements a two-tier transformation design separating raw ingestion views from analytical consumption marts.

### 1. Ingestion & Staging Layer (`staging`)
Raw Parquet datasets are mapped via 1-to-1 zero-copy views, normalizing naming, parsing timestamps, and casting numeric types:
- **`staging.stg_customers`**: Grain is **one customer** (`customer_id`). Cleans customer names, lowercases emails, casts registration timestamp.
- **`staging.stg_orders`**: Grain is **one order** (`order_id`). Foreign key `customer_id`. Contains order timestamp, status (`pending`, `completed`, `cancelled`), and total amount.
- **`staging.stg_order_items`**: Grain is **one line item** (`order_item_id`). Foreign key `order_id`. Contains SKU, category, unit price, quantity, and subtotal.

### 2. Marts Layer (`marts`)
Business-ready materialized tables optimized for dimensional reporting and OLAP aggregation:

#### `marts.dim_customers` (Customer Dimension)
- **Grain**: 1 row per customer (`customer_id`).
- **Primary / Foreign Keys**: Primary key `customer_id`.
- **Metrics & Attributes**:
  - `registered_at`: Original account creation timestamp.
  - `total_orders`: Cumulative count of orders placed.
  - `lifetime_spend`: Sum of `total_amount` across completed orders.
  - `first_order_date` & `most_recent_order_date`: Customer lifecycle activity window.

#### `marts.fct_daily_sales` (Daily Sales Mart)
- **Grain**: 1 row per **day** and **product category** (`order_day`, `category`).
- **Dimensions**: `order_day` (truncated to date), `category` (Electronics, Audio, Office, Accessories, etc.).
- **Metrics**:
  - `total_orders`: Count of distinct orders for the date and category.
  - `items_sold`: Total unit volume of items sold.
  - `gross_revenue`: Sum of all item subtotals.
  - `net_revenue`: Sum of subtotals strictly for `completed` orders.


## Getting Started

### 1. Prerequisites & Environment Setup

This project uses Python 3.12 managed via `uv` on ARM64 Linux (Ubuntu PRoot on Termux).

#### Option A: Using Pre-configured Environment
If you are working in the existing PRoot container, the dedicated virtualenv is ready at `/root/venvs/mds-lab`:
```bash
source /root/venvs/mds-lab/bin/activate
```

#### Option B: Fresh Setup with `uv`
To create a clean local virtual environment and install all dependencies:
```bash
# Create virtual environment
uv venv .venv --python 3.12

# Activate environment
source .venv/bin/activate

# Install dependencies from pyproject.toml
uv pip install -e .
```

---

### 2. Generate Synthetic Raw Data

Generate e-commerce raw datasets (`customers`, `orders`, and `order_items`) saved as compressed Parquet files under `data/raw/`:
```bash
python generate_raw_data.py
```

---

### 3. Run Transformations

You can run transformations using either the standalone DuckDB script or the dbt project:

#### Method A: Direct DuckDB ELT Runner
Runs lightweight DuckDB staging views and analytical marts with zero external overhead:
```bash
python pipeline_runner.py
```

#### Method B: dbt Core Transformations
Execute SQL transformations, dimensional models, and marts using dbt:
```bash
dbt run --profiles-dir .
```

---

### 4. Data Quality & Contract Testing

Run automated data quality tests verifying primary keys, null checks, allowed value sets, and referential integrity:
```bash
dbt test --profiles-dir .
```

The test suite executes **29 assertions** across two categories:
- **Generic Schema Contracts (26 tests)**:
  - Primary key uniqueness and non-nullability on `customer_id`, `order_id`, and `order_item_id`.
  - Referential integrity (`relationships` tests) ensuring `orders.customer_id -> customers.customer_id` and `order_items.order_id -> orders.order_id`.
  - Allowed status values (`accepted_values`: `['completed', 'returned', 'cancelled']`).
  - Non-null metrics on revenue, counts, and date grains across `dim_customers` and `fct_daily_sales`.
- **Singular Business Invariant Tests (`tests/*.sql` - 3 tests)**:
  - `assert_net_revenue_le_gross_revenue`: Validates net revenue never exceeds gross revenue.
  - `assert_no_future_orders`: Asserts order dates are not set in the future.
  - `assert_positive_item_amounts`: Asserts quantities, unit prices, and subtotals are strictly positive (> 0).

---

### 5. Pipeline Orchestration with Dagster

The lab features a unified orchestration layer using **Dagster Software-Defined Assets (SDA)**:
- **`raw_parquet_data`**: Orchestrates synthetic ingestion into `data/raw/*.parquet`.
- **`modern_data_stack_dbt_assets`**: Translates dbt models into first-class Dagster assets, streaming `dbt build` (transformations + tests) into the lineage graph.
- **`mds_full_pipeline_job`**: End-to-end execution job.
- **`daily_mds_pipeline_schedule`**: Configured daily cron schedule (`0 0 * * *`).

#### Run the Full Pipeline via Dagster CLI:
```bash
PYTHONPATH=src:. dagster job execute -m modern_data_stack_lab -j mds_full_pipeline_job
```

#### Launch Dagster Web UI (Development Server):
```bash
PYTHONPATH=src:. dagster dev -m modern_data_stack_lab -h 0.0.0.0 -p 3000
```
Open `http://localhost:3000` to inspect the visual dependency DAG, launch runs, and view asset health.

---

### 6. Querying the Warehouse

Query the local DuckDB warehouse (`data/warehouse.duckdb`) directly via Python or the DuckDB CLI.

#### Using Python
```python
import duckdb

con = duckdb.connect("data/warehouse.duckdb")

# Inspect top customers by lifetime spend
print(con.execute("SELECT * FROM marts.dim_customers LIMIT 5;").pl())

# Inspect daily revenue by product category
print(con.execute("SELECT * FROM marts.fct_daily_sales ORDER BY order_day DESC LIMIT 5;").pl())
```

#### Using DuckDB CLI
```bash
duckdb data/warehouse.duckdb -c "SELECT * FROM marts.dim_customers LIMIT 5;"
```

---

### 7. Reverse-ETL: Syncing Marts to Operational Databases

Syndicate modeled analytical marts from DuckDB back into operational databases (SQLite or PostgreSQL) where backend services (e.g., Spring Boot, REST APIs) can query enriched profiles at low latency:

```bash
# Syncs marts into operational SQLite (data/operational.db)
python reverse_etl.py

# Or export to PostgreSQL:
POSTGRES_URL="postgresql://user:password@localhost:5432/appdb" python reverse_etl.py
```

Inspect synced operational SQLite tables:
```bash
sqlite3 data/operational.db "SELECT customer_name, lifetime_spend, synced_at FROM operational_customer_profiles LIMIT 5;"
```

---

### 8. Interactive Terminal BI Dashboard

Visualize executive KPIs, category revenue distributions, VIP customer rankings, and daily velocity directly in your terminal with zero browser/server overhead:

```bash
python dashboard.py
```

Features included:
- **Executive Metric Cards**: Cohort size, completed net revenue, total order volume, and average customer LTV.
- **Category Share Bar Chart**: Units sold, net revenue, and proportional share bars.
- **VIP Customer Segment Table**: Top lifetime value customers, order history, and activity dates.
- **Daily Velocity Trend Chart**: Recent 6-day order volumes and revenue bars.



