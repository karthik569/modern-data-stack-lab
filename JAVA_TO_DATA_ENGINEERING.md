# Data Engineering Primer for Java Developers

Welcome! If you come from a Java, Spring Boot, or transactional backend engineering background, modern data engineering can feel full of buzzwords (OLAP, Parquet, dbt, Marts, Kimball). 

This guide bridges the gap by mapping core Data Engineering (DE) paradigms directly to familiar Java and relational database (RDBMS) patterns, using the codebase in this repository as a concrete reference.

---

## 1. High-Level Mental Model: Software Engineering vs Data Engineering

| Java / Backend Engineering (SWE) | Data Engineering (DE) | In This Repository |
| :--- | :--- | :--- |
| **Primary Goal** | High concurrency, low latency per transaction (CRUD) | High throughput, massive batch scans & aggregations |
| **Storage Engine** | Row-oriented RDBMS (PostgreSQL, MySQL, Oracle) | Columnar storage (**Parquet**, **DuckDB**) |
| **Schema Paradigm** | Third Normal Form (3NF) to prevent duplicate writes | Dimensional Modeling (Kimball Star Schema / Denormalized) |
| **Data Processing** | Java Streams, OOP entities, ORMs (Hibernate/JPA) | Vectorized columnar engines (**Polars**, SQL in **DuckDB**) |
| **Transformation Logic** | Service layer business logic & DB migrations (Flyway/Liquibase) | Declarative analytical SQL models (**dbt**) |
| **Orchestration** | Quartz, Spring Scheduled, Message Queues (RabbitMQ/Kafka) | DAG Orchestrators (**Dagster**, Airflow) |

---

## 2. OLTP vs OLAP: Row-Oriented vs Column-Oriented

### The Java / OLTP World (Row Store)
In transactional backend systems (OLTP):
- You read or write **one entire row/entity** at a time (e.g. `UserRepository.findById(Long id)`).
- Hard disks/pages store all fields of customer #1 adjacent to each other:
  `[id=1, name="Alice", email="alice@corp.com", ...][id=2, name="Bob", ...]`
- Great for quick point-lookups and single-record writes (`INSERT INTO orders VALUES (...)`).
- **Terrible for analytics**: If you run `SELECT AVG(total_amount) FROM orders;`, the engine must read every column of every customer into memory just to discard 90% of the payload.

### The Data Engineering / OLAP World (Columnar Store)
In analytical warehouses (OLAP):
- You rarely query one user; you query millions of records asking: *"What was total revenue per category last quarter?"*
- **Parquet** and **DuckDB** store data **column-by-column**:
  `total_amount: [100.0, 45.5, 320.0, ...]`  
  `status: ['completed', 'pending', 'completed', ...]`
- **Vectorized Scans & Compression**: Because identical data types sit together, run-length and dictionary compression shrink storage by 70–90%. When computing `SUM(total_amount)`, the CPU reads **only** the `total_amount` column straight into vector registers (SIMD), skipping disk I/O for unused fields.

```text
Row Storage (PostgreSQL / JPA):
[ID | Name | Country | Amount] -> Page 1: Alice, Page 2: Bob

Column Storage (Parquet / DuckDB):
[Amount Column]: [100.0, 45.5, 320.0] -> Scanned in a single CPU burst
[Country Column]: ['US', 'IN', 'DE']
```

---

## 3. Storage Format: Database Tables vs Parquet Files

In Java backends, data lives in database tables managed by a server daemon. In Data Engineering, files in object storage (S3, GCS, or local filesystem) are treated as first-class datasets:

- **Apache Parquet (`data/raw/*.parquet`)**: An open, immutable, binary columnar file format with built-in metadata (row groups, column statistics, min/max values for partition pruning).
- **DuckDB**: Think of DuckDB as the **"SQLite for Analytics"**. It is an embedded in-process C++ engine (no background server daemon needed) that executes analytical SQL directly over Parquet files, Arrow tables, or memory.

```python
# No database server setup required - directly query Parquet files!
import duckdb
duckdb.query("SELECT category, SUM(subtotal) FROM 'data/raw/order_items.parquet' GROUP BY 1")
```

---

## 4. Modeling: 3NF (Normalized) vs Kimball Dimensional Modeling (Star Schema)

As a Java developer, you are taught **Third Normal Form (3NF)**: normalize tables, split tables to eliminate redundancy, and use foreign keys.

In Data Engineering, excessive joins at large scale are expensive. We use **Dimensional Modeling (Kimball Star Schema)**:

### Fact Tables (`fct_`)
- Represent **business events or measurements** occurring at a specific point in time.
- Numeric metrics and foreign keys.
- Example in this repo: `marts/fct_daily_sales.sql`
  - Grain: 1 day per category.
  - Metrics: `total_orders`, `items_sold`, `gross_revenue`, `net_revenue`.

### Dimension Tables (`dim_`)
- Represent the **context/entities** (Who, What, Where) surrounding an event.
- Denormalized attributes to avoid multi-table joins during BI dashboard queries.
- Example in this repo: `marts/dim_customers.sql`
  - Grain: 1 row per customer.
  - Contains descriptive fields plus pre-computed customer aggregations (`lifetime_spend`, `total_orders`).

---

## 5. ETL vs ELT: Where Does Transformation Happen?

### Legacy ETL (Extract -> Transform -> Load)
Popular when compute/warehouse storage was expensive. Specialized servers (like Java/Spring Batch apps or Informatica) transformed data in memory before writing clean rows into the database.

### Modern ELT (Extract -> Load -> Transform)
With engines like DuckDB, Snowflake, and BigQuery:
1. **Extract & Load**: Ingest raw, unaltered data straight into storage/data lake (`generate_raw_data.py` -> `data/raw/*.parquet`).
2. **Transform in the Warehouse**: Use SQL inside the analytical engine to build staging views and materialized marts. Compute is co-located with storage.

---

## 6. What is `dbt` (Data Build Tool)?

If you know Java database tools, here is the analogy:
- **Flyway / Liquibase**: Manages DDL migrations (`CREATE TABLE`, `ALTER TABLE`) to evolve a transactional database schema version by version.
- **dbt**: Manages **data transformations as code**. 

### How dbt works:
- You write pure `SELECT` statements (e.g. `models/marts/dim_customers.sql`).
- dbt automatically wraps your SQL with `CREATE TABLE AS SELECT ...` or `CREATE VIEW AS SELECT ...` depending on configuration.
- **Dependency Graph (DAG)**: When you use `{{ ref('stg_orders') }}`, dbt parses your SQL, builds a Directed Acyclic Graph, and determines the exact topological order to execute models in parallel.
- **Data Contracts & Testing (Like JUnit / Bean Validation for Data)**:
  Instead of writing manual boilerplate assertions or JUnit integration tests with mock containers, data quality contracts are declared directly alongside the schema in YAML.
  
  In this repo (`models/staging/schema.yml` and `models/marts/schema.yml`), dbt automatically compiles and executes 26 SQL assertion tests:
  ```yaml
  columns:
    - name: customer_id
      data_tests:
        - unique       # Compiles to: SELECT customer_id FROM ... GROUP BY 1 HAVING count(*) > 1
        - not_null     # Compiles to: SELECT * FROM ... WHERE customer_id IS NULL
    - name: status
      data_tests:
        - accepted_values:
            arguments:
              values: ['completed', 'returned', 'cancelled']
    - name: order_id
      data_tests:
        - relationships:  # Foreign-key check equivalent to JPA @ManyToOne constraint
            arguments:
              to: ref('stg_orders')
              field: order_id
  ```
  Run with:
  ```bash
  dbt test --profiles-dir .
  ```
  If any assertion returns rows (e.g. orphan foreign keys or duplicate IDs), dbt fails the build immediately before bad data can corrupt downstream marts.

---

## 7. Mapping Java Code to Data Engineering Constructs

### Example 1: Summing Aggregations

#### In Java (Streams / Entity List):
```java
Map<String, BigDecimal> revenueByCategory = orderItems.stream()
    .collect(Collectors.groupingBy(
        OrderItem::getCategory,
        Collectors.reducing(BigDecimal.ZERO, OrderItem::getSubtotal, BigDecimal::add)
    ));
```

#### In Modern Data Engineering (DuckDB SQL / dbt):
```sql
SELECT 
    category,
    ROUND(SUM(subtotal), 2) AS gross_revenue
FROM staging.stg_order_items
GROUP BY category;
```
*Why SQL here? The vectorized engine executes this across millions of rows in milliseconds using C++ SIMD, whereas Java Streams iterate object references on the JVM heap.*

### Example 2: Dataframes (Polars vs Java Collections)
Data engineers use **Polars** (written in Rust) or **PyArrow** instead of iterating POJO collections. A DataFrame is an in-memory columnar table that can process millions of operations per second with minimal memory footprint.

---

## 8. Change Data Capture (CDC), Event Sourcing & the Outbox Pattern

As a Java / microservices engineer, you may know Kafka, Debezium, and the **Transactional Outbox Pattern**:
- Instead of batch polling a database every midnight, your Spring service commits mutations to the database.
- A tool like **Debezium** captures row-level changes from the MySQL/PostgreSQL write-ahead log (WAL) and publishes an append-only event stream to Kafka:
  ```json
  {"op": "c", "order_id": 1059, "status": "pending", "ts_ms": 1775664145000}
  {"op": "u", "order_id": 1059, "status": "completed", "ts_ms": 1775665945000}
  ```
- A Kafka Connect S3 Sink stores these immutable events into Parquet files.

### How Data Engineers Reconstruct Current State (Deduplication)
In the warehouse, data engineers use **SQL Window Functions** over the event stream rather than updating records in place.

See [simulate_cdc_events.py](file:///sdcard/Download/termux/modern-data-stack-lab/simulate_cdc_events.py) and [models/staging/stg_orders_cdc_current.sql](file:///sdcard/Download/termux/modern-data-stack-lab/models/staging/stg_orders_cdc_current.sql):
```sql
with ranked_events as (
    select
        order_id,
        status,
        total_amount,
        row_number() over (
            partition by order_id
            order by ts_ms desc
        ) as deduplication_rank
    from raw_cdc_events
)
select * from ranked_events where deduplication_rank = 1;
```
This guarantees that analytical queries always see the latest snapshot of each entity with zero mutable locks!

---

## 9. Reverse-ETL: Syncing Analytical Marts Back to Operational Apps (Spring / JPA)

In software engineering, transactional apps cannot query analytical warehouses directly because:
- Analytics queries can lock tables or have unpredictable latency.
- Spring Boot / JPA entities are mapped to operational stores like PostgreSQL or MySQL.

**Reverse-ETL** solves this by syncing modeled, pre-aggregated marts (`dim_customers`, `fct_daily_sales`) **back** into an operational database so your Spring Boot services, microservices, and customer-facing APIs can query enriched data via standard `@Entity` repositories with sub-millisecond response times.

### Example in this repo ([reverse_etl.py](file:///sdcard/Download/termux/modern-data-stack-lab/reverse_etl.py)):
```bash
# Syncs DuckDB marts into operational SQLite / PostgreSQL
python reverse_etl.py
```
This populates operational tables like `operational_customer_profiles`, allowing a Spring Boot entity like:
```java
@Entity
@Table(name = "operational_customer_profiles")
public class CustomerProfile {
    @Id
    private Long customerId;
    private String customerName;
    private BigDecimal lifetimeSpend; // Pre-calculated by DuckDB / dbt!
    private Integer totalOrders;
    private LocalDateTime syncedAt;
}
```

---

## 10. Data Lake Architecture & Hive Partition Pruning

In relational databases (RDBMS), large tables are indexed (B-Trees) or partitioned into database tablespaces.

In Data Engineering and cloud storage (S3 / GCS / HDFS), files are laid out using **Apache Hive Partitioning**:
```text
data/lake/orders/
  ├── year=2026/
  │   ├── month=07/data.parquet
  │   ├── month=08/data.parquet
  │   ├── month=09/data.parquet
  │   └── month=10/data.parquet
```

### Why This Matters: Partition Pruning (Skipping Disk I/O)
When an analytical query asks:
```sql
SELECT * FROM stg_lake_orders WHERE order_year = 2026 AND order_month = 10;
```
DuckDB detects the directory path, scans **only** `month=10/data.parquet`, and **skips 75% of the data lake entirely** without even opening the other files!

In this repo:
- [generate_raw_data.py](file:///sdcard/Download/termux/modern-data-stack-lab/generate_raw_data.py) partitions orders into `data/lake/orders/year=YYYY/month=MM/`.
- [models/staging/stg_lake_orders.sql](file:///sdcard/Download/termux/modern-data-stack-lab/models/staging/stg_lake_orders.sql) demonstrates DuckDB automatically extracting `year` and `month` as virtual columns from directory paths.

---

## 11. Slowly Changing Dimensions (SCD Type 2) & dbt Snapshots

In transactional databases, an update statement overwrites the old row:
`UPDATE customers SET email = 'new@corp.com' WHERE customer_id = 1;`
In software engineering, you lose the historical record unless you use complex audit tables (like Hibernate Envers).

In Data Engineering, **Slowly Changing Dimensions (SCD Type 2)** tracks historical state changes by keeping old versions and stamping validity windows:
- `dbt_valid_from`: Timestamp when this version of the entity became active.
- `dbt_valid_to`: Timestamp when this version was superseded (or `NULL` if currently active).

### Example in this repo ([snapshots/customers_snapshot.sql](file:///sdcard/Download/termux/modern-data-stack-lab/snapshots/customers_snapshot.sql)):
```sql
{% snapshot customers_snapshot %}
{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_name', 'email', 'country']
    )
}}
select customer_id, customer_name, email, country, created_at from {{ ref('stg_customers') }}
{% endsnapshot %}
```
Run with:
```bash
dbt snapshot --profiles-dir .
```
This automatically maintains `snapshots.customers_snapshot` in DuckDB, enabling point-in-time time-travel queries (e.g. *"What was the customer's country on the date of Order #42?"*) with zero manual boilerplate!

---

## 12. The Semantic Layer & Metric Store (Decoupling Business Logic from SQL)

In traditional software systems, business metrics (e.g. *Average Order Value*, *Active Subscriptions*, *Churn*) are calculated inside Java domain services:
```java
public BigDecimal calculateAov(List<Order> orders) {
    BigDecimal netSales = ...;
    return netSales.divide(BigDecimal.valueOf(orders.size()), RoundingMode.HALF_UP);
}
```
**The Problem in Analytics**: Every BI tool (Tableau, Looker, Excel, ad-hoc Python) writes their own SQL `GROUP BY` logic. If Marketing defines AOV including taxes and Finance excludes discounts, the numbers disagree across dashboards.

**The Solution: The Semantic Layer / Metric Store**:
- Business logic is declared **once as code** in YAML ([models/semantic/metricflow_semantic_models.yml](file:///sdcard/Download/termux/modern-data-stack-lab/models/semantic/metricflow_semantic_models.yml)).
- Defines **Entities** (`order_id`, `customer_id`), **Dimensions** (`status`, `order_date`), and **Measures** (`order_count`, `order_completed_amount`).
- Metrics like `average_order_value` (`type: ratio`) are computed dynamically regardless of what dimensions or time windows a user slices by.
- Powered by a daily time spine ([models/semantic/metricflow_time_spine.sql](file:///sdcard/Download/termux/modern-data-stack-lab/models/semantic/metricflow_time_spine.sql)) to guarantee accurate time-grain aggregations without missing zero-activity days.

---

## 13. Where to Go From Here in this Codebase

Follow this recommended path to see these concepts in action:

1. **Inspect Raw Generation & Data Lake**: Look at [generate_raw_data.py](file:///sdcard/Download/termux/modern-data-stack-lab/generate_raw_data.py) to see both flat Parquet and Hive-partitioned files generated.
2. **Explore CDC & Streaming Logs**: Run `python simulate_cdc_events.py` and inspect [models/staging/stg_orders_cdc_current.sql](file:///sdcard/Download/termux/modern-data-stack-lab/models/staging/stg_orders_cdc_current.sql).
3. **Explore Hive Partitioning**: Check [models/staging/stg_lake_orders.sql](file:///sdcard/Download/termux/modern-data-stack-lab/models/staging/stg_lake_orders.sql) to see zero-overhead partition pruning.
4. **Run SCD Type 2 Snapshots**: Run `dbt snapshot --profiles-dir .` and inspect [snapshots/customers_snapshot.sql](file:///sdcard/Download/termux/modern-data-stack-lab/snapshots/customers_snapshot.sql).
5. **Inspect the Semantic Layer**: Check [models/semantic/metricflow_semantic_models.yml](file:///sdcard/Download/termux/modern-data-stack-lab/models/semantic/metricflow_semantic_models.yml) and [models/semantic/metricflow_time_spine.sql](file:///sdcard/Download/termux/modern-data-stack-lab/models/semantic/metricflow_time_spine.sql).
6. **Run the Standalone ELT**: Check [pipeline_runner.py](file:///sdcard/Download/termux/modern-data-stack-lab/pipeline_runner.py).
7. **Run dbt & Contracts**: Run `dbt run --profiles-dir .` and `dbt test --profiles-dir .` (all 26 tests).
8. **Launch Orchestrator**: Run `PYTHONPATH=src:. dagster job execute -m modern_data_stack_lab -j mds_full_pipeline_job`.
9. **Reverse-ETL to App DB**: Run `python reverse_etl.py` to populate operational tables.
10. **Launch Terminal BI**: Run `python dashboard.py` for rich terminal KPI dashboards.



