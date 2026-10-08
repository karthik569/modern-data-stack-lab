#!/usr/bin/env python3
"""
Terminal BI Analytics Dashboard
--------------------------------
Interactive, zero-overhead Terminal BI dashboard powered by DuckDB & Rich.
Visualizes executive KPIs, category revenue distributions, and customer cohorts
directly from modeled OLAP marts in data/warehouse.duckdb.
"""

import sys
from pathlib import Path
import duckdb
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.columns import Columns
from rich.text import Text

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "warehouse.duckdb"

console = Console()

def get_db_connection():
    if not DB_PATH.exists():
        console.print(f"[bold red]Warehouse database not found at:[/bold red] {DB_PATH}")
        console.print("[yellow]Please run `python pipeline_runner.py` or `dbt run --profiles-dir .` first![/yellow]")
        sys.exit(1)
    return duckdb.connect(str(DB_PATH), read_only=True)

def render_kpi_cards(con):
    kpi_query = """
    SELECT
        COUNT(*) AS total_customers,
        COALESCE(SUM(lifetime_spend), 0) AS total_revenue,
        COALESCE(SUM(total_orders), 0) AS total_orders,
        COALESCE(AVG(lifetime_spend), 0) AS avg_customer_ltv
    FROM marts.dim_customers;
    """
    row = con.execute(kpi_query).fetchone()
    total_cust, total_rev, total_ord, avg_ltv = row

    # Recent active days
    active_days = con.execute("SELECT COUNT(DISTINCT order_day) FROM marts.fct_daily_sales;").fetchone()[0]

    p1 = Panel(
        f"[bold cyan]{total_cust:,}[/bold cyan]\n[dim]Total Customers[/dim]",
        title="👥 Cohort Size",
        border_style="cyan"
    )
    p2 = Panel(
        f"[bold green]${total_rev:,.2f}[/bold green]\n[dim]Net Completed Revenue[/dim]",
        title="💰 Total Net Revenue",
        border_style="green"
    )
    p3 = Panel(
        f"[bold magenta]{total_ord:,}[/bold magenta]\n[dim]Orders across {active_days} days[/dim]",
        title="📦 Total Orders",
        border_style="magenta"
    )
    p4 = Panel(
        f"[bold yellow]${avg_ltv:,.2f}[/bold yellow]\n[dim]Average Spend / Customer[/dim]",
        title="⭐ Avg Customer LTV",
        border_style="yellow"
    )

    console.print(Columns([p1, p2, p3, p4], equal=True))

def render_category_performance(con):
    query = """
    SELECT
        category,
        SUM(total_orders) AS orders,
        SUM(items_sold) AS units_sold,
        SUM(net_revenue) AS net_revenue,
        SUM(gross_revenue) AS gross_revenue
    FROM marts.fct_daily_sales
    GROUP BY category
    ORDER BY net_revenue DESC;
    """
    df = con.execute(query).pl()

    max_rev = float(df["net_revenue"].max()) if len(df) > 0 else 1.0
    total_net = float(df["net_revenue"].sum()) if len(df) > 0 else 1.0

    table = Table(title="📊 Revenue & Volume Breakdown by Category", expand=True, border_style="blue")
    table.add_column("Category", style="bold white", width=16)
    table.add_column("Orders", justify="right", style="cyan")
    table.add_column("Units Sold", justify="right", style="yellow")
    table.add_column("Net Revenue", justify="right", style="bold green")
    table.add_column("Revenue Share Bar", justify="left", style="green")

    for row in df.iter_rows(named=True):
        rev = float(row["net_revenue"])
        bar_len = int((rev / max_rev) * 25)
        bar = "█" * bar_len + "░" * (25 - bar_len)
        pct = (rev / total_net) * 100 if total_net > 0 else 0.0

        table.add_row(
            row["category"],
            f"{row['orders']:,}",
            f"{row['units_sold']:,}",
            f"${rev:,.2f}",
            f"{bar} [dim]{pct:.1f}%[/dim]"
        )

    console.print(table)

def render_top_customers(con, limit=5):
    query = f"""
    SELECT
        customer_id,
        customer_name,
        country,
        total_orders,
        lifetime_spend,
        first_order_date,
        most_recent_order_date
    FROM marts.dim_customers
    ORDER BY lifetime_spend DESC
    LIMIT {limit};
    """
    df = con.execute(query).pl()

    table = Table(title=f"🏆 Top {limit} High-Value Customers (VIP Segment)", expand=True, border_style="gold1")
    table.add_column("ID", justify="right", style="dim", width=6)
    table.add_column("Customer Name", style="bold white")
    table.add_column("Country", justify="center", style="cyan", width=8)
    table.add_column("Orders", justify="right", style="yellow")
    table.add_column("Lifetime Spend", justify="right", style="bold green")
    table.add_column("First Order", style="dim")
    table.add_column("Latest Order", style="dim")

    for row in df.iter_rows(named=True):
        table.add_row(
            str(row["customer_id"]),
            row["customer_name"],
            row["country"] or "N/A",
            str(row["total_orders"]),
            f"${float(row['lifetime_spend']):,.2f}",
            str(row["first_order_date"])[:10] if row["first_order_date"] else "-",
            str(row["most_recent_order_date"])[:10] if row["most_recent_order_date"] else "-"
        )

    console.print(table)

def render_recent_daily_trend(con, days=7):
    query = f"""
    SELECT
        order_day,
        SUM(total_orders) AS daily_orders,
        SUM(net_revenue) AS daily_revenue
    FROM marts.fct_daily_sales
    GROUP BY order_day
    ORDER BY order_day DESC
    LIMIT {days};
    """
    df = con.execute(query).pl()

    table = Table(title=f"📈 Recent Daily Revenue Velocity (Last {days} Days)", expand=True, border_style="magenta")
    table.add_column("Order Date", style="bold white", width=14)
    table.add_column("Orders", justify="right", style="cyan")
    table.add_column("Net Revenue", justify="right", style="bold green")
    table.add_column("Trend Bar", justify="left", style="magenta")

    max_daily = float(df["daily_revenue"].max()) if len(df) > 0 else 1.0

    for row in df.iter_rows(named=True):
        rev = float(row["daily_revenue"])
        bar_len = int((rev / max_daily) * 20) if max_daily > 0 else 0
        bar = "■" * bar_len

        table.add_row(
            str(row["order_day"])[:10],
            f"{row['daily_orders']:,}",
            f"${rev:,.2f}",
            bar
        )

    console.print(table)

def main():
    con = get_db_connection()
    console.clear()
    
    header = Panel(
        Text("🚀 Modern Data Stack Lab — Executive Terminal BI Dashboard", style="bold white", justify="center"),
        subtitle="[dim]DuckDB In-Process OLAP Warehouse Engine[/dim]",
        border_style="bright_blue"
    )
    console.print(header)
    console.print()

    # 1. Executive KPIs
    render_kpi_cards(con)
    console.print()

    # 2. Category Performance
    render_category_performance(con)
    console.print()

    # 3. Top VIP Customers
    render_top_customers(con, limit=5)
    console.print()

    # 4. Recent Daily Trend
    render_recent_daily_trend(con, days=6)
    console.print()

    con.close()

if __name__ == "__main__":
    main()
