import os
import shutil
import sys
from pathlib import Path
from dagster import (
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from dagster_dbt import (
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

# Project paths
PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
DBT_PROJECT_DIR = PROJECT_DIR
MANIFEST_PATH = PROJECT_DIR / "target" / "manifest.json"

# Resolve dbt executable (falling back to venv bin directory)
DBT_EXE = shutil.which("dbt") or str(Path(sys.prefix) / "bin" / "dbt")

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    packaged_project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)

# 1. Software-Defined Asset: Raw Synthetic Ingestion
@asset(
    group_name="ingestion",
    description="Generates raw e-commerce synthetic Parquet files (customers, orders, order items)",
)
def raw_parquet_data(context: AssetExecutionContext) -> None:
    if str(PROJECT_DIR) not in sys.path:
        sys.path.insert(0, str(PROJECT_DIR))
    from generate_raw_data import run as generate_data
    context.log.info("Starting synthetic raw Parquet generation...")
    generate_data()
    context.log.info("Raw Parquet datasets generated successfully.")

# 2. Software-Defined Assets: dbt Staging Views and Marts
@dbt_assets(
    manifest=MANIFEST_PATH,
    project=dbt_project,
)
def modern_data_stack_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# 3. Pipeline Job: End-to-end Ingestion + dbt Transformation
mds_pipeline_job = define_asset_job(
    name="mds_full_pipeline_job",
    selection=[raw_parquet_data, modern_data_stack_dbt_assets],
    description="Executes end-to-end ingestion and dbt transformations into DuckDB warehouse",
)

# 4. Schedule: Daily Pipeline Execution
daily_mds_schedule = ScheduleDefinition(
    job=mds_pipeline_job,
    cron_schedule="0 0 * * *",
    name="daily_mds_pipeline_schedule",
)

# 5. Dagster Definitions
defs = Definitions(
    assets=[raw_parquet_data, modern_data_stack_dbt_assets],
    jobs=[mds_pipeline_job],
    schedules=[daily_mds_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR, dbt_executable=DBT_EXE),
    },
)
