from pathlib import Path
import sys
import dlt
from dagster import Definitions, define_asset_job, ScheduleDefinition
from dagster_dlt import dlt_assets, DagsterDltResource
from dagster_dbt import DbtCliResource, dbt_assets

# مسیرها
ROOT_DIR = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = ROOT_DIR / "dbt"
DBT_PROFILES_DIR = Path.home() / ".dbt"
sys.path.insert(0, str(ROOT_DIR / "data_extract_load"))

# ---------- DLT ----------
from load_data_jobs import job_ads_source  # اسم فانکشن DLT در فایل load_data_jobs.py

pipeline = dlt.pipeline(
    pipeline_name="job_ads_pipeline",
    destination="duckdb",
    dataset_name="staging",
)

dlt_resource = DagsterDltResource(
    pipeline_name="job_ads_pipeline",
    destination="duckdb",
    dataset_name="staging",
)

# تعریف DLT asset
job_ads_assets = dlt_assets(
    dlt_source=job_ads_source(),
    dlt_pipeline=pipeline
)

# ---------- DBT ----------
dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
)

@dbt_assets(manifest=str(DBT_PROJECT_DIR / "target" / "manifest.json"))
def dbt_job_ads_assets(context):
    """Run dbt models."""
    yield from context.run_dbt_command(["run"])

# ---------- JOB ----------
# اینجا به جای string، از خود object asset استفاده می‌کنیم
full_etl_job = define_asset_job(
    name="full_etl_job",
    selection=[job_ads_assets, dbt_job_ads_assets]
)

# ---------- SCHEDULE ----------
full_etl_schedule = ScheduleDefinition(
    job=full_etl_job,
    cron_schedule="0 2 * * *",  # هر روز ساعت ۲ صبح
)

# ---------- DAGSTER DEFINITIONS ----------
defs = Definitions(
    assets=[job_ads_assets, dbt_job_ads_assets],
    jobs=[full_etl_job],
    schedules=[full_etl_schedule],
    resources={
        "dlt": dlt_resource,
        "dbt": dbt_resource,
    },
)
