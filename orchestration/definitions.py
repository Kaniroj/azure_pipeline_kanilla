from pathlib import Path
import sys
import dlt
from dagster import Definitions
from dagster_dlt import dlt_assets, DagsterDltResource
from dagster_dbt import DbtCliResource, dbt_assets

# مسیرها
ROOT_DIR = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = ROOT_DIR / "dbt"
DBT_PROFILES_DIR = Path.home() / ".dbt"
sys.path.insert(0, str(ROOT_DIR / "data_extract_load"))

# ایمپورت منبع داده از DLT
from load_data_jobs import job_ads_source

# ---------- DLT ----------
pipeline = dlt.pipeline(
    pipeline_name="job_ads_pipeline",
    destination="duckdb",
    dataset_name="staging"
)

dlt_resource = DagsterDltResource(
    pipeline_name="job_ads_pipeline",
    destination="duckdb",
    dataset_name="staging"
)

job_ads_source_obj = job_ads_source()

@dlt_assets(dlt_source=job_ads_source_obj, dlt_pipeline=pipeline)
def job_ads_assets():
    pass


# ---------- DBT ----------
dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
)

# از decorator قدیمی استفاده می‌کنیم
@dbt_assets(manifest=str(DBT_PROJECT_DIR / "target" / "manifest.json"))
def dbt_job_ads_assets(context):
    yield from context.run_dbt_command(["run"])


# ---------- DAGSTER DEFINITIONS ----------
defs = Definitions(
    assets=[job_ads_assets, dbt_job_ads_assets],
    resources={
        "dlt": dlt_resource,
        "dbt": dbt_resource,
    },
)
