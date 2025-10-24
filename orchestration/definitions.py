from pathlib import Path
import sys
import dlt
from dagster import Definitions
from dagster_dlt import dlt_assets, DagsterDltResource
from dagster_dbt import DbtCliResource, load_assets_from_dbt_project

# مسیرها
ROOT_DIR = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = ROOT_DIR / "dbt"
DBT_PROFILES_DIR = Path.home() / ".dbt"
sys.path.insert(0, str(ROOT_DIR / "data_extract_load"))

# ---------- DLT ----------
from load_data_jobs import jobsearch_source  # مطمئن شو اسم فانکشن در فایل DLT همین هست

pipeline = dlt.pipeline(
    pipeline_name="job_ads_pipeline",
    destination="duckdb",
    dataset_name="staging"
)

dlt_resource = DagsterDltResource()

@dlt_assets(dlt_source=jobsearch_source, dlt_pipeline=pipeline)
def dlt_jobsearch_source_jobsearch_resource(_):
    """DLT asset that loads job ads from API."""
    return

# ---------- DBT ----------
dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
)

# این متد جدید load_assets_from_dbt_project باعث میشه Dagster
# به‌صورت خودکار فایل manifest.json رو بخونه و meta.dagster.asset_key رو تطبیق بده
dbt_assets = load_assets_from_dbt_project(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
)

# ---------- DAGSTER DEFINITIONS ----------
defs = Definitions(
    assets=[dlt_jobsearch_source_jobsearch_resource, dbt_assets],
    resources={
        "dlt": dlt_resource,
        "dbt": dbt_resource,
    },
)
