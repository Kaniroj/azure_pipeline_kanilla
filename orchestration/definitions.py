from pathlib import Path
import sys
from dagster import Definitions
from dagster_dlt import dlt_assets, DagsterDltResource

# اضافه کردن مسیر منبع داده
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "data_extract_load"))

from load_data_jobs import job_ads_source

# ساخت resource از DLT
dlt_resource = DagsterDltResource(
    pipeline_name="job_ads_pipeline",
    destination="duckdb",  # یا snowflake, bigquery, postgres...
    dataset_name="staging",
)

# تعریف asset در Dagster
job_ads_assets = dlt_assets(
    dlt_source=job_ads_source,
    dlt_resource_key="dlt_resource",
)

# تعریف اصلی Dagster
defs = Definitions(
    assets=[job_ads_assets],
    resources={"dlt_resource": dlt_resource},
)
