from pathlib import Path
import sys
from dagster import Definitions
from dagster_dlt import dlt_assets, DagsterDltResource

# --- مسیر پروژه ---
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "data_extract_load"))

# --- ایمپورت منبع داده ---
from load_data_jobs import job_ads_source

# --- تعریف منبع DLT برای Dagster ---
dlt_resource = DagsterDltResource(
    pipeline_name="job_ads_pipeline",
    destination="duckdb",      # مقصد داده
    dataset_name="staging"     # نام دیتاست
)

# --- تعریف DLT Assets ---
job_ads_assets = dlt_assets(job_ads_source)

# --- تعریف Dagster Definitions ---
defs = Definitions(
    assets=[job_ads_assets],
    resources={"dlt": dlt_resource},
)
