from pathlib import Path
import sys
import dlt
from dagster import Definitions
from dagster_dlt import dlt_assets, DagsterDltResource

# --- مسیر پروژه ---
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "data_extract_load"))

# --- ایمپورت منبع داده ---
from load_data_jobs import job_ads_source

# --- ساخت pipeline واقعی DLT ---
pipeline = dlt.pipeline(
    pipeline_name="job_ads_pipeline",
    destination="duckdb",
    dataset_name="staging"
)

# --- ساخت DagsterDltResource ---
dlt_resource = DagsterDltResource(
    pipeline_name="job_ads_pipeline",
    destination="duckdb",
    dataset_name="staging"
)

# --- اجرای منبع DLT برای گرفتن آبجکت منبع ---
job_ads_source_obj = job_ads_source()

# --- استفاده از decorator برای تعریف asset ---
@dlt_assets(dlt_source=job_ads_source_obj, dlt_pipeline=pipeline)
def job_ads_assets():
    pass  # نیازی به body نیست

# --- تعریف اصلی Dagster ---
defs = Definitions(
    assets=[job_ads_assets],
    resources={"dlt": dlt_resource},
)
