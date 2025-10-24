# file: data_extract_load/load_data_jobs.py

import dlt
import requests
import json

# DLT configuration (برای کنترل رفتار staging)
dlt.config["load.truncate_staging_dataset"] = True

# پارامترهای پیش‌فرض
DEFAULT_TABLE_NAME = "job_ads"
DEFAULT_LIMIT = 100


def _get_ads(url_for_search: str, params: dict) -> dict:
    """درخواست به API و بازگرداندن JSON نتیجه."""
    headers = {"accept": "application/json"}
    response = requests.get(url_for_search, headers=headers, params=params)
    response.raise_for_status()
    return json.loads(response.content.decode("utf-8"))


@dlt.resource(
    table_name=DEFAULT_TABLE_NAME,
    write_disposition="append",  # داده‌های جدید به قبلی اضافه می‌شوند
)
def job_ads_resource(params: dict):
    """
    DLT resource — داده‌ها را از API JobTech به صورت صفحه‌به‌صفحه می‌خواند.
    """
    url = "https://jobsearch.api.jobtechdev.se/search"
    limit = params.get("limit", DEFAULT_LIMIT)
    offset = 0

    while True:
        page_params = dict(params, offset=offset)
        data = _get_ads(url, page_params)

        hits = data.get("hits", [])
        if not hits:
            break

        for ad in hits:
            yield ad

        if len(hits) < limit or offset > 1900:
            break

        offset += limit


@dlt.source
def job_ads_source(q: str = "developer", limit: int = DEFAULT_LIMIT):
    """
    DLT source — برای Dagster.
    می‌تواند با پارامترهای مختلف (q یا limit) صدا زده شود.
    """
    params = {"q": q, "limit": limit}
    return job_ads_resource(params)

if __name__ == "__main__":
    print("🚀 Starting DLT pipeline...")

    from pathlib import Path

pipeline = dlt.pipeline(
    pipeline_name="job_ads_pipeline",
    destination=dlt.destinations.duckdb(
        str(Path(__file__).parents[1] / "data_warehouse" / "job_ads.duckdb")
    ),
    dataset_name="staging"
)


    # اجرای سورس DLT
load_info = pipeline.run(job_ads_source())

print("✅ Pipeline completed!")
print(load_info)

