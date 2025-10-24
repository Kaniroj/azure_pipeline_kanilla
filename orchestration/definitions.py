from pathlib import Path
import dlt
import dagster as dg
#from dagster_dlt import dlt_resource, dlt_source
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dagster import Definitions
from dagster_dlt import dlt_assets, DagsterDltResource
import sys
import os

DUCKDB_PATH = os.getenv("DUCKDB_PATH")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")
#db_path = str(Path(__file__).parents[1] / "data_warehouse/job_ads.duckdb")
sys.path.insert(0, "../data_extract_load")
from load_data_jobs import jobsearch_source

