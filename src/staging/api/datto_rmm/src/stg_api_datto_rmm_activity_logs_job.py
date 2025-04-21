"""
Datto RMM Activity Logs (Job Category) ETL Flow

This flow:
1. Extracts activity log data filtered by 'job' category
2. Transforms nested job execution details into structured columns
3. Loads results into MinIO and PostgreSQL in parallel
"""

from prefect import flow, task
from prefect.artifacts import *

from load.load_minio import *
from load.load_postgres import *

from utilities.setup_logger import *
from utilities.task_prep import *
from utilities.vault_mgr import *

from transform.transform_api_datto_rmm_activity_logs_job import *
from extract.extract_api_datto_rmm import *

import sys
import inspect
import traceback
import concurrent.futures
import pandas as pd
from pathlib import Path
import os
import json
import datetime as dt

results_list = []


@task(tags=["extract", "get", "api", "batch"])
def extract_api_datto_rmm_activity_logs(config: dict,
                                        days: int = 1,
                                        categories: list = []) -> pd.DataFrame:
    """
    Extracts activity logs filtered by category and time window.
    Adds metadata markers post-extraction.
    """
    try:
        vault = VaultManager()
        datto = ExtractApiDattoRMM(config=config, vault=vault)

        from_dt = dt.datetime.now() - dt.timedelta(days=days)
        from_dt = from_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        data = datto.create_activity_logs_dataframe(from_dt=from_dt, categories=categories)
        df = data["data"]
        result = data["result"]
        results_list.append(result)

        df['_SOURCE_PRODUCT'] = config["DETAILS"]["product"]
        df['_SOURCE_SUBJECT'] = config["DETAILS"]["subject"]
        df['_SOURCE_ORIGIN'] = config["DATA"]["origin"]
        df['_UTC_EXTRACTION_DATETIME'] = config["TIMESTAMPS"]["_IN_DATA_TIMESTAMP"]

        return df

    except Exception:
        t = traceback.format_exc()
        result = {
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": t
        }
        results_list.append(result)
        return pd.DataFrame()


@task(tags=["transform"])
def transform_dataframe(df, config: dict) -> pd.DataFrame:
    """
    Transforms Datto RMM activity log data using job-specific parsing logic.
    """
    try:
        transform = TransformApiDattoRMM(df)
        data = transform.transform_activity_logs_job_dataframe()
        df = data["data"]
        result = data["result"]
        results_list.append(result)

        return df

    except Exception:
        t = traceback.format_exc()
        result = {
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": f"Error: {t}"
        }
        results_list.append(result)
        print(result)
        sys.exit(1)


@task(tags=["load", "put", "object_storage", "minio"])
def load_minio(df: pd.DataFrame, config: dict, vault: VaultManager) -> None:
    """
    Uploads transformed data to MinIO object storage.
    """
    try:
        minio = MinioLoad(df_input=df, config=config, vault=vault)
        data = minio.upload_to_minio()
        result = data["result"]
        results_list.append(result)

    except Exception:
        t = traceback.format_exc()
        result = {
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": t
        }
        results_list.append(result)
        print(results_list)
        sys.exit(1)


@task(tags=["load", "put", "database", "postgresql"])
def load_postgres(df: pd.DataFrame, config: dict, vault: VaultManager) -> None:
    """
    Inserts transformed job log data into a PostgreSQL table.
    """
    try:
        postgres = PostgresLoad(df_input=df, config=config, vault=vault)
        data = postgres.load_to_postgres()
        result = data["result"]
        results_list.append(result)

    except Exception:
        t = traceback.format_exc()
        result = {
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": t
        }
        results_list.append(result)


@flow(name="stg_api_datto_rmm_activity_logs_job")
def stg_api_datto_rmm_activity_logs_job() -> None:
    """
    Orchestrates the activity log ETL pipeline (filtered by job category).
    - Extract logs for the past 45 days
    - Transform job-related execution metadata
    - Load results to object storage and SQL database
    """
    print(f"[INFO] Working dir: {os.getcwd()}")

    tasks = prepare_tasks(config_dir=f"{Path(__file__).parent.resolve()}/config/activity_logs_job/config.yaml")["data"]
    vault = VaultManager()

    df = extract_api_datto_rmm_activity_logs(days=45,
                                             categories=["job"],
                                             config=tasks[0])
    df = transform_dataframe(df, tasks[1])

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_minio = executor.submit(load_minio, df=df, config=tasks[2], vault=vault)
        future_postgres = executor.submit(load_postgres, df=df, config=tasks[3], vault=vault)
        concurrent.futures.wait([future_minio, future_postgres])

    print("#" * 75)
    print("\n        FINAL RESULTS\n")
    print("--------------------------------")
    for result in results_list:
        print("\n" + json.dumps(result, indent=4))
        print("---------")
    print("\n" + "#" * 75)


if __name__ == "__main__":
    stg_api_datto_rmm_activity_logs_job()
