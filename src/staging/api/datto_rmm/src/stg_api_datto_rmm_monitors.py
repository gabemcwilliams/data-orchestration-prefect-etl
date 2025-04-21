"""
Datto RMM Devices ETL Flow

This flow:
1. Extracts device data via API
2. Transforms the data into a normalized schema
3. Loads results into MinIO and PostgreSQL concurrently
"""

from prefect import flow, task
from prefect.artifacts import *

from load.load_minio import MinioLoad
from load.load_postgres import PostgresLoad

from utilities.task_prep import prepare_tasks
from utilities.vault_mgr import VaultManager

from extract.extract_api_datto_rmm import ExtractApiDattoRMM
from transform.transform_api_datto_rmm_devices import TransformApiDattoRMM

from loguru import logger
import sys
import os
import inspect
import traceback
import concurrent.futures
from pathlib import Path
import pandas as pd
import json

results_list = []


@task(tags=["extract", "get", "api", "batch"])
def extract_api_datto_rmm_devices(config: dict, vault: VaultManager) -> pd.DataFrame:
    """
    Extracts device data from the Datto RMM API.
    """
    try:
        extract = ExtractApiDattoRMM(config=config, vault=vault)
        data = extract.create_devices_dataframe()
        result = data["result"]
        df = data["data"]

        df['_SOURCE_PRODUCT'] = config["DETAILS"]["product"]
        df['_SOURCE_SUBJECT'] = config["DETAILS"]["subject"]
        df['_SOURCE_ORIGIN'] = config["DATA"]["origin"]
        df['_UTC_EXTRACTION_DATETIME'] = config["TIMESTAMPS"]["_IN_DATA_TIMESTAMP"]

        results_list.append(result)
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
def transform_dataframe(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Transforms the raw extracted Datto RMM device data.
    """
    try:
        transform = TransformApiDattoRMM(df)
        data = transform.transform_devices_dataframe()
        result = data["result"]
        df = data["data"]

        results_list.append(result)
        return df

    except Exception as e:
        t = traceback.format_exc()
        result = {
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": f"Error: {e}"
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
    Inserts transformed data into a PostgreSQL table.
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


@flow
def stg_api_datto_rmm_devices() -> None:
    """
    Main flow to orchestrate Datto RMM device ETL:
    - Extract → Transform → Load (MinIO + PostgreSQL)
    """
    print(f"[INFO] Current working directory: {os.getcwd()}")

    tasks = prepare_tasks(config_dir=f"{Path(__file__).parent.resolve()}/config/devices/config.yaml")["data"]
    vault = VaultManager()

    df = extract_api_datto_rmm_devices(config=tasks[0], vault=vault)
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
    stg_api_datto_rmm_devices()