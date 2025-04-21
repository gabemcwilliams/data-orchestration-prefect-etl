"""
Datto RMM Account + Site Variables ETL Flow

This flow:
1. Extracts both account-level and site-level variables
2. Appends standard metadata fields
3. Merges both sources into one DataFrame
4. Loads results into MinIO and PostgreSQL in parallel
"""

from prefect import flow, task
from prefect.artifacts import *

from load.load_minio import *
from load.load_postgres import *

from utilities.setup_logger import *
from utilities.task_prep import *
from utilities.vault_mgr import *

from extract.extract_api_datto_rmm import *

import sys
import inspect
import traceback
import concurrent.futures
import pandas as pd
from pathlib import Path
import os
import json

results_list = []


@task(tags=["extract", "get", "api", "batch"])
def extract_api_datto_rmm_account_variables(config: dict, vault: VaultManager) -> pd.DataFrame:
    """
    Extracts account-level global variable metadata.
    """
    try:
        datto = ExtractApiDattoRMM(config=config, vault=vault)
        data = datto.create_account_variables_dataframe()
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
        results_list.append({
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": t
        })
        return pd.DataFrame()


@task(tags=["extract", "get", "api", "batch"])
def extract_api_datto_rmm_site_variables(config: dict, vault: VaultManager) -> pd.DataFrame:
    """
    Extracts site-level variable metadata.
    """
    try:
        datto = ExtractApiDattoRMM(config=config, vault=vault)
        data = datto.create_site_variables_dataframe()
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
        results_list.append({
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": t
        })
        return pd.DataFrame()


@task(tags=["load", "put", "object_storage", "minio"])
def load_minio(df: pd.DataFrame, config: dict, vault: VaultManager) -> None:
    """
    Uploads combined variable data to MinIO.
    """
    try:
        minio = MinioLoad(df_input=df, config=config, vault=vault)
        result = minio.upload_to_minio()["result"]
        results_list.append(result)

    except Exception:
        t = traceback.format_exc()
        results_list.append({
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": t
        })
        print(results_list)
        sys.exit(1)


@task(tags=["load", "put", "database", "postgresql"])
def load_postgres(df: pd.DataFrame, config: dict, vault: VaultManager) -> None:
    """
    Loads variable data into PostgreSQL.
    """
    try:
        postgres = PostgresLoad(df_input=df, config=config, vault=vault)
        result = postgres.load_to_postgres()["result"]
        results_list.append(result)

    except Exception:
        t = traceback.format_exc()
        results_list.append({
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": t
        })


@flow(name="stg_api_datto_rmm_account_site_variables")
def stg_api_datto_rmm_account_site_variables() -> None:
    """
    Executes the variable ETL pipeline:
    - Extracts account and site-level variables
    - Combines the two datasets
    - Loads into MinIO and PostgreSQL
    """
    print(f"[INFO] Running from: {os.getcwd()}")

    tasks = prepare_tasks(config_dir=f"{Path(__file__).parent.resolve()}/config/account_site_variables/config.yaml")["data"]
    vault = VaultManager()

    df_account_vars = extract_api_datto_rmm_account_variables(config=tasks[0], vault=vault)
    df_site_vars = extract_api_datto_rmm_site_variables(config=tasks[1], vault=vault)
    df_combined = pd.concat([df_account_vars, df_site_vars], ignore_index=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(load_minio, df=df_combined, config=tasks[2], vault=vault)
        executor.submit(load_postgres, df=df_combined, config=tasks[3], vault=vault)

    print("#" * 75)
    print("\n        FINAL RESULTS\n")
    print("--------------------------------")
    for result in results_list:
        print("\n" + json.dumps(result, indent=4))
        print("---------")
    print("\n" + "#" * 75)


if __name__ == "__main__":
    stg_api_datto_rmm_account_site_variables()
