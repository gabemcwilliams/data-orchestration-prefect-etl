"""
Datto RMM Account Sites ETL Flow

This flow:
1. Extracts all account-level site metadata
2. Appends standard source metadata columns
3. Loads results into MinIO and PostgreSQL in parallel
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
def extract_api_datto_rmm_account_sites(config: dict, vault: VaultManager) -> pd.DataFrame:
    """
    Extracts account site metadata from Datto RMM API and appends source markers.
    """
    try:
        datto = ExtractApiDattoRMM(config=config, vault=vault)
        data = datto.create_account_sites_dataframe()
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


@task(tags=["load", "put", "object_storage", "minio"])
def load_minio(df: pd.DataFrame, config: dict, vault: VaultManager) -> None:
    """
    Uploads account site metadata to MinIO object storage.
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
    Loads account site metadata into PostgreSQL.
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


@flow(name="stg_api_datto_rmm_account_sites")
def stg_api_datto_rmm_account_sites() -> None:
    """
    Main flow to extract, transform, and load Datto RMM account sites data.
    """
    print(f"[INFO] Running from: {os.getcwd()}")
    tasks = prepare_tasks(config_dir=f"{Path(__file__).parent.resolve()}/config/account_sites/config.yaml")["data"]

    vault = VaultManager()
    df = extract_api_datto_rmm_account_sites(config=tasks[0], vault=vault)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_minio = executor.submit(load_minio, df=df, config=tasks[1], vault=vault)
        future_postgres = executor.submit(load_postgres, df=df, config=tasks[2], vault=vault)
        concurrent.futures.wait([future_minio, future_postgres])

    print("#" * 75)
    print("\n        FINAL RESULTS\n")
    print("--------------------------------")
    for result in results_list:
        print("\n" + json.dumps(result, indent=4))
        print("---------")
    print("\n" + "#" * 75)


if __name__ == "__main__":
    stg_api_datto_rmm_account_sites()
