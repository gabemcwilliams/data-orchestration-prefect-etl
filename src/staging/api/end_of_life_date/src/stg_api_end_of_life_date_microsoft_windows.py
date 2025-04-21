"""
Microsoft Windows End of Life ETL Flow

This flow:
1. Extracts workstation and server EOL data via public API
2. Transforms the combined data into a normalized schema
3. Loads results into PostgreSQL
"""

from prefect import flow, task
from prefect.artifacts import *

from load.load_minio import *
from load.load_postgres import *

from utilities.setup_logger import *
from utilities.task_prep import *
from utilities.vault_mgr import *

from extract.extract_api_end_of_life_date import *
from transform.transform_api_end_of_life_date_microsoft_windows import *

import sys
import os
import inspect
import traceback
import concurrent.futures
import pandas as pd
from pathlib import Path
import json

results_list = []


@task(tags=["extract", "get", "api", "batch"])
def extract_api_end_of_life_date_microsoft_windows_workstation(config: dict, vault: VaultManager) -> pd.DataFrame:
    """
    Extracts Microsoft Windows workstation EOL data from the public API.
    Returns a DataFrame with extracted data and metadata columns.
    """
    try:
        extract = ExtractApiEndOfLifeDate(config=config, vault=vault)
        data = extract.create_windows_dataframe()
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
        print(t)
        return pd.DataFrame()


@task(tags=["extract", "get", "api", "batch"])
def extract_api_end_of_life_date_microsoft_windows_server(config: dict, vault: VaultManager) -> pd.DataFrame:
    """
    Extracts Microsoft Windows Server EOL data from the public API.
    Returns a DataFrame with extracted data and metadata columns.
    """
    try:
        extract = ExtractApiEndOfLifeDate(config=config, vault=vault)
        data = extract.create_windows_server_dataframe()
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
        print(t)
        return pd.DataFrame()


@task(tags=["transform"])
def transform_dataframe(df_workstation, df_server, config: dict) -> pd.DataFrame:
    """
    Concatenates workstation and server DataFrames,
    then transforms them into a normalized schema.
    """
    try:
        df = pd.concat([df_workstation, df_server], ignore_index=True)
        transform = TransformApiEndOfLifeDate(df)
        data = transform.transform_microsoft_windows_dataframe()
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


@task(tags=["load", "put", "database", "postgresql"])
def load_postgres(df: pd.DataFrame, config: dict, vault: VaultManager) -> None:
    """
    Inserts the transformed EOL data into a PostgreSQL table.
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
        print(t)
        sys.exit(1)


@flow
def stg_api_end_of_life_date_microsoft_windows() -> None:
    """
    Main flow to orchestrate EOL date processing:
    - Extract workstation and server data
    - Transform into normalized schema
    - Load into PostgreSQL
    """
    print(f"[INFO] Current working directory: {os.getcwd()}")

    tasks = prepare_tasks(config_dir=f"{Path(__file__).parent.resolve()}/config/microsoft_windows/config.yaml")["data"]
    vault = VaultManager()

    df_workstation = extract_api_end_of_life_date_microsoft_windows_workstation(config=tasks[0], vault=vault)
    df_server = extract_api_end_of_life_date_microsoft_windows_server(config=tasks[1], vault=vault)

    df = transform_dataframe(df_workstation=df_workstation, df_server=df_server, config=tasks[2])
    load_postgres(df=df, config=tasks[3], vault=vault)

    print("#" * 75)
    print("\n        FINAL RESULTS\n")
    print("--------------------------------")
    for result in results_list:
        print("\n" + json.dumps(result, indent=4))
        print("---------")
    print("\n" + "#" * 75)


if __name__ == "__main__":
    stg_api_end_of_life_date_microsoft_windows()