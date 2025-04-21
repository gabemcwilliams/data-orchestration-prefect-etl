"""
Datto RMM Device Mart ETL Flow

This intermediate data mart flow:
1. Extracts normalized device records from staging schema (PostgreSQL)
2. Loads curated records into the 'marts' schema for analytics and reporting
"""

from prefect import flow, task
from prefect.artifacts import *

from connections.conn_postgresql import *

from utilities.setup_logger import *
from utilities.task_prep import *
from utilities.vault_mgr import *

import sys
import os
import inspect
import traceback
import polars as pl
from pathlib import Path
import json

results_list = []


@task(tags=["extract", "get", "database", "postgresql"])
def read_postgres(conn_configs: dict, vault: VaultManager) -> pl.DataFrame:
    """
    Extracts curated device records from the staging schema using a SQL file.
    Returns a Polars DataFrame.
    """
    try:
        database = 'staging'
        postgres = ConnPostgresql(config=conn_configs['postgres_linapi'], vault=vault)
        data = postgres.conn_to_postgres(database=database)
        conn = data['engine']

        result = data['result']
        result['operation'] = 'read'
        results_list.append(result)

        with open(f'{Path(__file__).parent.resolve()}/sql/mart_datto_rmm_devices.sql', 'r') as s:
            query = s.read()

        df = pl.read_database(connection=conn, query=query, infer_schema_length=None)
        return df

    except Exception as e:
        t = traceback.format_exc()
        result = {
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": t
        }
        results_list.append(result)


@task(tags=["load", "put", "database", "postgresql"])
def write_postgres(df: pl.DataFrame, conn_configs: dict, vault: VaultManager) -> None:
    """
    Writes the Polars DataFrame to the 'marts' schema, replacing the device mart table.
    """
    try:
        database = 'marts'
        schema = 'public'
        table = f'{schema}.mart_datto_rmm_devices'

        postgres = ConnPostgresql(config=conn_configs['postgres_linapi'], vault=vault)
        data = postgres.conn_to_postgres(database=database)
        conn = data['engine']

        result = data['result']
        result['operation'] = 'write'
        result['table'] = table
        results_list.append(result)

        df.write_database(
            table_name=table,
            connection=conn,
            if_table_exists='replace'
        )

    except Exception as e:
        t = traceback.format_exc()
        result = {
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": t
        }
        results_list.append(result)


@flow
def mart_datto_rmm_devices() -> None:
    """
    Main orchestration flow for populating the Datto RMM device mart.
    Extracts from staging and writes to the marts schema.
    """
    print(f"The script is being run from: {os.getcwd()}")
    conn_configs = read_config(config_dir=f"{Path(__file__).parent.resolve()}/config/main/config.yaml")['data']
    vault = VaultManager()

    print(conn_configs)

    df = read_postgres(conn_configs=conn_configs, vault=vault)
    write_postgres(df=df, conn_configs=conn_configs, vault=vault)

    print("#" * 75)
    print("\n        FINAL RESULTS\n")
    print("--------------------------------")
    for result in results_list:
        print("\n" + json.dumps(result, indent=4))
        print("\n---------")
    print("\n" + "#" * 75)


if __name__ == "__main__":
    mart_datto_rmm_devices()