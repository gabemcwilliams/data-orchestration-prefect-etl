from prefect import flow, task
from prefect.artifacts import *

from connections.conn_postgresql import *

from utilities.setup_logger import *
from utilities.task_prep import *
from utilities.vault_mgr import *

import sys
import inspect
import traceback
import polars as pl
from pathlib import Path

# collects task results >> cli print(results_list)
results_list = []


# Postgres [EXTRACT]
@task(tags=["extract", "get", "database", "postgresql"])
def read_postgres(conn_configs: dict) -> dict:
    try:
        database = 'staging'

        postgres = ConnPostgresql(config=conn_configs['postgres_linapi'])
        data = postgres.conn_to_postgres(database=database)
        conn = data['engine']

        result = data['result']
        result['operation'] = 'read'
        results_list.append(result)

        with open(f'{Path(__file__).parent.resolve()}/sql/mart_datto_rmm_ms_patch_events.sql', 'r') as s:
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


# Postgres [LOAD]
@task(tags=["load", "put", "database", "postgresql"])
def write_postgres(df, conn_configs: dict) -> None:
    try:
        database = 'marts'
        schema = 'public'
        table = f'{schema}.mart_datto_rmm_ms_patch_events'

        postgres = ConnPostgresql(config=conn_configs['postgres_linapi'])
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


# RUN FLOW
@flow
def mart_datto_rmm_ms_patch_events() -> None:
    print(f"The script is being run from: {os.getcwd()}")
    conn_configs = read_config(config_dir=f"{Path(__file__).parent.resolve()}/config/main/config.yaml")["data"]

    print(conn_configs)

    df = read_postgres(conn_configs=conn_configs)
    write_postgres(df=df, conn_configs=conn_configs)

    print("#" * 75)
    print("\n        FINAL RESULTS\n")
    print("--------------------------------")

    for result in results_list:
        print("\n")
        print(json.dumps(result, indent=4))
        print("\n---------")

    print("\n")
    print("#" * 75)


if __name__ in "__main__":
    mart_datto_rmm_ms_patch_events()
