from prefect import flow, task
from prefect.artifacts import *

from extract import *
from load import *

from transform import *

from utilities import *

import os
import sys
import yaml
import inspect
import traceback
import concurrent.futures
import pandas as pd
from pathlib import Path

# collects task results >> cli print(results_list)
results_list = []


# API [EXTRACT]
@task(tags=["extract", "get", "api", "batch"])
def extract_scalepad_frontend_hardware_assets(config: dict, vault: VaultManager, otp_gen=GenerateOTP):
    try:
        scalepad = ExtractScalepad(config=config, vault=vault, otp_gen=otp_gen)

        data = scalepad.create_hardware_assets_dataframe()
        result = data["result"]
        results_list.append(result)
        df = data["data"]

        # add marker columns
        df['_SOURCE_PRODUCT'] = config["DETAILS"]["product"]
        df['_SOURCE_SUBJECT'] = config["DETAILS"]["subject"]
        df['_SOURCE_ORIGIN'] = config["DATA"]["origin"]
        df['_UTC_EXTRACTION_DATETIME'] = config["TIMESTAMPS"]["_IN_DATA_TIMESTAMP"]

        return df

    except Exception as e:
        t = traceback.format_exc()
        result = {
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": t
        }

        results_list.append(result)
        print(results_list)
        sys.exit(1)


# Minio [LOAD]
@task(tags=["load", "put", "object_storage", "minio"])
def load_minio(df: pd.DataFrame, config: dict, vault: VaultManager) -> None:
    try:
        minio = MinioLoad(df_input=df, config=config, vault=vault)
        data = minio.upload_to_minio()
        result = data["result"]
        results_list.append(result)

    except Exception as e:
        t = traceback.format_exc()
        result = {
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": t
        }

        results_list.append(result)
        print(results_list)
        sys.exit(1)


# # DataFrame [TRANSFORM]
# @task(tags=["transform"])
# def transform_dataframe(df, config: dict):
#     try:
#         transform = TransformApiscalepad(df)
#         data = transform.transform_hardware_assets_dataframe()
#         result = data["result"]
#         df = data["data"]
#         results_list.append(result)
#
#         return df
#
#     except Exception as e:
#         t = traceback.format_exc()
#         result = {
#             "task_name": inspect.currentframe().f_code.co_name,
#             "status_code": 500,
#             "message": f"Error: {e}",
#         }
#
#         results_list.append(result)
#         print(results_list)
#         sys.exit(1)


# Postgres [LOAD]
@task(tags=["load", "put", "database", "postgresql"])
def load_postgres(df: pd.DataFrame, config: dict, vault: VaultManager) -> None:
    try:
        df.rename(columns={"uid": "id"}, inplace=True)

        postgres = PostgresLoad(df_input=df, config=config, vault=vault)
        data = postgres.load_to_postgres()
        result = data["result"]
        results_list.append(result)

    except Exception as e:
        t = traceback.format_exc()
        result = {
            "task_name": inspect.currentframe().f_code.co_name,
            "status_code": 500,
            "message": t
        }

        results_list.append(result)
        print(results_list)
        sys.exit(1)


# RUN FLOW
@flow
def stg_frontend_scalepad_hardware_assets() -> None:
    print(f"The script is being run from: {os.getcwd()}")
    tasks = prepare_tasks(config_dir=f"{Path(__file__).parent.resolve()}/config/assets/config.yaml")["data"]

    vault = VaultManager()
    otp_gen = GenerateOTP()

    df = extract_scalepad_frontend_hardware_assets(config=tasks[0], vault=vault, otp_gen=otp_gen)
    load_minio(df=df, config=tasks[1], vault=vault)  # review if this is needed to be stored for later retrieval
    # df = transform_dataframe(df, tasks[2])
    load_postgres(df=df, config=tasks[3], vault=vault)

    print("#" * 75)
    print("\n        FINAL RESULTS\n")
    print("--------------------------------")

    for result in results_list:
        print("\n")
        print(json.dumps(result, indent=4))
        print("---------")

    print("\n")
    print("#" * 75)


if __name__ in "__main__":
    api_url = os.environ.get("PREFECT_API_URL")
    if not api_url:
        raise EnvironmentError("PREFECT_API_URL is not set. Please set it in the environment.")

    print(f"PREFECT_API_URL: {api_url}")

    flow_func = stg_frontend_scalepad_hardware_assets
    flow_name = flow_func.__name__

    flow_func.from_source(
        source=str(Path(__file__).parent.resolve()),
        entrypoint=f"{Path(__file__).name}:{flow_name}"
    ).deploy(
        name=flow_name,
        work_pool_name="default-worker-pool",  # Or load from env/config
    )