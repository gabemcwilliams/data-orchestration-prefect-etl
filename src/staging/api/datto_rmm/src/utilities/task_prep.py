"""
Task Preparation Utility

This function is used to:
- Read a task-specific config.yaml
- Inject standardized UTC timestamp metadata into each task
- Return a structured object containing task configs and metadata
"""

import yaml
import datetime as dt
import inspect
import os
from loguru import logger


def prepare_tasks(config_dir: str = "./config.yaml") -> dict:
    """
    Loads and prepares a list of ETL task configurations from a YAML file.

    Args:
        config_dir (str): Full or relative path to the YAML config file.

    Returns:
        dict: {
            "data": [list of stamped tasks],
            "result": {
                "task_name": ...,
                "status_message": "Success" | "Failure",
                "status_code": 200 | 500
            }
        }

    Notes:
        - Each task in the YAML should be listed under `TASKS:`.
        - Timestamps injected are consistent across tasks.
    """
    try:
        stamped_tasks = []

        try:
            with open(config_dir, "r") as stream:
                yaml_config = yaml.safe_load(stream)
        except Exception as e:
            print(f"[ERROR] Could not load config: {e}")
            print(f"[DEBUG] Current working directory: {os.getcwd()}")
            exit(1)

        TASKS = yaml_config["TASKS"]

        # Consistent UTC timestamp metadata for logging and file tagging
        utc_now = dt.datetime.now(dt.timezone.utc)
        data_timestamps = {
            "_IN_DATA_TIMESTAMP": utc_now.strftime('%Y-%m-%d %H:%M:%S'),  # for display/logs
            "_OUT_DATA_TIMESTAMP": utc_now.strftime('%Y_%m_%d_%H%M%S'),    # for filenames
            "_YEAR_DATA_TIMESTAMP": f'{utc_now.year}',
            "_MONTH_DATA_TIMESTAMP": f'{utc_now.month:02d}',
            "_DAY_DATA_TIMESTAMP": f'{utc_now.day:02d}',
        }

        for TASK in TASKS:
            updated_task = TASK.copy()
            updated_task["TIMESTAMPS"] = data_timestamps
            stamped_tasks.append(updated_task)

        return {
            "data": stamped_tasks,
            "result": {
                "task_name": inspect.currentframe().f_code.co_name,
                "status_message": "Success",
                "status_code": 200,
            }
        }

    except Exception as e:
        return {
            "result": {
                "task_name": inspect.currentframe().f_code.co_name,
                "status_message": str(e),
                "status_code": 500,
            }
        }
