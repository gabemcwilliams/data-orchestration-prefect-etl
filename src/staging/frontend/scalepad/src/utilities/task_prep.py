import yaml
import datetime as dt
import inspect
import os


# Pull Config Info from YAML
def prepare_tasks(config_dir: str = "./config.yaml") -> dict:
    try:
        stamped_tasks = []

        try:
            with open(config_dir, "r") as stream:
                yaml_config = yaml.safe_load(stream)
        except Exception as e:
            print(e)
            print(os.getcwd())
            exit(1)

        TASKS = yaml_config["TASKS"]

        utc_now = dt.datetime.now(dt.timezone.utc)  # datetime for cataloging

        data_timestamps = {

            "_IN_DATA_TIMESTAMP": utc_now.strftime('%Y-%m-%d %H:%M:%S'),  # timestamp found inside data
            "_OUT_DATA_TIMESTAMP": utc_now.strftime('%Y_%m_%d_%H%M%S'),  # timestamp found in file
            "_YEAR_DATA_TIMESTAMP": f'{utc_now.year}',  # folder and path dates
            "_MONTH_DATA_TIMESTAMP": f'{utc_now.month:02d}',  # folder and path dates
            "_DAY_DATA_TIMESTAMP": f'{utc_now.day:02d}',  # folder and path dates

        }

        for TASK in TASKS:
            updated_task = TASK.copy()
            updated_task["TIMESTAMPS"] = data_timestamps
            stamped_tasks.append(updated_task)

        result = {
            "data": stamped_tasks,
            "result": {
                "task_name": inspect.currentframe().f_code.co_name,
                "status_message": "Success",
                "status_code": 200
            }
        }

        return result

    except Exception as e:
        result = {
            "result": {
                "task_name": inspect.currentframe().f_code.co_name,
                "status_message": str(e),
                "status_code": 500
            }
        }

        return result
