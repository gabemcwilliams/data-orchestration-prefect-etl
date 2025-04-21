import datetime as dt
import traceback
import inspect
import sys
import pandas as pd
import json
import re
from loguru import logger


class TransformApiDattoRMM:
    """
    Class responsible for transforming Datto RMM API response data into a cleaned and structured pandas DataFrame.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        """Initialize and apply all transformation steps."""
        self.__df = df

        self.transform_response_actions()
        self.transform_timestamps()
        self.transform_alert_context()
        self.transform_replace_nan_with_none()

    @property
    def df(self) -> pd.DataFrame:
        """Returns the transformed DataFrame."""
        return self.__df

    def transform_monitors_dataframe(self) -> dict:
        """Wraps the DataFrame in a response-style dictionary for downstream usage."""
        return {
            "data": self.__df,
            "result": {
                "job_title": inspect.currentframe().f_code.co_name,
                "status_code": 200,
                "message": "DataFrame created successfully"
            }
        }

    def transform_response_actions(self):
        """Converts 'responseActions' column from list/dict format to a JSON string, ensuring a consistent structure."""
        logger.info(f"[START] - {inspect.currentframe().f_code.co_name}")

        try:
            def split_to_cols_ra_list(row):
                default_action = [{
                    "action_time": 0,
                    "action_type": None,
                    "description": None,
                    "action_reference": None,
                    "action_reference_int": None
                }]
                self.__df.loc[row.index, 'response_actions'] = json.dumps(
                    row['response_actions'] if row.get('response_actions') is not None else default_action
                )

            self.__df.apply(lambda row: split_to_cols_ra_list(row), axis=1)

        except Exception:
            logger.exception("Error during response_actions transformation")
            sys.exit(traceback.format_exc())

    def transform_timestamps(self):
        """Converts 'timestamp' from Unix timestamp to pandas datetime."""
        logger.info(f"[START] - {inspect.currentframe().f_code.co_name}")

        try:
            def fix_timestamp(row):
                self.__df.loc[row.index, 'timestamp'] = (
                    pd.NaT if row.get('timestamp') is None else dt.datetime.fromtimestamp(row['timestamp'])
                )

            self.__df.apply(lambda row: fix_timestamp(row), axis=1)

        except Exception:
            logger.exception("Error during timestamp transformation")
            sys.exit(traceback.format_exc())

    def transform_alert_context(self):
        """
        Parses and flattens the 'alert_context' field, handles class-based structure,
        and normalizes field names into snake_case.
        """
        logger.info(f"[START] - {inspect.currentframe().f_code.co_name}")

        try:
            def class_context(alert_context):
                """
                TODO: Not currently in use. Alert context input is highly dynamic and too complex
                to flatten reliably across all @class types. This function may be used later to
                extract structured fields from dicts using regex as needed.
                """
                try:
                    match alert_context.get("@class"):
                        case 'comp_script_ctx':
                            return alert_context.get("samples")
                        case 'perf_disk_usage_ctx':
                            pattern = re.compile(r'(diskName)[^A-Z]+([A-Z])[^a-z]+(totalVolume)[^\d.]+([\d.]+)[^a-z]+(freeSpace)[^\d.]+([\d.]+)')
                            parsed = pattern.findall(str(alert_context))
                            return {
                                'disk_name': parsed[0][1],
                                'total_volume': parsed[0][3],
                                'free_space': parsed[0][5],
                                'percent_remaining': round(float(parsed[0][5]) / float(parsed[0][3]) * 100, 2)
                            }
                        case 'perf_resource_usage_ctx':
                            pattern = re.compile(r'(percentage)[^\d.]+([\d.]+)[^a-z]+(type)\W+([A-Z]+)')
                            parsed = pattern.findall(str(alert_context))
                            return {'percentage': parsed[0][1], 'type': parsed[0][3]}
                        case 'online_offline_status_ctx':
                            return {"status": "OFFLINE"}
                        case 'srvc_status_ctx':
                            return {
                                'serviceName': alert_context.get("service_name"),
                                'status': alert_context.get("status")
                            }
                        case _:
                            return None
                except Exception:
                    logger.exception("Error parsing alert_context")
                    return {"status_code": 500, "function": "parse_alert_context", "message": traceback.format_exc()}

            self.__df["alert_class"] = self.__df["alert_context"].apply(lambda x: x.get("@class"))

            for index, row in self.__df.iterrows():
                for k, v in row['alert_context'].items():
                    if k == '@class':
                        self.__df.loc[index, 'alert_context_class'] = v
                    else:
                        self.__df.loc[index, f'alert_context_{k[0].capitalize()}{k[1:]}'] = v

            for index, row in self.__df.iterrows():
                for k in list(row.keys()):
                    if k.startswith('alert_context_'):
                        new_name = re.sub(r'(?<![A-Z])(?=[A-Z]){3,}', '_', k)
                        new_name = re.sub(r'_{2,}', '_', new_name).lower()
                        self.__df.rename(columns={k: new_name}, inplace=True)

            self.__df['alert_context_last_triggered'] = pd.to_datetime(
                self.__df['alert_context_last_triggered'], unit='ms')

            self.__df.drop('alert_context', axis=1, inplace=True)

        except Exception:
            logger.exception("Error during alert_context transformation")
            sys.exit(traceback.format_exc())

    def transform_replace_nan_with_none(self):
        """Replaces string 'nan' with Python None for SQL compatibility."""
        logger.info(f"[START] - {inspect.currentframe().f_code.co_name}")
        try:
            self.__df.replace({"nan": None}, inplace=True)
        except Exception:
            logger.exception("Error replacing 'nan' with None")
            sys.exit(traceback.format_exc())