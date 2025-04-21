import datetime as dt
import traceback
import inspect
import sys

import pandas as pd
import json
import re


class TransformApiDattoRMM:
    def __init__(self, df: pd.DataFrame) -> None:
        """Initializes the Transform class and applies all necessary patch log transformations."""
        self.__df = df
        self.transform_explode_patch_details()

    @property
    def df(self) -> pd.DataFrame:
        return self.__df

    def transform_activity_logs_patch_dataframe(self) -> dict:
        """Returns transformed patch dataframe with status metadata."""
        return {
            "data": self.__df,
            "result": {
                "job_title": inspect.currentframe().f_code.co_name,
                "status_code": 200,
                "message": "DataFrame created successfully"
            }
        }

    def transform_explode_patch_details(self) -> None:
        """Explodes the nested patch log JSON details into structured DataFrame columns."""
        print(f"\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n")

        def model(data_dict: dict) -> dict:
            try:
                model_dict = data_dict.copy()
                _details = {} if data_dict.get("details") is None else json.loads(data_dict["details"])

                model_dict['device_hostname'] = _details.get('device.hostname', '').upper() or None
                model_dict['device_uid'] = _details.get('device.uid')
                model_dict['entity'] = _details.get('entity')
                model_dict['event_action'] = _details.get('event.action')
                model_dict['event_category'] = _details.get('event.category')
                model_dict['patch_activity_action'] = _details.get('patch_activity.action')
                model_dict['patch_activity_from_cache'] = _details.get('patch_activity.from_cache')

                model_dict['patch_activity_patch_install_end'] = pd.to_datetime(
                    _details.get('patch_activity.patch_install_end', pd.NaT), unit='ms', errors='coerce')
                model_dict['patch_activity_patch_install_start'] = pd.to_datetime(
                    _details.get('patch_activity.patch_install_start', pd.NaT), unit='ms', errors='coerce')
                model_dict['patch_activity_patch_uid'] = _details.get('patch_activity.patch_uid')
                model_dict['patch_activity_policy_uid'] = _details.get('patch_activity.policy_uid')
                model_dict['patch_activity_run_date'] = pd.to_datetime(
                    _details.get('patch_activity.run_date', pd.NaT), unit='ms', errors='coerce')
                model_dict['patch_activity_success'] = _details.get('patch_activity.success')

                model_dict['patch_update_end_date'] = pd.to_datetime(
                    _details.get('patch_update.end_date', pd.NaT), unit='ms', errors='coerce')
                model_dict['patch_update_id'] = _details.get('patch_update.id')
                model_dict['patch_update_start_date'] = pd.to_datetime(
                    _details.get('patch_update.start_date', pd.NaT), unit='ms', errors='coerce')
                model_dict['patch_update_title'] = _details.get('patch_update.title')
                model_dict['patch_update_uid'] = _details.get('patch_update.uid')

                model_dict['site_name'] = _details.get('site.name')
                model_dict['uid'] = _details.get('uid')
                model_dict['source_forwarded_ip'] = _details.get('source.forwarded_ip')

                model_dict['patch_activity_info'] = (
                    _details.get('patch_activity.info', '').split('\n') if _details.get('patch_activity.info') else []
                )

                # Regex extracts from patch_activity.result
                result_str = _details.get('patch_activity.result', '')

                model_dict['patch_activity_hresult'] = re.search(
                    r'HResult\s+:\s(0x\w+)', result_str
                ).group(1) if re.search(r'HResult\s+:\s(0x\w+)', result_str) else None

                title_str = _details.get('patch_update.title', '')
                model_dict['patch_activity_patch_title'] = re.search(
                    r'([^\'(]+)\s\(?.*', title_str
                ).group(1) if re.search(r'([^\'(]+)\s\(?.*', title_str) else None

                model_dict['patch_activity_kb_id'] = re.search(
                    r'.*\((KB\d+)\).*', title_str
                ).group(1) if re.search(r'.*\((KB\d+)\).*', title_str) else None

                model_dict['patch_activity_update_source'] = re.search(
                    r'Update Source\s+:\s(\w+)', result_str
                ).group(1) if re.search(r'Update Source\s+:\s(\w+)', result_str) else None

                model_dict['patch_activity_message_text'] = re.search(
                    r'Message Text\s+:\s(.*)', result_str
                ).group(1) if re.search(r'Message Text\s+:\s(.*)', result_str) else None

                return model_dict

            except Exception:
                sys.exit(traceback.format_exc())

        try:
            self.__df = pd.DataFrame([model(data) for data in self.__df.to_dict(orient='records')])
            self.__df.drop(['details'], axis=1, inplace=True)
        except Exception:
            sys.exit(traceback.format_exc())
