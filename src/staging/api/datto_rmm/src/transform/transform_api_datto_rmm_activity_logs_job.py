import datetime as dt  # datetime manipulation
import traceback  # view stack
import inspect  # get function name
import sys

import pandas as pd  # dataframe manipulation
import json  # deserialize
import re  # regex


class TransformApiDattoRMM:
    def __init__(self, df: pd.DataFrame) -> None:
        self.__df = df

        # transform explode patch cols
        self.transform_explode_patch_details()

    @property
    def df(self) -> pd.DataFrame:
        return self.__df

    def transform_activity_logs_patch_dataframe(self) -> dict:
        return {
            "data": self.__df,
            "result": {
                "job_title": inspect.currentframe().f_code.co_name,
                "status_code": 200,
                "message": "DataFrame created successfully"
            }
        }

    def transform_explode_patch_details(self):
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')

        def model(data_dict: dict) -> dict:
            try:
                model_dict = data_dict

                _details = {} if data_dict.get('details', None) is None else json.loads(data_dict['details'])

                model_dict['device_hostname'] = _details.get('device.hostname', None)
                if model_dict['device_hostname']:
                    model_dict['device_hostname'] = model_dict['device_hostname'].upper()

                model_dict['device_uid'] = _details.get('device.uid', None)
                model_dict['entity'] = _details.get('entity', None)
                model_dict['event_action'] = _details.get('event.action', None)
                model_dict['event_category'] = _details.get('event.category', None)
                model_dict['patch_activity_action'] = _details.get('patch_activity.action', None)
                model_dict['patch_activity_from_cache'] = _details.get('patch_activity.from_cache', None)

                model_dict['patch_activity_patch_install_end'] = pd.to_datetime(
                    _details.get('patch_activity.patch_install_end', pd.NaT), unit='ms', errors='coerce')

                model_dict['patch_activity_patch_install_start'] = pd.to_datetime(
                    _details.get('patch_activity.patch_install_start', pd.NaT), unit='ms', errors='coerce')

                model_dict['patch_activity_patch_uid'] = _details.get('patch_activity.patch_uid', None)
                model_dict['patch_activity_policy_uid'] = _details.get('patch_activity.policy_uid', None)
                model_dict['patch_activity_run_date'] = pd.to_datetime(
                    _details.get('patch_activity.run_date', pd.NaT), unit='ms', errors='coerce')

                model_dict['patch_activity_success'] = _details.get('patch_activity.success', None)
                model_dict['patch_update_end_date'] = pd.to_datetime(
                    _details.get('patch_update.end_date', pd.NaT), unit='ms', errors='coerce')

                model_dict['patch_update_id'] = _details.get('patch_update.id', None)
                model_dict['patch_update_start_date'] = pd.to_datetime(
                    _details.get('patch_update.start_date', pd.NaT), unit='ms', errors='coerce')

                model_dict['patch_update_title'] = _details.get('patch_update.title', None)
                model_dict['patch_update_uid'] = _details.get('patch_update.uid', None)
                model_dict['site_name'] = _details.get('site.name', None)
                model_dict['uid'] = _details.get('uid', None)
                model_dict['source_forwarded_ip'] = _details.get('source.forwarded_ip', None)

                model_dict['patch_activity_info'] = _details.get('patch_activity.info', ' ').split('\n') \
                    if _details.get('patch_activity.info', None) else []

                raw_result = _details.get('patch_activity.result', '')
                raw_title = _details.get('patch_update.title', '')

                model_dict['patch_activity_hresult'] = re.search(r'HResult\s+:\s(\d+x\w+)\n+.*', raw_result)
                if model_dict['patch_activity_hresult']:
                    model_dict['patch_activity_hresult'] = model_dict['patch_activity_hresult'].group(1)

                model_dict['patch_activity_patch_title'] = re.search(r'([^\'\(]+)\s\(?.*', raw_title)
                if model_dict['patch_activity_patch_title']:
                    model_dict['patch_activity_patch_title'] = model_dict['patch_activity_patch_title'].group(1)

                model_dict['patch_activity_kb_id'] = re.search(r'.*\((KB\d+)\).*', raw_title)
                if model_dict['patch_activity_kb_id']:
                    model_dict['patch_activity_kb_id'] = model_dict['patch_activity_kb_id'].group(1)

                model_dict['patch_activity_update_source'] = re.search(r'Update Source\s+:\s(\w+)', raw_result)
                if model_dict['patch_activity_update_source']:
                    model_dict['patch_activity_update_source'] = model_dict['patch_activity_update_source'].group(1)

                model_dict['patch_activity_message_text'] = re.search(r'Message Text\s+:\s(.*)', raw_result)
                if model_dict['patch_activity_message_text']:
                    model_dict['patch_activity_message_text'] = model_dict['patch_activity_message_text'].group(1)

                return model_dict

            except Exception as e:
                t = traceback.format_exc()
                sys.exit(t)

        try:
            self.__df = pd.DataFrame([model(data) for data in self.__df.to_dict(orient='records')])
            self.__df.drop(['details'], axis=1, inplace=True)

        except Exception as e:
            t = traceback.format_exc()
            sys.exit(t)