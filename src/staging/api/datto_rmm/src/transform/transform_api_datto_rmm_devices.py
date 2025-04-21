"""
Transform logic for Datto RMM device records.

Performs:
- Patch status calculations
- Audit and reboot duration flags
- OS version and type parsing
- Cloud provider tagging (e.g., AWS Workspaces)

All transformations modify `self.__df` in place and return a unified output via `.transform_devices_dataframe()`.
"""

import datetime as dt
import traceback
import inspect
import sys
import re
import pandas as pd


class TransformApiDattoRMM:
    """
    Class to encapsulate and apply all transformation logic to the Datto RMM devices dataset.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self.__df = df

        self.append_patch_percentage_column()
        self.append_calculated_columns()
        self.replace_undefined_values()
        self.parse_os_ver_info()
        self.transform_cloud_category_cols()

    @property
    def df(self) -> pd.DataFrame:
        return self.__df

    def transform_devices_dataframe(self) -> dict:
        """
        Wraps and returns the transformed dataframe in a structured format.
        """
        return {
            "data": self.__df,
            "result": {
                "job_title": inspect.currentframe().f_code.co_name,
                "status_code": 200,
                "message": "DataFrame created successfully"
            }
        }

    def append_patch_percentage_column(self) -> None:
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')

        def model(data_dict: dict) -> dict:
            try:
                model_dict = data_dict.copy()

                total_patches = (
                    model_dict.get('patches_approved_pending', 0) +
                    model_dict.get('patches_installed', 0)
                )

                if total_patches != 0:
                    model_dict['patch_status_percentage'] = round(
                        model_dict.get('patches_installed', 0) / total_patches * 100, 2
                    )
                else:
                    model_dict['patch_status_percentage'] = 0

                return model_dict
            except Exception:
                t = traceback.format_exc()
                sys.exit(t)

        self.__df = pd.DataFrame([model(data) for data in self.__df.to_dict(orient='records')])

    def append_calculated_columns(self) -> None:
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')

        try:
            now = dt.datetime.now()

            self.__df['no_audit_last_30_days'] = self.__df['last_audit_date'].apply(
                lambda x: 1 if x < now - dt.timedelta(days=30) else 0)

            self.__df['offline_last_30_days'] = self.__df['adjusted_last_seen'].apply(
                lambda x: 1 if x < now - dt.timedelta(days=30) else 0)

            self.__df['no_reboot_last_30_days'] = self.__df['last_reboot'].apply(
                lambda x: 1 if x < now - dt.timedelta(days=30) else 0)

        except Exception:
            t = traceback.format_exc()
            sys.exit(t)

    def replace_undefined_values(self) -> None:
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')

        try:
            self.__df.replace({'null': None, '': None}, inplace=True)
        except Exception:
            t = traceback.format_exc()
            sys.exit(t)

    def parse_os_ver_info(self) -> None:
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')

        def model(data_dict: dict) -> dict:
            try:
                model_dict = data_dict
                os_string = model_dict.get('operating_system', '') or ''

                # Extract basic version info
                version_match = re.search(r'.*\s(\d+\.\d+\.\d+).*', os_string)
                model_dict['os_build'] = version_match.group(1) if version_match else None

                name_match = re.search(r'(.*)\s(\d+\.\d+\.\d+).*', os_string)
                model_dict['os_name'] = name_match.group(1) if name_match else None

                os_lower = os_string.lower()
                model_dict['os_type'] = (
                    'Microsoft' if 'windows' in os_lower else
                    'Linux' if 'linux' in os_lower else
                    'MacOS' if 'mac' in os_lower else
                    'Unknown'
                )

                # Edition parsing (Windows)
                if model_dict['os_type'] == 'Microsoft':
                    if any(term in os_lower for term in [' pro', ' home', ' workstations', ' business']):
                        model_dict['os_release_edition'] = 'Workstation'
                    elif 'enterprise' in os_lower:
                        model_dict['os_release_edition'] = 'Enterprise'
                    elif 'iot' in os_lower:
                        model_dict['os_release_edition'] = 'IoT'
                    else:
                        model_dict['os_release_edition'] = 'Standard'

                model_dict['os_is_lts'] = ' lts' in os_lower

                # Optional: release info field (e.g., "20H2", "2016")
                release_match = re.match(r'(\w{5,}\s)+([\dA-Z\.]{1,4}\s?(R2)?).*', os_string)
                model_dict['release_info'] = release_match.group(2).strip() if release_match else None

                return model_dict

            except Exception:
                t = traceback.format_exc()
                sys.exit(t)

        try:
            self.__df = pd.DataFrame([model(data) for data in self.__df.to_dict(orient='records')])
        except Exception:
            t = traceback.format_exc()
            sys.exit(t)

    def transform_cloud_category_cols(self) -> None:
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')

        def model(data_dict: dict) -> dict:
            try:
                model_dict = data_dict

                # Identify AWS Workspace patterns in hostname
                result = re.match(r'^(\bEC2AMAZ\b|\bWSAMZN\b|\bIP-\b).*', model_dict.get('hostname', ''))
                if result:
                    model_dict['cloud_category'] = 'AWS'
                    model_dict['cloud_type'] = 'Workspace'

                return model_dict

            except Exception:
                t = traceback.format_exc()
                sys.exit(t)

        self.__df = pd.DataFrame([model(data) for data in self.__df.to_dict(orient='records')])
