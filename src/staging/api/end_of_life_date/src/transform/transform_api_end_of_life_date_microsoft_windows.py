"""
Transform logic for Microsoft Windows End-of-Life (EOL) API records.

This class performs:
- Parsing of 'release_label' and 'cycle' into normalized version info
- Duplication of rows for standard editions into workstation and enterprise
- Duplication of server LTSC rows to account for dual classification
- Cleanup of redundant release columns

All transformations modify `self.__df` in place.
"""

import datetime as dt
import traceback
import inspect
import sys
import re
import pandas as pd
from loguru import logger


class TransformApiEndOfLifeDate:
    """
    Applies transformation logic to Microsoft Windows EOL dataset.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self.__df = df

        self.transform_cycle_column()
        self.duplicate_standard_rows()
        self.duplicate_server_ltsc_rows()
        self.transform_drop_release_cols()

    @property
    def df(self) -> pd.DataFrame:
        return self.__df

    def transform_microsoft_windows_dataframe(self) -> dict:
        """
        Returns the transformed dataframe and status metadata.
        """
        return {
            "data": self.__df,
            "result": {
                "job_title": inspect.currentframe().f_code.co_name,
                "status_code": 200,
                "message": "DataFrame created successfully"
            }
        }

    def transform_cycle_column(self) -> None:
        """
        Parses 'release_label' and 'cycle' columns to extract structured fields:
        - release_model
        - release_version
        - os_release_edition
        - service_pack
        - release_info
        """
        def model(data_dict: dict = dict()) -> dict:
            try:
                model_dict = data_dict
                _release_label = " " if data_dict.get('release_label') is None else data_dict.get('release_label')

                result_1 = re.match(r"^(\d+\.?\d+?)\\s?.*", _release_label)
                result_2 = re.match(r'^(\S+)(\s?|-?)', _release_label)

                if result_1:
                    model_dict['release_model'] = str(result_1.group(1))
                elif result_2:
                    model_dict['release_model'] = str(result_2.group(1))
                else:
                    model_dict['release_model'] = None

                result = re.match(r".*\\s(\w{4})\\s.*", _release_label)
                model_dict['release_version'] = str(result.group(1)) if result else None

                try:
                    result = re.match(r".*\\(([EWLTSIo]{1,3})\\).*", _release_label)
                    model_dict['os_release_edition'] = result.group(1)
                except Exception:
                    model_dict['os_release_edition'] = "S"

                _cycle = " " if data_dict.get('cycle') is None else data_dict.get('cycle')

                result = re.match(r"^(\d{4})-?.*", _cycle)
                if result:
                    model_dict['release_version'] = str(result.group(1))

                result = re.match(r"^(\d{2}[A-Z]\d).*", _cycle)
                if result:
                    model_dict['release_version'] = str(result.group(1)).strip()
                else:
                    model_dict['release_version'] = None

                if model_dict['release_version'] is None:
                    result = re.match(r".*-(R2)-?.*", _cycle)
                    if result:
                        model_dict['release_version'] = str(result.group(1)).strip()

                result = re.match(r".*SP(\d+).*", _cycle)
                model_dict['service_pack'] = f"SP{result.group(1)}" if result else None

                if model_dict['release_version'] is None:
                    result = re.match(r"^(\w{4}).*", _cycle)
                    model_dict['release_version'] = str(result.group(1)) if result else None

                if 'iot' in _release_label.lower():
                    model_dict['os_release_edition'] = "IoT"
                elif '(e)' in _release_label.lower():
                    model_dict['os_release_edition'] = "Enterprise"
                elif '(w)' in _release_label.lower():
                    model_dict['os_release_edition'] = "Workstation"
                else:
                    model_dict['os_release_edition'] = "Standard"

                if model_dict['release_version'] is not None:
                    model_dict['release_info'] = model_dict['release_version']
                elif model_dict['release_model'] is not None:
                    model_dict['release_info'] = model_dict['release_model']
                else:
                    model_dict['release_info'] = None

                if model_dict['release_info'] and model_dict['release_info'].startswith('R2'):
                    result = re.match(r'(\w{4})-?.*', model_dict['cycle'])
                    if result:
                        model_dict['release_info'] = f"{result.group(1)} R2"

                return model_dict
            except Exception:
                t = traceback.format_exc()
                print(t)
                logger.error(t)
                sys.exit(t)

        self.__df = pd.DataFrame([model(data) for data in self.__df.to_dict(orient='records')])

    def duplicate_standard_rows(self):
        """
        Duplicates Standard rows to include additional editions:
        - Workstation
        - Enterprise
        - Keeps Standard with suffixes added to 'cycle'.
        """
        standard_edition_df = self.__df[self.__df['os_release_edition'] == 'Standard']
        _row_build_list = []

        for index, row in standard_edition_df.iterrows():
            _workstation = row.copy()
            _workstation['os_release_edition'] = 'Workstation'
            _workstation['cycle'] = _workstation['cycle'] + '-w'
            _row_build_list.append(_workstation)

            _enterprise = row.copy()
            _enterprise['os_release_edition'] = 'Enterprise'
            _enterprise['cycle'] = _enterprise['cycle'] + '-e'
            _row_build_list.append(_enterprise)

            _standard = row.copy()
            _standard['os_release_edition'] = 'Standard'
            _standard['cycle'] = _standard['cycle'] + '-s'
            _row_build_list.append(_standard)

        workstation_enterprise_dup_df = pd.DataFrame(_row_build_list)
        non_standard_edition_df = self.__df[self.__df['os_release_edition'] != 'Standard']
        self.__df = pd.concat([workstation_enterprise_dup_df, non_standard_edition_df], ignore_index=True)

    def duplicate_server_ltsc_rows(self):
        """
        Duplicates rows for Windows Server with LTSC edition so they also appear as Standard.
        """
        server_ltsc_edition_df = self.__df[(self.__df['os_is_lts'] == True) & (self.__df['is_server'] == True)]
        _row_build_list = []

        for index, row in server_ltsc_edition_df.iterrows():
            _ltsc = row.copy()
            _ltsc['cycle'] = _ltsc['cycle'] + '-ltsc'
            _ltsc['os_is_lts'] = True
            _row_build_list.append(_ltsc)

            _standard = row.copy()
            _standard['cycle'] = _standard['cycle']
            _standard['os_is_lts'] = False
            _row_build_list.append(_standard)

        server_ltsc_dup_df = pd.DataFrame(_row_build_list)
        reverse_filter_df = self.__df[~((self.__df['os_is_lts'] == True) & (self.__df['is_server'] == True))]
        self.__df = pd.concat([server_ltsc_dup_df, reverse_filter_df], ignore_index=True)

    def transform_drop_release_cols(self):
        """
        Drops columns that were parsed into normalized structure:
        - release_model
        - release_version
        - release_label
        """
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')
        try:
            self.__df.drop(columns=['release_model', 'release_version', 'release_label'], axis=1, inplace=True)
        except Exception:
            t = traceback.format_exc()
            print(t)
            logger.error(t)
            sys.exit(t)