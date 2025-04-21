"""
Extractor for Microsoft End-of-Life API data.

Handles:
- Secure secret injection from Vault
- API pagination for raw product data
- Normalized extraction of Windows and Windows Server EOL records

Returns structured pandas DataFrames with lifecycle metadata.
"""

import pandas as pd
import datetime as dt
import re
import requests
import json
import os
import hvac
import inspect
import traceback
import sys
from loguru import logger


class ExtractApiEndOfLifeDate:
    """
    Class to extract structured data from Microsoft EOL API endpoints.
    Injects secrets via Vault, handles HTTP calls and basic normalization.
    """

    def __init__(self, config: dict, vault) -> None:
        self.__details = config["DETAILS"]
        self.__data = config["DATA"]
        self.__timestamps = config["TIMESTAMPS"]
        self.__secrets = config["SECRETS"]

        self.__secrets.update(
            vault.read_secret(
                mount_point=config["SECRETS"]["mount_point"],
                path=config["SECRETS"]["path"]
            )
        )

    @staticmethod
    def __api_pagination(url: str = "", headers: dict = {}, params: dict = {}) -> dict:
        """
        Basic paginated GET request wrapper.
        """
        try:
            headers["Content-Type"] = "application/json"
            print(f'Request URL: {url}')
            logger.info(f"Requesting URL: {url}")

            resp = requests.get(url, headers=headers, params=params)
            content = resp.content.decode('utf-8')
            c_dict = json.loads(content)

            return {
                "data": c_dict,
                "result": {
                    "status_code": 200,
                    "task_title": inspect.currentframe().f_code.co_name,
                    "message": "Success"
                }
            }

        except Exception:
            t = traceback.format_exc()
            logger.error(t)
            return {
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 500,
                    "message": t
                }
            }

    def create_all_products_list(self) -> dict:
        """
        Fetches all product records available in the public API.
        """
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')
        try:
            request_url = f'{self.__secrets["base_uri"]}/api/all.json'
            data = self.__api_pagination(request_url)
            c_dict = data["data"]

            return {
                "data": c_dict,
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "message": "DataFrame created successfully"
                }
            }

        except Exception:
            t = traceback.format_exc()
            print(t)
            logger.error(t)
            return {
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 500,
                    "message": t
                }
            }

    def create_windows_dataframe(self):
        """
        Queries Windows OS endpoint and returns a cleaned dataframe of EOL records.
        Adds `is_server=False` marker for downstream transformations.
        """
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')

        def model(data_dict: dict = dict()) -> dict:
            try:
                model_dict = {
                    'cycle': data_dict.get('cycle'),
                    'release_label': data_dict.get('releaseLabel'),
                    'is_server': False,
                    'release_date': pd.to_datetime(data_dict.get('releaseDate', pd.NaT), format='%Y-%m-%d'),
                    'eol_date': pd.to_datetime(data_dict.get('eol', pd.NaT), format='%Y-%m-%d'),
                    'os_build': data_dict.get('latest'),
                    'link': data_dict.get('link'),
                    'os_is_lts': data_dict.get('lts')
                }
                return model_dict
            except Exception:
                t = traceback.format_exc()
                print(t)
                logger.error(t)
                sys.exit(t)

        try:
            request_url = f'{self.__secrets["base_uri"]}/api/windows.json'
            data = self.__api_pagination(request_url)
            c_dict = data["data"]

            df = pd.DataFrame([model(data) for data in c_dict])
            return {
                "data": df,
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "message": "DataFrame created successfully"
                }
            }

        except Exception:
            t = traceback.format_exc()
            print(t)
            logger.error(t)
            return {
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 500,
                    "message": t
                }
            }

    def create_windows_server_dataframe(self):
        """
        Queries Windows Server OS endpoint and returns a cleaned dataframe of EOL records.
        Adds `is_server=True` marker for downstream transformations.
        """
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')

        def model(data_dict: dict = dict()) -> dict:
            try:
                model_dict = {
                    'cycle': data_dict.get('cycle'),
                    'release_label': data_dict.get('releaseLabel'),
                    'is_server': True,
                    'release_date': pd.to_datetime(data_dict.get('releaseDate', pd.NaT), format='%Y-%m-%d'),
                    'eol_date': pd.to_datetime(data_dict.get('eol', pd.NaT), format='%Y-%m-%d'),
                    'os_build': data_dict.get('latest'),
                    'link': data_dict.get('link'),
                    'os_is_lts': data_dict.get('lts')
                }
                return model_dict
            except Exception:
                t = traceback.format_exc()
                print(t)
                logger.error(t)
                sys.exit(t)

        try:
            request_url = f'{self.__secrets["base_uri"]}/api/windows-server.json'
            data = self.__api_pagination(request_url)
            c_dict = data["data"]

            df = pd.DataFrame([model(data) for data in c_dict])
            return {
                "data": df,
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "message": "DataFrame created successfully"
                }
            }

        except Exception:
            t = traceback.format_exc()
            print(t)
            logger.error(t)
            return {
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 500,
                    "message": t
                }
            }
