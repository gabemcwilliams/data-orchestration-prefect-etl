import pandas as pd
from base64 import b64encode
import datetime as dt

import requests

import json
import os
import hvac
import traceback
import inspect
import re
import sys

import numpy as np
import re

import urllib.parse

from io import StringIO


class ExtractScalepad:

    def __init__(self, config: dict, vault, otp_gen) -> None:
        self.__details = config["DETAILS"]
        self.__data = config["DATA"]
        self.__timestamps = config["TIMESTAMPS"]
        self.__secrets = config["SECRETS"]

        self.__vault_mgr = vault
        self.__otp_gen = otp_gen

        try:
            vault_secrets = self.__vault_mgr.read_secret(
                mount_point=config["SECRETS"]["mount_point"],
                path=config["SECRETS"]["path"]
            )
        except Exception as e:
            t = traceback.format_exc()
            sys.exit(f"Failed to fetch secrets from Vault: {t}")

        # Add Secrets to Config
        self.__secrets.update(vault_secrets)

    def create_hardware_assets_dataframe(self) -> dict:
        try:
            # Base URL
            base_uri = 'https://app.scalepad.com'
            login_url = f"{self.__secrets['base_uri']}/signin"

            # Create a session to maintain cookies
            session = requests.Session()

            # Fetch the login page (initial CSRF token)
            resp = session.get(login_url)

            form_step = 0

            # Print resp
            print("Login Response:", resp)
            print(f"Form Step: {form_step}")

            # Extract cookies
            cookies = session.cookies.get_dict()

            # Regular expressions to extract FORM_FORM and FORM_STEP values
            form_form_match = re.search(r'name="FORM_FORM"\s+value="([^"]+)"', resp.text)

            # Store extracted values in variables
            form_form = form_form_match.group(1) if form_form_match else None

            # Iterate through 3 steps of username / password following gotoStep in response json then MFA
            for step in np.arange(1, 3):

                # Refresh cookies from the session before each request
                cookies = session.cookies.get_dict()

                # Decode CSRF token dynamically
                csrf_token = urllib.parse.unquote(cookies.get("WM_CSRF", ""))

                print(f"Step {step} - CSRF Token:", csrf_token)

                # Define the login payload
                payload = {
                    "form_version": "multistep-signin-v1",
                    "email": self.__secrets['username'],
                    "password": self.__secrets['password'],
                    "mfa_code": self.__otp_gen.generate_otp_from_secret(self.__secrets['totp']),
                    "FORM_FORM": form_form,
                    "FORM_STEP": step,  # Updated step dynamically
                    "nosubmit": "false"
                }

                # Headers including the latest CSRF token
                headers = {
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "X-Requested-With": "XMLHttpRequest",
                    "x-wm_csrf": csrf_token,  # Updated CSRF token dynamically
                }

                # Perform the login request with updated session cookies
                resp = session.post(login_url, data=payload, headers=headers, cookies=cookies)
                try:
                    form_step = resp.json()['gotoStep']
                except:
                    pass

                # Print resp
                print("Login Response:", resp.json())
                print(f"Form Step: {form_step}")

                # Update cookies and CSRF token after request
                cookies = session.cookies.get_dict()



            ### - Download CSV as StringIO into DataFrame - ###

            # Define the API URL
            url = f"{base_uri}/api/AssetManagement/Asset/Console/Spreadsheet"

            # Headers (copied from the curl request)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Referer": f"{self.__secrets['base_uri']}/asset/hardware?Columns=AssignedTo%2CExpires%2CHasInitiative%2CManufacturer%2CModel%2CPurchased%2CRenewalAvailable%2CType%2CUser",
                "Content-Type": "application/json",
                "Origin": f"{self.__secrets['base_uri']}",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "Priority": "u=0",
                "TE": "trailers",
            }

            # JSON Payload (modified for clarity)
            payload = {
                "Query": {
                    "Parameters": {},
                    "Pagination": {"PageNumber": 0, "Size": 50, "PageId": ""},
                    "Sort": [],
                },
                "SelectedColumns": [],
                "AllColumns": True,
                "Scope": {"Type": "Account"},
                "AssetType": "Hardware",
                "SpreadsheetType": "Csv",
            }

            # Send the POST request
            response = requests.post(url, headers=headers, cookies=cookies, json=payload)

            # Check if request was successful
            if response.status_code == 200:
                # # Save the spreadsheet as a CSV file
                # with open("d:/exports/scalepad_assets.csv", "wb") as file:
                #     file.write(response.content)
                # Convert the response content into a DataFrame
                csv_data = response.content.decode('utf-8')  # Decode bytes to string
                df = pd.read_csv(StringIO(csv_data))  # Read CSV data into a DataFrame

                print("Spreadsheet successfully downloaded as 'scalepad_assets.csv'")

                return {
                    "data": df,
                    "result": {
                        "job_title": self.__details["task_title"],
                        "status_code": 200,
                        "message": "DataFrame created successfully"
                    }
                }

            else:
                print(f"Failed to download spreadsheet. Status code: {response.status_code}")
                print("Response:", response.text)

                return {
                    "result": {
                        "job_title": self.__details["task_title"],
                        "status_code": response.status_code,
                        "message": "Failed to download spreadsheet"
                    }
                }

        except Exception as e:
            t = traceback.format_exc()
            return {
                "result": {
                    "job_title": self.__details["task_title"],
                    "status_code": 500,
                    "message": t
                }
            }
