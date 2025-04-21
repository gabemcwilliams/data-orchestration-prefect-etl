import datetime
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


class ExtractApiDattoRMM:
    """
    Extractor class for Datto RMM API that handles authentication, pagination,
    and structured DataFrame creation for various endpoints.
    """

    def __init__(self, config: dict, vault) -> None:
        """
        Initializes the extractor with configuration and Vault client.
        Retrieves secrets from Vault and sets up API token.
        """
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

        self.__access_token = self.__create_token()["access_token"]

    def __create_token(self, headers: dict = {}, data: dict = {}) -> dict:
        """
        Private method to create an access token using Datto RMM OAuth.
        Returns a dict containing the token or error details.
        """
        try:
            token_uri = f'{self.__secrets["base_uri"]}/auth/oauth/token'
            headers["Content-Type"] = "application/x-www-form-urlencoded"

            data.update({
                "grant_type": "password",
                "username": self.__secrets["api_key"],
                "password": self.__secrets["api_secret"]
            })

            resp = requests.post(token_uri, headers=headers, data=data, auth=("public-client", "public"))
            resp.raise_for_status()
            c_dict = resp.json()

            logger.info("Access token successfully created.")
            return {
                "access_token": c_dict["access_token"],
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "message": "Success"
                }
            }

        except Exception:
            t = traceback.format_exc()
            logger.error("Error while creating access token.")
            logger.debug(t)
            return {
                "access_token": "error",
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 500,
                    "message": t
                }
            }

    def __api_pagination(self, url: str = "", headers: dict = {}, params: dict = {}) -> dict:
        """
        Internal helper for paginated GET requests against Datto API.

        Args:
            url (str): The full API URL to query.
            headers (dict): Optional HTTP headers.
            params (dict): Optional query parameters.

        Returns:
            dict: Result payload and metadata.
        """
        try:
            headers["Authorization"] = f'Bearer {self.__access_token}'
            headers["Content-Type"] = "application/json"

            logger.info(f"Fetching: {url}")
            resp = requests.get(url, headers=headers, params=params)
            resp.raise_for_status()
            c_dict = resp.json()

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
            logger.error(f"API pagination error at: {url}")
            logger.debug(t)
            return {
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 500,
                    "message": t
                }
            }

    def create_account_dataframe(self) -> dict:
        """
        Extracts account-level metadata and flattens the JSON response
        into a single-row DataFrame.
        """
        logger.info(f"[START] - {inspect.currentframe().f_code.co_name}")

        def model(data_dict: dict = {}) -> dict:
            try:
                model_dict = {
                    'id': data_dict.get('id'),
                    'name': data_dict.get('name'),
                    'billing_email': data_dict.get('descriptor', {}).get('bilingEmail'),
                    'device_limit': data_dict.get('descriptor', {}).get('deviceLimit'),
                    'time_zone': data_dict.get('descriptor', {}).get('timeZone'),
                    'uid': data_dict.get('uid'),
                    'currency': data_dict.get('currency'),
                    'number_of_devices': data_dict.get('devicesStatus', {}).get('numberOfDevices'),
                    'number_of_online_devices': data_dict.get('devicesStatus', {}).get('numberOfOnlineDevices'),
                    'number_of_offline_devices': data_dict.get('devicesStatus', {}).get('numberOfOfflineDevices'),
                    'number_of_on_demand_devices': data_dict.get('devicesStatus', {}).get('numberOfOnDemandDevices'),
                    'number_of_managed_devices': data_dict.get('devicesStatus', {}).get('numberOfManagedDevices')
                }
                return model_dict
            except Exception:
                t = traceback.format_exc()
                logger.error("Model parse error in create_account_dataframe")
                logger.debug(t)
                sys.exit(t)

        try:
            request_url = f'{self.__secrets["base_uri"]}/api/v2/account'
            data = self.__api_pagination(request_url)
            c_dict = data["data"]
            df = pd.DataFrame([model(c_dict)])

            logger.info(f"Created account dataframe with shape {df.shape}")
            return {
                "data": df,
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "message": "Success"
                }
            }

        except Exception:
            t = traceback.format_exc()
            logger.error("Failed to fetch or model account data")
            logger.debug(t)
            return {
                "result": {
                    "status_code": 500,
                    "message": t
                }
            }

    def create_account_sites_dataframe(self) -> dict:
        """
        Retrieves all sites under the account and returns them as a DataFrame.
        Handles paginated responses.
        """
        logger.info(f"[START] - {inspect.currentframe().f_code.co_name}")

        def model(data_dict: dict = {}) -> dict:
            try:
                proxy = data_dict.get('proxySettings') or {}
                devices = data_dict.get('devicesStatus') or {}

                model_dict = {
                    'id': data_dict.get('id'),
                    'uid': data_dict.get('uid'),
                    'account_uid': data_dict.get('accountUid'),
                    'name': data_dict.get('name'),
                    'description': data_dict.get('description'),
                    'notes': data_dict.get('notes'),
                    'on_demand': data_dict.get('onDemand'),
                    'splashtop_auto_install': data_dict.get('splashtopAutoInstall'),
                    'proxySettingsHost': proxy.get('host'),
                    'proxySettingsPassword': '*****' if proxy.get('password') else None,
                    'proxy_settings_type': proxy.get('type'),
                    'proxy_settings_port': proxy.get('port'),
                    'proxy_settings_username': proxy.get('username'),
                    'num_of_total_devices': devices.get('numberOfDevices'),
                    'number_of_online_devices': devices.get('numberOfOnlineDevices'),
                    'number_of_online_devices': devices.get('numberOfOfflineDevices'),
                    'autotask_company_nameame': data_dict.get('autotaskCompanyName'),
                    'autotask_company_id': data_dict.get('autotaskCompanyId'),
                    'portal_url': data_dict.get('portalUrl')
                }
                return model_dict
            except Exception:
                t = traceback.format_exc()
                logger.error("Model parse error in create_account_sites_dataframe")
                logger.debug(t)
                sys.exit(t)

        try:
            request_url = f'{self.__secrets["base_uri"]}/api/v2/account/sites'
            data = self.__api_pagination(request_url)
            c_dict = data["data"]
            next_page = c_dict.get('pageDetails', {}).get("nextPageUrl")

            df = pd.DataFrame([model(site) for site in c_dict.get("sites", [])])

            while next_page:
                data = self.__api_pagination(next_page)
                c_dict = data["data"]
                next_page = c_dict.get('pageDetails', {}).get("nextPageUrl")
                df_current = pd.DataFrame([model(site) for site in c_dict.get("sites", [])])
                df = pd.concat([df, df_current], ignore_index=True)

            logger.info(f"Created sites dataframe with shape {df.shape}")
            return {
                "data": df,
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "message": "Success"
                }
            }

        except Exception:
            t = traceback.format_exc()
            logger.error("Failed to fetch or model sites data")
            logger.debug(t)
            return {
                "result": {
                    "status_code": 500,
                    "message": t
                }
            }

    def create_account_alerts_open_dataframe(self) -> dict:
        """
        Retrieves open alerts from the Datto RMM API and returns them as a DataFrame.
        Handles pagination and flattens alert-related fields.
        """
        logger.info(f"[START] - {inspect.currentframe().f_code.co_name}")

        def model(data_dict: dict = {}) -> dict:
            try:
                alert_info = {
                    'alert_uid': data_dict.get('alertUid'),
                    'priority': data_dict.get('priority'),
                    'diagnostics': data_dict.get('diagnostics'),
                    'resolved': data_dict.get('resolved'),
                    'resolved_by': data_dict.get('resolvedBy'),
                    'resolved_on': pd.to_datetime(data_dict.get('resolved_on', pd.NaT), unit='ms', errors='coerce'),
                    'muted': data_dict.get('muted'),
                    'ticket_number': data_dict.get('ticketNumber'),
                    'timestamp': data_dict.get('timestamp', pd.NaT),
                    'alert_context': data_dict.get('alertContext'),
                    'response_actions': data_dict.get('responseActions'),
                    'auto_resolve_mins': data_dict.get('autoresolveMins')
                }

                source_info = data_dict.get('alertSourceInfo', {})
                monitor_info = data_dict.get('alert_monitor_info', {})

                alert_info.update({
                    'device_uid': source_info.get('deviceUid'),
                    'hostname': source_info.get('deviceName', '').upper() if isinstance(source_info.get('deviceName'),
                                                                                        str) else None,
                    'site_uid': source_info.get('siteUid'),
                    'site_name': source_info.get('siteName'),
                    'creates_ticket': monitor_info.get('createsTicket'),
                    'sends_emails': monitor_info.get('sendsEmails')
                })
                return alert_info
            except Exception:
                logger.exception("Model parse error in open alert")
                sys.exit(1)

        try:
            request_url = f'{self.__secrets["base_uri"]}/api/v2/account/alerts/open'
            data = self.__api_pagination(request_url)
            c_dict = data["data"]
            next_page = c_dict.get('pageDetails', {}).get("nextPageUrl")

            df = pd.DataFrame([model(d) for d in c_dict.get("alerts", [])])
            logger.info(f"Initial open alerts retrieved: {df.shape[0]} rows")

            while next_page:
                logger.info(f"Fetching next page: {next_page}")
                data = self.__api_pagination(next_page)
                c_dict = data["data"]
                next_page = c_dict.get('pageDetails', {}).get("nextPageUrl")
                df_next = pd.DataFrame([model(d) for d in c_dict.get("alerts", [])])
                df = pd.concat([df, df_next], ignore_index=True)

            logger.info(f"Final open alerts dataframe shape: {df.shape}")
            return {
                "data": df,
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "message": "Success"
                }
            }

        except Exception:
            t = traceback.format_exc()
            logger.error("Failed to fetch or model open alerts")
            logger.debug(t)
            return {
                "result": {
                    "status_code": 500,
                    "message": t
                }
            }

    def create_account_alerts_resolved_dataframe(self, monitor_history_age=7) -> dict:
        """
        Retrieves resolved alerts from Datto RMM API, paginated until time window delta is satisfied.

        Args:
            monitor_history_age (int): Lookback window in days for resolved alert history.

        Returns:
            dict: DataFrame and status result
        """
        logger.info(f"[START] - {inspect.currentframe().f_code.co_name}")

        def model(data_dict: dict = {}) -> dict:
            try:
                alert_info = {
                    'alert_uid': data_dict.get('alertUid'),
                    'priority': data_dict.get('priority'),
                    'diagnostics': data_dict.get('diagnostics'),
                    'resolved': data_dict.get('resolved'),
                    'resolved_by': data_dict.get('resolvedBy'),
                    'resolved_on': pd.to_datetime(data_dict.get('resolvedOn', pd.NaT), unit='ms', errors='coerce'),
                    'muted': data_dict.get('muted'),
                    'ticket_number': data_dict.get('ticketNumber'),
                    'timestamp': data_dict.get('timestamp', pd.NaT),
                    'alert_context': data_dict.get('alertContext'),
                    'response_actions': data_dict.get('responseActions'),
                    'autoresolve_mins': data_dict.get('autoresolveMins')
                }

                source_info = data_dict.get('alertSourceInfo', {})
                monitor_info = data_dict.get('alertMonitorInfo', {})

                alert_info.update({
                    'device_uid': source_info.get('deviceUid'),
                    'hostname': source_info.get('deviceName', '').upper() if isinstance(source_info.get('deviceName'),
                                                                                        str) else None,
                    'site_uid': source_info.get('siteUid'),
                    'site_name': source_info.get('siteName'),
                    'creates_ticket': monitor_info.get('createsTicket'),
                    'sends_emails': monitor_info.get('sendsEmails')
                })

                return alert_info
            except Exception:
                logger.exception("Model parse error in resolved alert")
                sys.exit(1)

        try:
            delta = 1 - monitor_history_age
            request_url = f'{self.__secrets["base_uri"]}/api/v2/account/alerts/resolved'
            data = self.__api_pagination(request_url)
            c_dict = data["data"]

            next_page = c_dict.get('pageDetails', {}).get("nextPageUrl")
            df = pd.DataFrame([model(d) for d in c_dict.get("alerts", [])])

            while next_page:
                logger.info(f"Fetching page: {next_page}")
                data = self.__api_pagination(next_page)
                c_dict = data["data"]
                next_page = c_dict.get('pageDetails', {}).get("nextPageUrl")

                df_next = pd.DataFrame([model(d) for d in c_dict.get("alerts", [])])
                df_next['timestamp'] = pd.to_datetime(df_next['timestamp'], unit='ms', errors='coerce')

                end_date = df_next["timestamp"].max()
                time_delta = dt.datetime.strptime(end_date.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S") - \
                             dt.datetime.strptime(self.__timestamps["_IN_DATA_TIMESTAMP"], "%Y-%m-%d %H:%M:%S")

                if time_delta.days <= delta:
                    logger.info("Resolved alert delta window reached.")
                    break

                df = pd.concat([df, df_next], ignore_index=True)

            logger.info(f"Final resolved alerts dataframe shape: {df.shape}")
            return {
                "data": df,
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "message": "Success"
                }
            }

        except Exception:
            t = traceback.format_exc()
            logger.error("Failed to fetch or model resolved alerts")
            logger.debug(t)
            return {
                "result": {
                    "status_code": 500,
                    "message": t
                }
            }

    def create_account_variables_dataframe(self) -> dict:
        """
        Extracts account-level custom variables from Datto RMM.
        Attaches site UID and name for cross-reference.

        Returns:
            dict: DataFrame and result metadata
        """
        logger.info(f"[START] - {inspect.currentframe().f_code.co_name}")

        def model(data_dict: dict = {}, account_dict: dict = {}) -> dict:
            try:
                return {
                    'id': data_dict.get('id'),
                    'name': data_dict.get('name'),
                    'value': data_dict.get('value'),
                    'masked': data_dict.get('masked'),
                    'site_uid': account_dict.get('uid'),
                    'site_name': account_dict.get('name')
                }
            except Exception:
                logger.exception("Model parse error in create_account_variables_dataframe")
                raise

        try:
            df_account = self.create_account_dataframe()["data"]
            account = df_account.to_dict(orient='records')[0]

            request_url = f'{self.__secrets["base_uri"]}/api/v2/account/variables'
            data = self.__api_pagination(request_url)
            c_dict = data["data"]

            next_page = c_dict.get('pageDetails', {}).get("nextPageUrl")
            df = pd.DataFrame([model(row, account) for row in c_dict.get("variables", [])])

            while next_page:
                logger.info(f"Fetching next page: {next_page}")
                data = self.__api_pagination(next_page)
                c_dict = data["data"]
                df_page = pd.DataFrame([model(row, account) for row in c_dict.get("variables", [])])
                df = pd.concat([df, df_page], ignore_index=True)
                next_page = c_dict.get('pageDetails', {}).get("nextPageUrl")

            logger.info(f"Created account variables dataframe with shape: {df.shape}")
            return {
                "data": df,
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "message": "Success"
                }
            }

        except Exception:
            t = traceback.format_exc()
            logger.error("Failed to extract account variables")
            logger.debug(t)
            return {
                "result": {
                    "status_code": 500,
                    "message": t
                }
            }

    def create_activity_logs_dataframe(self,
                                       size: int = 250,
                                       order: str = "desc",
                                       from_dt: dt = None,
                                       until_dt: dt = None,
                                       entities: list = None,
                                       categories: list = None,
                                       actions: list = None,
                                       site_ids: list = None,
                                       user_ids: list = None) -> dict:
        """
        Fetches activity logs from the Datto RMM API and returns them as a DataFrame.
        Supports optional filtering and pagination.

        Args:
            size (int): Number of records per request page.
            order (str): 'asc' or 'desc'.
            from_dt (datetime): Start datetime filter.
            until_dt (datetime): End datetime filter.
            entities (list): Filter by entity type.
            categories (list): Filter by category.
            actions (list): Filter by action type.
            site_ids (list): Filter by site ID.
            user_ids (list): Filter by user ID.

        Returns:
            dict: Pandas DataFrame and status metadata
        """
        logger.info(f"[START] - {inspect.currentframe().f_code.co_name}")

        def model(data_dict: dict = {}) -> dict:
            try:
                site = data_dict.get('site') or {}
                return {
                    'id': data_dict.get('id'),
                    'entity': data_dict.get('entity', '').lower() if data_dict.get('entity') else None,
                    'category': data_dict.get('category'),
                    'action': data_dict.get('action'),
                    'date': pd.to_datetime(data_dict.get('date'), unit='s', errors='coerce'),
                    'site_id': site.get('id'),
                    'site_name': site.get('name'),
                    'device_id': data_dict.get('deviceId'),
                    'hostname': data_dict.get('hostname', '').upper() if data_dict.get('hostname') else None,
                    'user': data_dict.get('user'),
                    'details': data_dict.get('details', {}),
                    'has_std_out': data_dict.get('hasStdOut'),
                    'has_std_err': data_dict.get('hasStdErr')
                }
            except Exception:
                logger.exception("Failed to model activity log row")
                raise

        try:
            params = {
                "size": size,
                "order": order,
                "from": from_dt,
                "until": until_dt,
                "entities": entities,
                "categories": categories,
                "actions": actions,
                "site_ids": site_ids,
                "user_ids": user_ids
            }

            request_url = f'{self.__secrets["base_uri"]}/api/v2/activity-logs'
            data = self.__api_pagination(request_url, params=params)
            c_dict = data["data"]

            df = pd.DataFrame([model(entry) for entry in c_dict.get("activities", [])])
            next_page = c_dict.get('pageDetails', {}).get('nextPageUrl')

            while next_page:
                logger.info(f"Fetching next page: {next_page}")
                data = self.__api_pagination(next_page)
                c_dict = data["data"]
                df_page = pd.DataFrame([model(entry) for entry in c_dict.get("activities", [])])
                df = pd.concat([df, df_page], ignore_index=True)
                next_page = c_dict.get('pageDetails', {}).get('nextPageUrl')

            logger.info(f"Final activity logs dataframe shape: {df.shape}")
            return {
                "data": df,
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "message": "Success"
                }
            }

        except Exception:
            t = traceback.format_exc()
            logger.error("Failed to retrieve or parse activity logs")
            logger.debug(t)
            return {
                "result": {
                    "status_code": 500,
                    "message": t
                }
            }

    def create_devices_dataframe(self) -> dict:
        """
        Extracts all device metadata from Datto RMM, including nested structures
        like UDFs, antivirus, and patch management fields. Handles pagination.

        Returns:
            dict: DataFrame and status result
        """

        logger.info(f"[START] - {inspect.currentframe().f_code.co_name}")

        def model(data_dict: dict = {}) -> dict:

            try:
                model_dict = {
                    'id': data_dict.get('id'),
                    'uid': data_dict.get('uid'),
                    'site_id': data_dict.get('siteId'),
                    'site_uid': data_dict.get('siteUid'),
                    'site_name': data_dict.get('siteName'),
                    'hostname': data_dict.get('hostname', '').upper() if isinstance(data_dict.get('hostname'),
                                                                                    str) else None,
                    'int_ip_address': data_dict.get('intIpAddress'),
                    'ext_ip_address': data_dict.get('extIpAddress'),
                    'operating_system': data_dict.get('operatingSystem'),
                    'last_logged_in_user': data_dict.get('lastLoggedInUser'),
                    'domain': data_dict.get('domain'),
                    'cag_version': data_dict.get('cagVersion'),
                    'display_version': data_dict.get('displayVersion'),
                    'description': data_dict.get('description'),
                    'a_64_bit': data_dict.get('a64Bit'),
                    'reboot_required': data_dict.get('rebootRequired'),
                    'online': data_dict.get('online'),
                    'suspended': data_dict.get('suspended'),
                    'deleted': data_dict.get('deleted'),
                    'last_seen': pd.to_datetime(data_dict.get('lastSeen', pd.NaT), unit='ms', errors='coerce'),
                    'last_reboot': pd.to_datetime(data_dict.get('lastReboot', pd.NaT), unit='ms', errors='coerce'),
                    'last_audit_date': pd.to_datetime(data_dict.get('lastAuditDate', pd.NaT), unit='ms', errors='coerce'),
                    'creation_date': pd.to_datetime(data_dict.get('creationDate', pd.NaT), unit='ms', errors='coerce'),
                    'portal_url': data_dict.get('portalUrl'),
                    'device_class': data_dict.get('deviceClass'),
                    'snmp_enabled': data_dict.get('snmpEnabled'),
                    'software_status': data_dict.get('softwareStatus'),
                    'web_remote_url': data_dict.get('webRemoteUrl'),
                    'warranty_date': data_dict.get('warrantyDate'),
                }

                # deviceType subfields
                device_type = data_dict.get('deviceType') or {}
                model_dict['category'] = device_type.get('category')
                model_dict['type'] = device_type.get('type')
                model_dict['is_server'] = 'server' in str(device_type.get('category', '')).lower()

                # antivirus
                antivirus = data_dict.get('antivirus') or {}
                model_dict['antivirus_product'] = antivirus.get('antivirusProduct')
                model_dict['antivirus_status'] = (
                    re.sub(r'(?<!^)(?=[A-Z])', ' ', antivirus.get('antivirusStatus', '')).strip()
                    if antivirus.get('antivirusStatus') else None
                )

                # patchManagement
                patching = data_dict.get('patchManagement') or {}
                model_dict['patch_status'] = (
                    re.sub(r'(?<!^)(?=[A-Z])', ' ', patching.get('patchStatus', '')).strip()
                    if patching.get('patchStatus') else None
                )
                model_dict['patches_approved_pending'] = patching.get('patchesApprovedPending')
                model_dict['patches_not_approved'] = patching.get('patchesNotApproved')
                model_dict['patches_installed'] = patching.get('patchesInstalled')

                # Adjust last seen if online
                if model_dict['online']:
                    model_dict['adjusted_last_seen'] = pd.to_datetime(
                        dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                        format='%Y-%m-%d %H:%M:%S', errors='coerce')
                else:
                    model_dict['adjusted_last_seen'] = model_dict['last_seen']

                # udf values
                udf = data_dict.get('udf') or {}
                for i in range(1, 31):
                    model_dict[f'udf{i}'] = udf.get(f'udf{i}')
                model_dict['local_timezone'] = udf.get('udf10')  # Specific label

                return model_dict

            except Exception:
                logger.exception("Model parse error in create_devices_dataframe")
                sys.exit(1)

        try:
            request_url = f'{self.__secrets["base_uri"]}/api/v2/account/devices'
            data = self.__api_pagination(request_url)
            c_dict = data["data"]
            next_page = c_dict.get('pageDetails', {}).get("nextPageUrl")

            df = pd.DataFrame([model(row) for row in c_dict.get("devices", [])])

            while next_page:
                logger.info(f"Fetching device page: {next_page}")
                data = self.__api_pagination(next_page)
                c_dict = data["data"]
                df_next = pd.DataFrame([model(row) for row in c_dict.get("devices", [])])
                df = pd.concat([df, df_next], ignore_index=True)
                next_page = c_dict.get('pageDetails', {}).get("nextPageUrl")

            logger.info(f"Created devices dataframe with shape {df.shape}")
            return {
                "data": df,
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "message": "Success"
                }
            }

        except Exception:
            t = traceback.format_exc()
            logger.error("Failed to fetch or model device data")
            logger.debug(t)
            return {
                "result": {
                    "status_code": 500,
                    "message": t
                }
            }

    def create_site_variables_dataframe(self) -> dict:
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')

        def model(data_dict: dict = {}, site_dict: dict = {}) -> dict:
            try:
                # init model dict
                model_dict = {}

                model_dict['id'] = data_dict.get('id', None)  # int | None
                model_dict['name'] = data_dict.get('name', None)  # str | None
                model_dict['value'] = data_dict.get('value', None)  # str | None
                model_dict['masked'] = data_dict.get('masked', None)  # bool | None

                # Add in UID and Site used as does not exist in Account api result, so can be concat to site variables df
                model_dict['site_uid'] = site_dict.get('uid', "[ACCOUNT]")  # int | None
                model_dict['site_name'] = site_dict.get('site_name', "[ACCOUNT]")  # str | None

                return model_dict
            except Exception as e:
                t = traceback.format_exc()
                sys.exit(t)

        try:

            # Get Site ID's
            df_sites = self.create_account_sites_dataframe()["data"]

            sites_info_list = []

            for index, row in df_sites.iterrows():
                sites_info_list.append({
                    "uid": row["uid"],
                    "site_name": row["name"]
                })

            # Initialize df_site_variables_combined if doesn't exist
            df_site_variables_combined = pd.DataFrame()

            # Create Site Variables Dataframe
            for site in sites_info_list:
                try:
                    print(site)
                    request_url = f'{self.__secrets["base_uri"]}/api/v2/site/{site["uid"]}/variables'
                    data = self.__api_pagination(request_url)

                    c_dict = data["data"]

                    next_page = c_dict.get('pageDetails', None)["nextPageUrl"] \
                        if c_dict.get('pageDetails', None) else print('No more pages')

                    # iterate and combine remaining pages
                    df = pd.DataFrame([model(data, site) for data in c_dict["variables"]])
                    while next_page:
                        data = self.__api_pagination(next_page)
                        c_dict = data["data"]

                        next_page = c_dict.get('pageDetails', None)["nextPageUrl"] \
                            if c_dict.get('pageDetails', None) else print('No more pages')

                        df_current_page = pd.DataFrame([model(data, site) for data in c_dict["variables"]])
                        df = pd.concat([df, df_current_page], ignore_index=False)

                    # Create dataframe if empty from init
                    try:
                        if df_site_variables_combined.empty:
                            print(f"df_site_variables_combined is {df_site_variables_combined.empty}")
                            df_site_variables_combined = df

                        else:
                            print(f"df_site_variables_combined shape: {df_site_variables_combined.shape}")
                            df_site_variables_combined = pd.concat([df_site_variables_combined, df], ignore_index=False)

                    except Exception as e:
                        print(e)

                        continue
                except Exception as e:
                    print(e)
                    continue

            return {
                "data": df_site_variables_combined,
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "message": "Success",
                }
            }

        except Exception as e:
            t = traceback.format_exc()
            return {
                "result":
                    {
                        "status_code": 500,
                        "message": t,
                    }
            }
