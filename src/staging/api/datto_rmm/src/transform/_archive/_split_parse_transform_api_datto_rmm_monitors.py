########################################################
#
#     Title: [TRANSFORM] - DattoRMM [API] - Normalize - Monitors
#     Created by: Gabe McWilliams
#     Date: 2023/04/19
#
########################################################


import pandas as pd
import traceback
import datetime as dt

# alertContext

import re

from pandas.tests.test_downstream import df


# Parser - Main entry point


class NormalizeMonitors:
    def __init__(self, config: dict, vault) -> None:
        self.__details = config["DETAILS"]
        self.__data = config["DATA"]
        self.__timestamps = config["TIMESTAMPS"]
        self.__non_nested_cols = NonNestedCols()
        self.__alert_monitor_info = AlertMonitorInfo()
        self.__alert_context = AlertContext()
        self.__alert_status = AlertStatus()
        self.__alert_source_info = AlertSourceInfo()
        self.__response_actions = ResponseActions()
        self.__diagnostics = Diagnostics()

    def transform_monitors(self, df) -> dict:
        non_nested_cols = [
            "alertUid", "priority", "resolved",
            "resolvedBy", "resolvedOn", "muted",
            "ticketNumber", "timestamp", "autoresolveMins"
        ]

        try:
            df_series_list = [self.__non_nested_cols.parse_non_nested_cols(df[non_nested_cols])["data"],
                              self.__alert_monitor_info.parse_alert_monitor_info(df[["alertUid", "alertMonitorInfo"]])[
                                  "data"],
                              self.__alert_context.parse_alert_context(df[["alertUid", "alertContext"]])["data"],
                              self.__alert_status.parse_alert_status(df[["alertUid", "alertContext"]])["data"],
                              self.__alert_source_info.parse_alert_source_info(df[["alertUid", "alertSourceInfo"]])[
                                  "data"],
                              self.__response_actions.parse_response_actions(df[["alertUid", "responseActions"]])[
                                  "data"],
                              self.__diagnostics.parse_diagnostics(df[["alertUid", "diagnostics"]])["data"]]

            return {
                "data": df_series_list,
                "result": {
                    "job_title": self.__details["task_title"],
                    "status_code": 200,
                    "message": "Success",
                }
            }


        except Exception as e:
            return {
                "result":
                    {
                        "job_title": self.__details["task_title"],
                        "status_code": 500,
                        "message": t
                    }
            }


#######################################################################################################################


class NonNestedCols:
    def __init__(self):
        pass

    def parse_non_nested_cols(self, df):

        df.fillna({"resolvedBy": "UKNOWN", "resolvedOn": pd.NaT, "timestamp": pd.NaT, "autoresolveMins": 0},
                  inplace=True)

        try:

            def resolved_on(resolved_on):
                r = resolved_on

                try:
                    date = pd.to_datetime(int(resolved_on), unit="s", errors="coerce")
                    date = date.strftime(format="%Y-%m-%d %H:%M:%S")

                    return date
                #
                except Exception as e:
                    #     print(resolved_on)
                    #     print(type(resolved_on))
                    #     print(e)
                    return pd.NaT
                # return pd.to_datetime(int(string), utc=True, unit='ms')

            df.loc[:, "resolvedOn"] = df["resolvedOn"].apply(lambda x: resolved_on(x))
            df.loc[:, "timestamp"] = df["timestamp"].apply(lambda x: pd.to_datetime(x, unit="s", errors="coerce"))

            return {
                "data": df,
                "result": {
                    "status_code": 200,
                    "message": "Success",
                    "function": "parse_non_nested_cols"

                }

            }

        except Exception as e:
            return {
                "result": {
                    "status_code": 500,
                    "message": t,
                    "function": "parse_non_nested_cols"
                }
            }


class AlertContext:

    def __init__(self):
        pass

    # TODO: review nested contents and crete AlertStatus Column will filling in missing values
    # def __split_alert_context__(self):
    #
    #     df_alert_status = df["alert_context"].apply(lambda x: x["status"])
    #
    # # TODO: review what this function does!
    #
    #     for i, row in df.iterrows():
    #         if row['response_actions'] is not None:
    #             for j, action in enumerate(row['response_actions']):
    #                 for k, v in action.items():
    #                     if k in ['actionReference', 'actionReferenceInt']:
    #                         pass
    #                     elif k == 'actionTime':
    #                         v = pd.to_datetime(action[k], unit='ms')
    #                         df.loc[i, k + str(j)] = v
    #                     else:
    #                         df.loc[i, k + str(j)] = action[k]
    #
    #     df.drop('response_actions', axis=1, inplace=True)
    #
    #     df['alertStatus'].apply(lambda x: split_alert_status(str(x)))
    #     df.drop('alertStatus', axis=1, inplace=True)

    def parse_alert_context(self, df):
        try:

            df = df.apply(lambda x: x.replace(str(x)))

            # alert_context
            df["alertClass"] = df["alertContext"].apply(lambda x: x["@class"])

            for index, row in df.iterrows():
                for k, v in row['alertContext'].items():
                    if k == '@class':
                        df.loc[index, 'alertContextClass'] = v
                    else:
                        df.loc[index, f'alertContext{k[0].capitalize()}{k[1:]}'] = v

            df['alertContextLastTriggered'] = pd.to_datetime(df['alertContextLastTriggered'], unit='ms')

            df.drop('alertContext', axis=1, inplace=True)

            return {
                "data": df,
                "status_code": 200,
                "function": "parse_alert_context",
                "message": "Success"
            }

        except Exception as e:
            return {
                "status_code": 500,
                "function": "parse_alert_context",
                "message": t
            }

    def __class_context__(self, alert_context):
        try:
            if alert_context["@class"] == 'comp_script_ctx':

                return alert_context["samples"]



            elif alert_context["@class"] == 'perf_disk_usage_ctx':
                disk_alert_prog = re.compile(
                    r'(\bdiskName\b)[^A-Z]+([A-Z])[^a-z]+(\btotalVolume\b)[^\d.]+([\d.]*)[^a-z]+(\bfreeSpace\b)[^\d.]+([\d.]*)')
                disk_alert_parse = disk_alert_prog.findall(str(alert_context))
                disk_info_dict = {'diskName': disk_alert_parse[0][1], 'totalVolume': disk_alert_parse[0][3],
                                  'freeSpace': disk_alert_parse[0][5], 'percentRemaining': round(
                        (float(disk_alert_parse[0][5]) / float(disk_alert_parse[0][3])) * 100, 2)}

                return disk_info_dict

            elif alert_context["@class"] == 'perf_resource_usage_ctx':
                cpu_alert_prog = re.compile(r'(\bpercentage\b)[^\d.]+([\d.]+)[^a-z]+(\btype\b)\W+([A-Z]+)')
                cpu_alert_parse = cpu_alert_prog.findall(str(alert_context))
                cpu_info_dict = {'percentage': cpu_alert_parse[0][1], 'type': cpu_alert_parse[0][1]}

                return cpu_info_dict

            elif alert_context["@class"] == 'online_offline_status_ctx':
                return "{'status':'OFFLINE'}"

            elif alert_context["@class"] == 'srvc_status_ctx':
                service_info_dict = {'serviceName': alert_context["serviceName"], 'status': alert_context["status"]}

                return service_info_dict

            # TODO: Are there two types of perf_resource_usage_ctx?
            # elif alert_context["@class"] == 'perf_resource_usage_ctx'

            else:
                return None

        except Exception as e:
            return {
                "status_code": 500,
                "function": "parse_alert_context",
                "message": t
            }


# alertStatus


class AlertStatus:

    def __init__(self):
        self.__parse_functions_list = [
            self.__parse_offline_monitor__,
            self.__parse_disk_usage_monitor__,
            self.__parse_cpu_monitor__,
            self.__parse_sophos_central_endpoint_monitor__,
            self.__parse_service_monitor__,
            self.__parse_backup_monitor__,
            self.__parse_firewall_disabled_monitor__,
            self.__parse_web_remote_monitor__,
            self.__parse_competitor_monitor__,
            self.__parse_opendns_monitor__,
            self.__parse_wsus_monitor__
        ]

        ## Combine all Functions to be use on Any Column

    def parse_alert_status(self, df):
        try:

            df["monitorSource"] = df["alertContext"].apply(self.__apply_parse_functions__)

            return {
                "data": df,
                "status_code": 200,
                "function": "parse_alert_status",
                "message": "Success"
            }

        except Exception as e:
            return {
                "status_code": 500,
                "function": "parse_alert_status",
                "message": t
            }

    def __apply_parse_functions__(self, alert_status):
        i = 1
        for func in self.__parse_functions_list:
            try:
                result = func(alert_status)
                i = i + 1
                if result:
                    return result

            except Exception as e:
                print(e)
                break

    ## Parse Ticket Category Source
    @staticmethod
    def __parse_offline_monitor__(alert_status: str) -> str:
        offline_prog = re.compile(r"\{\'status':\'OFFLINE\'}")
        exists = offline_prog.findall(str(alert_status))
        if exists:
            try:
                return 'Offline Alert'
            except Exception as e:
                pass

    @staticmethod
    def __parse_disk_usage_monitor__(alert_status: str) -> str:
        disk_usage_prog = re.compile(r"\{\'diskName':\s\'\w+\',")
        exists = disk_usage_prog.findall(str(alert_status))
        if exists:
            try:
                return 'Disk Usage High'
            except Exception as e:
                pass

    @staticmethod
    def __parse_cpu_monitor__(alert_status: str) -> str:
        cpu_prog = re.compile(r"\{(\'percentage\':\s\'[\d.]+)")
        exists = cpu_prog.findall(str(alert_status))
        if exists:
            try:
                return 'CPU High'
            except Exception as e:
                pass

    @staticmethod
    def __parse_backup_monitor__(alert_status: str) -> str:
        backup_prog = re.compile(r"\{\'Status':\s\'Manual Maintenance\'}")
        exists = backup_prog.findall(str(alert_status))
        if exists:
            try:
                return 'Backup Failed'
            except Exception as e:
                pass

    @staticmethod
    def __parse_firewall_disabled_monitor__(alert_status: str) -> str:
        domain_zone_prog = re.compile(r"{'STATUS': 'Domain Zone DISABLED!'}")
        profile_prog = re.compile(r"\{\'alert\':\s\'(private\s|public\s|domain\s)+profile\sis\sdisabled'}")
        firewall_prog_list = [
            domain_zone_prog,
            profile_prog
        ]
        for prog in firewall_prog_list:
            exists = prog.findall(str(alert_status))
            if exists:
                try:
                    return 'Windows Firewall Disabled'
                except Exception as e:
                    pass

    @staticmethod
    def __parse_sophos_central_endpoint_monitor__(alert_status: str) -> str:
        sophos_central_endpoint_prog = re.compile(r'(\bSophos\b)')
        exists = sophos_central_endpoint_prog.findall(str(alert_status))
        if exists:
            try:
                return 'Sophos Automation Failed'
            except Exception as e:
                pass

    @staticmethod
    def __parse_service_monitor__(alert_status: str) -> str:
        service_prog = re.compile(r"\'([^,\"]+)\s?\w?\'?,?\s\'?status\':\s\'stopped\'}")
        exists = service_prog.findall(str(alert_status).lower())

        if exists:
            try:
                service = (service_prog.findall(str(alert_status).lower()))[0].replace('service', "").replace(' ',
                                                                                                              "")
                return f'{service.capitalize()} Stopped'
            except Exception as e:
                pass

    @staticmethod
    def __parse_web_remote_monitor__(alert_status: str) -> str:
        web_remote_prog = re.compile(r"\{\'status\':\s\'[\w\s.]+webremote[.\w\s!]+\'}")
        exists = web_remote_prog.findall(str(alert_status).lower())
        if exists:
            try:
                return 'DattoRMM WebRemote Error'
            except Exception as e:
                pass

    @staticmethod
    def __parse_wsus_monitor__(alert_status: str) -> str:
        wsus_prog = re.compile(r'\{\'status\':\s\'.*wsus.*\'}')
        exists = wsus_prog.findall(str(alert_status).lower())
        if exists:
            try:
                return 'WSUS Misconfigured'
            except Exception as e:
                pass

    @staticmethod
    def __parse_opendns_monitor__(alert_status: str) -> str:
        opendns_prog = re.compile(r'\{\'alert\':\s\'.*opendns.*\'}')
        exists = opendns_prog.findall(str(alert_status).lower())
        if exists:
            try:
                return 'OpenDNS Automation Failed'
            except Exception as e:
                pass

    @staticmethod
    def __parse_competitor_monitor__(alert_status: str) -> str:
        rmm_prog = re.compile(r'.*\brmm\b.*')
        exists = rmm_prog.findall(
            str(alert_status).replace("{'Status': 'Atera Agent is not running.'}", "Atera RMM").replace(
                "{'Alert': 'Unsupported OS. Requires Server 2012 and up or Windows 8.1 and up.'}",
                "ConnectWise RMM").replace("{'Alert': 'Healthy'}", "ConnectWise RMM").lower())
        if exists:
            try:
                return 'Competitor RMM Found'
            except Exception as e:
                pass

    @staticmethod
    def __split_alert_status__(string):
        # print(string)

        result = re.findall(r"[\'\"]([\w\.\s\-,]+)[\'\"]", string)
        status_list = []
        for e in result:
            if e not in ['status', 'alert']:
                status_list.append(e.lower().lstrip().rstrip())
        status = " - ".join(status_list)
        return status


# alertSourceInfo

class AlertSourceInfo:
    def __init__(self):
        pass

    def parse_alert_source_info(self, df):
        try:
            for index, row in df.iterrows():
                try:
                    alert_source_info = row["alertSourceInfo"]
                    for k, v in alert_source_info.items():
                        df.loc[index, k] = v
                except Exception as e:
                    pass
            return {
                "status_code": 200,
                "function": "parse_alert_source_info",
                "data": df
            }

        except Exception as e:
            return {
                "status_code": 500,
                "function": "parse_alert_source_info",
                "message": t
            }


class Diagnostics:

    def __init__(self):
        pass

    def parse_diagnostics(self, df):
        try:
            df["diagnostics"] = df["diagnostics"].apply(self.__truncate_diag__)

            return {
                "data": df,
                "function": "parse_diagnostics",
                "status_code": 200,
                "status": "success"

            }

        except Exception as e:
            return {
                "function": "parse_diagnostics",
                "status_code": 500,
                "status": "error",
                "error": traceback.format_exc()
            }

    @staticmethod
    def __truncate_diag__(diagnostics):
        if diagnostics:
            # Remove newline ('\n') from diagnostic message
            shaped = re.sub(r'[\n\r]', ' ', str(diagnostics))
            return shaped[:500]
        else:
            return None


# responseActions

class ResponseActions:

    def __init__(self):
        pass

    def parse_response_actions(self, df):
        try:

            df.fillna(value="NULL", inplace=True)

            df['responseActions'].apply(lambda x: self.__split_response_actions__(x))

            return {
                "data": df,
                "function": "parse_response_actions",
                "status_code": 200,
                "status": "success"
            }
        except Exception as e:
            return {
                "function": "parse_response_actions",
                "status_code": 500,
                "status": "error",
                "error": traceback.format_exc()
            }

    @staticmethod
    def __split_response_actions__(ser):
        row_dict = {}

        string = re.sub(r'[\n\r\t]', ' ', str(ser))
        string = re.sub(r's\:', 's ', string)
        string = re.sub(r'\\n\\t', '', string)
        string = re.sub(r'\\n', '', string)
        results = re.findall(r'\"?([^\[\]\{\}\:\,]+)\"?', str(string))

        if len(results) <= 1:
            row_dict = {
                "actionTime": 0,
                "actionType": None,
                "description": None,
                "actionReference": None,
                "actionReferenceInt": None
            }
            return str(row_dict)
        else:
            return str(ser)

        # TODO: some of these responses are of length 1, 10, 21.

        # """
        #
        # Those of 21 are a list of 2 or more responses.
        # For time’s sake I'm just going to fill in NAN values with
        #  a dummy dict and return everything as a string.
        #
        # """


#
# elif (len(results) > 1) & (len(results) <= 6):
#     for index, result in enumerate(results):
#         # print(f'item = {result}')
#         if index % 2 == 0:
#             item_dict = {}
#             k = result.strip().replace("\'", "")
#         else:
#             v = result.strip().replace("\'", "")
#             item_dict[k] = v
#             row_dict.update(item_dict)
#
# elif (len(results) > 6):
#     i = 1
#     for index, result in enumerate(results):
#         # print(f'item = {result}')
#         if index % 2 == 0:
#             item_dict = {}
#             k = result.strip().replace("\'", "")
#         else:
#             v = result.strip().replace("\'", "")
#             item_dict[k] = v
#             i = i + 1
#             if i >= 6:
#                 break
#             row_dict.update(item_dict)

# return row_dict


# alertMonitorInfo

class AlertMonitorInfo:

    def __init__(self):
        pass

    @staticmethod
    def parse_alert_monitor_info(df):
        try:
            email_prog = re.compile(r"\{\'sendsEmails\'\:\s(True|False)")
            df['SendsEmails'] = df['alertMonitorInfo'].apply(lambda x: (email_prog.match(str(x))).group(1))
            ticket_prog = re.compile(r".*\'createsTicket\': (True|False)\}")
            df['CreatesTicket'] = df['alertMonitorInfo'].apply(lambda x: (ticket_prog.match(str(x))).group(1))

            df.drop(columns=['alertMonitorInfo'], inplace=True)

            return {
                "data": df,
                "function": "parse_alert_monitor_info",
                "status_code": 200,
                "status": "success"
            }

        except Exception as e:
            return {
                "function": "parse_alert_monitor_info",
                "status_code": 500,
                "status": "error",
                "error": traceback.format_exc()
            }
