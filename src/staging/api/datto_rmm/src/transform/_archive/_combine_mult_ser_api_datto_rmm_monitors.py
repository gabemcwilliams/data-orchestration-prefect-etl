# ########################################################
# #
# #     Title: [TRANSFORM] - DattoRMM [API] - Combine - Monitors
# #     Created by: Gabe McWilliams
# #     Date: 2023/04/19
# #
# ######################################################

import traceback

"""

    This section will take all of the parsed dataframes and combine them into a single dataframe.

    The 'split_parse_transform_api_datto_rmm_monitors.py' is used to parse the dataframes.
"""


class Combine:

    def __init__(self, config: dict, vault) -> None:
        self.__details = config["DETAILS"]
        self.__data = config["DATA"]
        self.__timestamps = config["TIMESTAMPS"]

    def combine_and_rename(self, df_list: list) -> dict:

        try:
            df = df_list[0]
            for df_current in df_list[1:]:
                print(f"df type: {type(df_current)}")
                # pd.merge(df_new, df, how="left", on="alertUid", suffixes=('_drop'))
                df = df.merge(df_current, on="alertUid", how="left", suffixes=('', '_DROP')).filter(
                    regex='^(?!.*_DROP)')

            ## Drop unstructured columns
            df.drop(columns=["alertContext", "alertSourceInfo", "responseActions"], axis=1, inplace=True)

            # Fix date column remove [ns] that doesn't work in minio and postgres
            df['alertContextLastTriggered'] = df['alertContextLastTriggered'].astype('datetime64[s]')



            return {
                "data": df,
                "result": {
                    "title": self.__details["task_title"],
                    "status_code": 200,
                    "message": "Success",
                }
            }

        except Exception as e:
            return {
                "result":
                    {
                        "title": self.__details["task_title"],
                        "status_code": 500,
                        "message": t
                    }
            }
