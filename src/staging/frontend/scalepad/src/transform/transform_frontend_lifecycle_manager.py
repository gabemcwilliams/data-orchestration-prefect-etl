import traceback  # view stack
import inspect  # get function name
import sys
import re
import pandas as pd  # dataframe manipulation


class TransformLifecycleManager:
    def __init__(self, df: pd.DataFrame) -> None:
        self.__df = df

        # keep first index of tags list
        self.transform_tags()


    @property
    def df(self) -> pd.DataFrame:
        return self.__df

    # def transform_activity_dataframe(self) -> dict:
    #     return {
    #         "data": self.__df,
    #         "result": {
    #             "job_title": inspect.currentframe().f_code.co_name,
    #             "status_code": 200,
    #             "message": "DataFrame created successfully"
    #         }
    #     }
    #
    # def transform_tags(self):
    #     print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')
    #
    #     try:
    #         # apply model to each row of dataframe
    #         self.__df['tag'] = \
    #             self.df['tags'].apply(
    #                 lambda x: None \
    #                     if isinstance(x, str) is False \
    #                     else x[2:38] # removing both [' and '] from str
    #             )  # str(list) | None -> str
    #
    #         self.__df.drop('tags', axis=1, inplace=True)
    #
    #     except Exception as e:
    #         t = traceback.format_exc()
    #         sys.exit(t)
