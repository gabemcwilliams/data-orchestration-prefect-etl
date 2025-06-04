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

