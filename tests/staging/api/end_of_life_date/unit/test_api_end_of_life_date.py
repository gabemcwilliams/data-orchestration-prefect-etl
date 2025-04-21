import pytest
import pandas as pd

from pathlib import Path


# test: ["prepare_tasks","logger"]
from src.staging.api.end_of_life_date.src.utilities.task_prep import *

# test: ["connections"]
# from src.staging.api.end_of_life_date.src.load.load_minio import *
# from src.staging.api.end_of_life_date.src.load.load_postgres import *

# test: ["all routes", "vault secrets", "all routes"]
from src.staging.api.end_of_life_date.src.extract.extract_api_end_of_life_date import *


@pytest.mark.end_of_life_date
class TestEndOfLifeDate:

    def setup_method(self):
        self.tasks = (prepare_tasks(config_dir=f"{Path(__file__).parent.resolve()}/tests.yaml"))['data']
        self.end_of_life_date = ExtractApiEndOfLifeDate(config=self.tasks[0])

    #     # self.postgres = PostgresLoad(config=self.tasks[1],df_input=)
    #     # self.minio = MinioLoad(config=self.tasks[2])

    # test: ["prepare_tasks","logger"]
    def test_yaml_import(self):
        assert isinstance(self.tasks, list)

    # test: ["all routes", "vault secrets", "all routes"]
    def test_create_windows_dataframe(self):
        data = self.end_of_life_date.create_windows_dataframe()

        result = data["result"]
        assert isinstance(result, dict)

        df = data["data"]
        assert isinstance(df, pd.DataFrame)


    def test_create_windows_server_dataframe(self):
        data = self.end_of_life_date.create_windows_server_dataframe()

        result = data["result"]
        assert isinstance(result, dict)

        df = data["data"]
        assert isinstance(df, pd.DataFrame)
