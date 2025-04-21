import pytest
import pandas as pd

from pathlib import Path

# test: ["prepare_tasks","logger"]
from src.staging.api.datto_rmm.src.utilities.task_prep import *

# test: ["connections"]
# from src.staging.api.datto_rmm.src.load.load_minio import *
# from src.staging.api.datto_rmm.src.load.load_postgres import *

# test: ["all routes", "vault secrets", "all routes"]
from src.staging.api.datto_rmm.src.extract.extract_api_datto_rmm import *


@pytest.mark.datto_rmm
class TestDattoRmm:

    def setup_method(self):
        self.tasks = (prepare_tasks(config_dir=f"{Path(__file__).parent.resolve()}/tests.yaml"))['data']
        self.datto_rmm = ExtractApiDattoRMM(config=self.tasks[0])

    #     # self.postgres = PostgresLoad(config=self.tasks[1],df_input=)
    #     # self.minio = MinioLoad(config=self.tasks[2])

    # test: ["prepare_tasks","logger"]
    def test_yaml_import(self):
        assert isinstance(self.tasks, list)

    # test: ["all routes", "vault secrets", "all routes"]
    def test_create_account_alerts_open_dataframe(self):
        data = self.datto_rmm.create_account_alerts_open_dataframe()

        result = data["result"]
        assert isinstance(result, dict)

        df = data["data"]
        assert isinstance(df, pd.DataFrame)


    def test_create_account_alerts_resolved_dataframe(self):
        data = self.datto_rmm.create_account_alerts_resolved_dataframe()

        result = data["result"]
        assert isinstance(result, dict)

        df = data["data"]
        assert isinstance(df, pd.DataFrame)

    def test_create_account_dataframe(self):
        data = self.datto_rmm.create_account_dataframe()

        result = data["result"]
        assert isinstance(result, dict)

        df = data["data"]
        assert isinstance(df, pd.DataFrame)


    def test_create_account_sites_dataframe(self):
        data = self.datto_rmm.create_account_sites_dataframe()

        result = data["result"]
        assert isinstance(result, dict)

        df = data["data"]
        assert isinstance(df, pd.DataFrame)

    def test_create_account_variables_dataframe(self):
        data = self.datto_rmm.create_account_variables_dataframe()

        result = data["result"]
        assert isinstance(result, dict)

        df = data["data"]
        assert isinstance(df, pd.DataFrame)


    def test_create_activity_logs_dataframe(self):
        data = self.datto_rmm.create_activity_logs_dataframe()

        result = data["result"]
        assert isinstance(result, dict)

        df = data["data"]
        assert isinstance(df, pd.DataFrame)

    def test_create_devices_dataframe(self):
        data = self.datto_rmm.create_devices_dataframe()

        result = data["result"]
        assert isinstance(result, dict)

        df = data["data"]
        assert isinstance(df, pd.DataFrame)


    def test_create_site_variables_dataframe(self):
        data = self.datto_rmm.create_site_variables_dataframe()

        result = data["result"]
        assert isinstance(result, dict)

        df = data["data"]
        assert isinstance(df, pd.DataFrame)
