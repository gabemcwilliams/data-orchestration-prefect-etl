"""
Connection wrapper for PostgreSQL using SQLAlchemy.

Features:
- Secrets fetched securely from Vault
- Dynamically constructs connection URI from secret config
- Returns engine + metadata for use in ETL pipelines
"""

import os
from sqlalchemy import create_engine
import hvac
import traceback
import inspect
from loguru import logger


class ConnPostgresql:
    """
    PostgreSQL connection utility using secrets from Vault.
    Supports multiple database targets and dynamic engine creation.
    """

    def __init__(self, config: dict, vault) -> None:
        self.__secrets = vault.read_secret(
            mount_point=config["mount_point"],
            path=config["path"]
        )



    def conn_to_postgres(self, database: str) -> dict:
        """
        Creates a SQLAlchemy engine connected to the specified PostgreSQL database.

        Args:
            database (str): Name of the target database

        Returns:
            dict: Contains SQLAlchemy engine and connection metadata
        """
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')
        logger.info(f"Connecting to PostgreSQL: {database}")

        try:
            user = self.__secrets["POSTGRES_USER"]
            password = self.__secrets["POSTGRES_PASSWORD"]
            uri = self.__secrets["POSTGRES_URI"]
            port = self.__secrets["POSTGRES_PORT"]

            db_uri = f'postgresql://{user}:{password}@{uri}:{port}/{database}'
            engine = create_engine(db_uri, echo=True)

            return {
                "engine": engine,
                "result": {
                    "job_title": inspect.currentframe().f_code.co_name,
                    "status_code": 200,
                    "database": database,
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
                    "database": database,
                    "message": t,
                }
            }
