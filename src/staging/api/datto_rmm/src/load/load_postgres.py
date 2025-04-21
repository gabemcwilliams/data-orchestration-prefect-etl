import os
from sqlalchemy import create_engine
import hvac
import traceback
import inspect


class PostgresLoad:
    """
    Handles loading a DataFrame to a PostgreSQL table with SSL verification using custom CA certificates.
    Reads connection credentials securely from Vault.
    """

    def __init__(self, df_input, config: dict, vault) -> None:
        self.__df_input = df_input
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

    def load_to_postgres(self):
        """
        Loads the provided DataFrame to the target PostgreSQL schema.table.
        Uses SSL with a specified root certificate for encrypted communication.

        Returns:
            dict: result metadata including table name and status code.
        """
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')

        database = self.__data["destination"]["database"]
        schema = self.__data["destination"]["schema"]
        table = f'{self.__data["source_method"]}_{self.__data["destination"]["table"]}'

        try:
            # Build secure connection URI
            user = self.__secrets["POSTGRES_USER"]
            password = self.__secrets["POSTGRES_PASSWORD"]
            uri = self.__secrets["POSTGRES_URI"]
            port = self.__secrets["POSTGRES_PORT"]
            ca_cert_path = os.environ.get('SSL_CERT_FILE', '/prefect/ca.crt')

            db_uri = (
                f'postgresql://{user}:{password}@{uri}:{port}/{database}'
                f'?sslmode=verify-full&sslrootcert={ca_cert_path}'
            )

            engine = create_engine(db_uri, echo=True)

            # Write DataFrame to the database
            self.__df_input.to_sql(
                name=table,
                con=engine,
                if_exists='replace',
                index=False,
                schema=schema
            )

            return {
                "result": {
                    "job_title": self.__details["task_title"],
                    "status_code": 200,
                    "database": database,
                    "schema": schema,
                    "table": table,
                    "content-type": "application/json",
                    "message": "Data loaded successfully",
                }
            }

        except Exception as e:
            t = traceback.format_exc()
            return {
                "result": {
                    "job_title": self.__details["task_title"],
                    "status_code": 500,
                    "database": database,
                    "schema": schema,
                    "table": table,
                    "message": t,
                }
            }
