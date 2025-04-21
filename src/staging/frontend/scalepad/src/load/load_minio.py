########################################################
#
#     Title: Minio [LOAD]
#     Created by: Gabe McWilliams
#     Date: 2023/04/19
#
########################################################



import os
from minio import Minio
from io import BytesIO
import re
import traceback
import hvac
import ssl
import inspect
import urllib3


class MinioLoad:

    def __init__(self, df_input, config, vault):
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

    def __create_minio_client__(self):
        try:
            # Load custom CA certificate for SSL verification
            ca_cert_path = os.environ.get('SSL_CERT_FILE', '/prefect/ca.crt')  # Path to the custom CA certificate inside the container
            context = ssl.create_default_context(cafile=ca_cert_path)  # Create an SSL context with the custom CA certificate

            # Create the MinIO client with the custom SSL context
            minio_client = Minio(
                endpoint=re.sub("https?://", "", self.__secrets["url"]),
                secure=True,
                access_key=self.__secrets["accessKey"],
                secret_key=self.__secrets["secretKey"],
                http_client=urllib3.PoolManager(ssl_context=context)  # Use the SSL context in the HTTP client
            )

            return {
                "data": minio_client,
                "result": {
                    "job_title": self.__details["task_title"],
                    "status_code": 200,
                    "message": "MinIO client created with SSL"
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

    def __create_upload_details__(self):
        filename_details = [
            self.__data["source_method"],
            self.__timestamps["_OUT_DATA_TIMESTAMP"],
            self.__details["product"],
            self.__details["subject"],
        ]

        filename = ("_".join(filename_details)) + f'.{self.__data["destination"]["file_type"]}'

        bucket_location_details = [
            self.__details["product"],
            self.__details["subject"],
            self.__data["source_method"],
            self.__timestamps["_YEAR_DATA_TIMESTAMP"],
            self.__timestamps["_MONTH_DATA_TIMESTAMP"],
            self.__timestamps["_DAY_DATA_TIMESTAMP"],
            filename
        ]

        bucket_location = "/".join(bucket_location_details)

        return bucket_location

    def upload_to_minio(self):
        print(f'\n============  [START] - {inspect.currentframe().f_code.co_name}  ============\n')

        flat_file = None
        try:
            data = self.__create_minio_client__()
            minio_client = data["data"]
            minio_object = self.__create_upload_details__()

            # Create buffer with dataframe
            if self.__data["destination"]["file_type"] == "parquet":
                flat_file = self.__df_input.to_parquet(index=False)
            elif self.__data["destination"]["file_type"] == "csv":
                flat_file = self.__df_input.to_csv(index=False)
            elif self.__data["destination"]["file_type"] == "json":
                flat_file = str.encode(self.__df_input.to_json(orient="records"))

            minio_client.put_object(
                bucket_name=self.__data["destination"]["bucket"],
                object_name=minio_object,
                data=BytesIO(flat_file),
                length=len(flat_file),
                content_type=f'application/{self.__data["destination"]["file_type"]}')

            return {
                "data": minio_object,
                "result": {
                    "job_title": self.__details["task_title"],
                    "status_code": 200,
                    "file_format": self.__data["destination"]["file_type"],
                    "message": "File uploaded to MinIO",
                }
            }

        except Exception as e:
            t = traceback.format_exc()
            return {
                "result": {
                    "job_title": self.__details["task_title"],
                    "status_code": 500,
                    "file_format": self.__data["destination"]["file_type"],
                    "message": t
                }
            }
