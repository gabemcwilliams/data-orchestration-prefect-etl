"""
VaultManager: Secure credential retrieval using HashiCorp Vault.

Supports:
- Token authentication
- Certificate-based authentication (preferred for automation)
"""

import hvac
import threading
import os
import traceback
from colorama import Fore, Style
from loguru import logger


class VaultManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.__initialized = False
        return cls._instance

    def __init__(self, auth_method="cert"):
        if not self.__initialized:
            self.auth_method = auth_method.lower()
            self.__client = self.get_client()
            self.__initialized = True

    def get_client(self) -> hvac.Client:
        """
        Returns a Vault client instance using the selected authentication method.
        """
        if self.auth_method == "cert":
            return self.get_cert_client()
        else:
            return self.get_token_client()

    @staticmethod
    def get_token_client() -> hvac.Client:
        """
        Initializes and authenticates a Vault client using token-based authentication.
        """
        try:
            vault_addr = os.environ.get("VAULT_ADDR")
            vault_token = os.environ.get("VAULT_TOKEN")
            vault_ca_cert = os.environ.get("VAULT_CACERT")
            vault_namespace = os.environ.get("VAULT_NAMESPACE")

            if not vault_addr or not vault_token:
                raise ValueError("Missing required environment variables: VAULT_ADDR and/or VAULT_TOKEN.")

            client = hvac.Client(
                url=vault_addr,
                token=vault_token,
                verify=vault_ca_cert,
                namespace=vault_namespace
            )

            if vault_namespace:
                client.adapter.namespace = vault_namespace

            if not client.is_authenticated():
                raise ValueError("Vault authentication failed. Please check your VAULT_TOKEN.")

            print(
                f"{Style.RESET_ALL} * Vault {Fore.LIGHTBLUE_EX}[TOKEN AUTH]{Style.RESET_ALL} "
                f"Client is {Fore.GREEN}[AUTHENTICATED]{Style.RESET_ALL}"
            )
            logger.info("Vault authenticated successfully using token method.")
            return client

        except Exception as e:
            logger.error(f"Vault token authentication failed: {traceback.format_exc()}")
            print(f"{Fore.RED}* [FAILED]{Style.RESET_ALL} to initialize Vault client: {traceback.format_exc()}")
            raise SystemExit(f"Error: {e}")

    @staticmethod
    def get_cert_client() -> hvac.Client:
        """
        Initializes and authenticates a Vault client using certificate-based authentication.
        """
        try:
            vault_addr = os.environ.get("VAULT_ADDR")
            vault_ca_cert = os.environ.get("VAULT_CACERT")
            vault_client_cert = os.environ.get("VAULT_CLIENT_CERT")
            vault_client_key = os.environ.get("VAULT_CLIENT_KEY")
            vault_namespace = os.environ.get("VAULT_NAMESPACE")

            if not vault_addr or not vault_client_cert or not vault_client_key:
                raise ValueError(
                    "Missing required environment variables: VAULT_ADDR, VAULT_CLIENT_CERT, or VAULT_CLIENT_KEY."
                )

            client = hvac.Client(
                url=vault_addr,
                cert=(vault_client_cert, vault_client_key),
                verify=vault_ca_cert,
                namespace=vault_namespace
            )

            if vault_namespace:
                client.adapter.namespace = vault_namespace

            auth_response = client.auth.cert.login()

            if "auth" not in auth_response or not auth_response["auth"]["client_token"]:
                raise ValueError("Vault authentication using cert method failed.")

            print(
                f"{Style.RESET_ALL} * Vault {Fore.LIGHTBLUE_EX}[CERT AUTH]{Style.RESET_ALL} "
                f"Client is {Fore.GREEN}[AUTHENTICATED]{Style.RESET_ALL}"
            )
            logger.info("Vault authenticated successfully using certificate method.")
            return client

        except Exception as e:
            logger.error(f"Vault certificate authentication failed: {traceback.format_exc()}")
            print(f"{Fore.RED}* [FAILED]{Style.RESET_ALL} to initialize Vault client: {traceback.format_exc()}")
            raise SystemExit(f"Error: {e}")

    def read_secret(self, mount_point: str, path: str) -> dict:
        """
        Reads a secret from the specified Vault KV path.

        Args:
            mount_point (str): The top-level KV mount (e.g., 'api', 'db')
            path (str): The relative path under that mount

        Returns:
            dict: Secret contents from Vault
        """
        return self.__client.secrets.kv.read_secret(mount_point=mount_point, path=f"/{path}")["data"]["data"]
