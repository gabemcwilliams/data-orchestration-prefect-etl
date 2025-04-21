import hvac  # HashiCorp Vault API client for Python
import threading  # Used for thread-safe singleton implementation
import os  # Allows access to environment variables
import traceback  # Captures detailed error information
from colorama import Fore, Style  # Provides colored output for logging


class VaultManager:
    _instance = None  # Stores the singleton instance of VaultManager
    _lock = threading.Lock()  # Ensures thread safety when creating an instance

    def __new__(cls, *args, **kwargs):
        """Creates a new instance of VaultManager if one does not already exist."""
        with cls._lock:  # Ensures thread safety
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.__initialized = False  # Ensure initialization
        return cls._instance

    def __init__(self, auth_method="cert"):
        """Initializes the Vault client, preventing re-initialization of an existing instance."""
        if not self.__initialized:  # Ensures the instance is initialized only once
            self.auth_method = auth_method.lower()  # Determines the authentication method ('token' or 'cert')
            self.__client = self.get_client()  # Retrieves the appropriate client
            self.__initialized = True  # Marks initialization as complete

    def get_client(self) -> hvac.Client:
        """Selects and initializes the Vault client based on the authentication method."""
        if self.auth_method == "cert":  # Uses certificate-based authentication if specified
            return self.get_cert_client()
        else:  # Defaults to token-based authentication
            return self.get_token_client()

    @staticmethod
    def get_token_client() -> hvac.Client:
        """Initializes and authenticates the Vault client using token authentication."""
        try:
            # Retrieve required environment variables for authentication
            vault_addr = os.environ.get("VAULT_ADDR")
            vault_token = os.environ.get("VAULT_TOKEN")
            vault_ca_cert = os.environ.get("VAULT_CACERT")
            vault_namespace = os.environ.get("VAULT_NAMESPACE")

            # Ensure necessary credentials are available
            if not vault_addr or not vault_token:
                raise ValueError("Missing required environment variables: VAULT_ADDR and/or VAULT_TOKEN.")

            # Create a Vault client instance
            client = hvac.Client(
                url=vault_addr,
                token=vault_token,
                verify=vault_ca_cert,  # TLS verification (None disables it)
                namespace=vault_namespace  # Optional namespace for Vault Enterprise
            )

            if vault_namespace:
                client.adapter.namespace = vault_namespace  # Explicitly set the namespace

            # Check if the authentication was successful
            if not client.is_authenticated():
                raise ValueError("Vault authentication failed. Please check your VAULT_TOKEN.")

            print(
                f"{Style.RESET_ALL} * Vault {Fore.LIGHTBLUE_EX}[TOKEN AUTH]{Style.RESET_ALL} "
                f"Client is {Fore.GREEN}[AUTHENTICATED]{Style.RESET_ALL}"
            )

            return client  # Return the authenticated client

        except Exception as e:
            # Log error details and exit the application
            print(f"{Fore.RED}* [FAILED]{Style.RESET_ALL} to initialize Vault client: {traceback.format_exc()}")
            raise SystemExit(f"Error: {e}")

    @staticmethod
    def get_cert_client() -> hvac.Client:
        """Initializes and authenticates the Vault client using certificate-based authentication."""
        try:
            # Retrieve required environment variables for certificate authentication
            vault_addr = os.environ.get("VAULT_ADDR")
            vault_ca_cert = os.environ.get("VAULT_CACERT")
            vault_client_cert = os.environ.get("VAULT_CLIENT_CERT")
            vault_client_key = os.environ.get("VAULT_CLIENT_KEY")
            vault_namespace = os.environ.get("VAULT_NAMESPACE")

            # Ensure necessary credentials are available
            if not vault_addr or not vault_client_cert or not vault_client_key:
                raise ValueError(
                    "Missing required environment variables: VAULT_ADDR, VAULT_CLIENT_CERT, or VAULT_CLIENT_KEY."
                )

            # Create a Vault client instance using certificate authentication
            client = hvac.Client(
                url=vault_addr,
                cert=(vault_client_cert, vault_client_key),  # TLS client certificate and key
                verify=vault_ca_cert,  # TLS verification (None disables it)
                namespace=vault_namespace  # Optional namespace for Vault Enterprise
            )

            if vault_namespace:
                client.adapter.namespace = vault_namespace  # Explicitly set the namespace

            # Authenticate using the certificate authentication method
            auth_response = client.auth.cert.login()

            # Validate the authentication response
            if "auth" not in auth_response or not auth_response["auth"]["client_token"]:
                raise ValueError("Vault authentication using cert method failed.")

            print(
                f"{Style.RESET_ALL} * Vault {Fore.LIGHTBLUE_EX}[CERT AUTH]{Style.RESET_ALL} "
                f"Client is {Fore.GREEN}[AUTHENTICATED]{Style.RESET_ALL}"
            )

            return client  # Return the authenticated client

        except Exception as e:
            # Log error details and exit the application
            print(f"{Fore.RED}* [FAILED]{Style.RESET_ALL} to initialize Vault client: {traceback.format_exc()}")
            raise SystemExit(f"Error: {e}")

    def read_secret(self, mount_point: str, path: str) -> dict:
        """Reads a secret from HashiCorp Vault at the specified mount point and path."""
        resp = self.__client.secrets.kv.read_secret(mount_point=mount_point, path=f'/{path}')
        return resp['data']['data']  # Extracts and returns the secret data
