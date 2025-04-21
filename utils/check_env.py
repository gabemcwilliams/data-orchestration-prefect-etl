"""
Vault Environment Diagnostics Script

Purpose:
Prints current environment variables related to Vault TLS configuration.
Useful for debugging Vault authentication issues with client certs or CA chains.
"""

import os

def print_vault_env():
    print("\n=========== Vault Environment Diagnostics ===========\n")

    print(f'VAULT_ADDR         : {os.environ.get("VAULT_ADDR", None)}')
    print(f'VAULT_CLIENT_CERT  : {os.environ.get("VAULT_CLIENT_CERT", None)}')
    print(f'VAULT_CLIENT_KEY   : {os.environ.get("VAULT_CLIENT_KEY", None)}')
    print(f'VAULT_CACERT       : {os.environ.get("VAULT_CACERT", None)}')
    print(f'SSL_CERT_FILE      : {os.environ.get("SSL_CERT_FILE", None)}')

    print("\n=====================================================\n")


if __name__ == "__main__":
    print_vault_env()
