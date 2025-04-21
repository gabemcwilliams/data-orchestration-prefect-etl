"""
Vault TLS Profile Registration Script (No YAML required)

Registers a client TLS certificate with HashiCorp Vault and creates a policy to allow
access to a namespaced secret path. Useful for testing or bootstrapping Vault profiles
in local or CI/CD setups.

Key Features:
- Verifies and registers client certificate with Vault's cert-based auth method
- Creates a read/list policy for `secret/data/{profile_name}/*`
- Enables the cert auth method if it's not already active

Arguments:
--profile_name: required name (used in Vault path/policy)
--cert_file: path to PEM public certificate (default: VAULT_CLIENT_CERT)
--ca_cert: CA certificate path for Vault TLS (default: SSL_CERT_FILE)
--vault_addr: Vault URL (default: env:VAULT_ADDR)
--vault_token: Vault token (default: env:VAULT_TOKEN)
"""

import os
import argparse
import hvac
from pathlib import Path
from colorama import Fore, Style

# ============================
# CLI ARGUMENTS
# ============================

parser = argparse.ArgumentParser(description="Register Vault TLS profile without YAML.")
parser.add_argument("--profile_name", required=True, help="Profile name used in Vault policy and cert registration")
parser.add_argument("--cert_file", default=os.getenv("VAULT_CLIENT_CERT", None), help="Path to the client certificate (PEM or public.crt)")
parser.add_argument("--ca_cert", default=os.getenv("SSL_CERT_FILE"), help="Path to Vault's CA cert (for TLS verification)")
parser.add_argument("--vault_addr", default=os.getenv("VAULT_ADDR", "http://127.0.0.1:8200"))
parser.add_argument("--vault_token", default=os.getenv("VAULT_TOKEN", "your-root-token"))
args = parser.parse_args()

# ============================
# VALIDATION
# ============================

cert_file = Path(args.cert_file)
if not cert_file.exists():
    raise FileNotFoundError(f"Client certificate not found: {cert_file}")

ca_cert_path = args.ca_cert or os.getenv("VAULT_CACERT", None)
if ca_cert_path and not Path(ca_cert_path).exists():
    raise FileNotFoundError(f"CA cert file not found: {ca_cert_path}")

# ============================
# INIT VAULT CLIENT
# ============================

client = hvac.Client(
    url=args.vault_addr,
    token=args.vault_token,
    verify=ca_cert_path
)

if not client.is_authenticated():
    raise ValueError("Vault authentication failed. Check your VAULT_TOKEN.")

print(
    f"{Fore.LIGHTYELLOW_EX}Vault Client is {Fore.GREEN}[AUTHENTICATED]{Style.RESET_ALL}"
)

# ============================
# ENABLE CERT AUTH IF NEEDED
# ============================

auth_methods = client.sys.list_auth_methods()
if "cert/" not in auth_methods:
    client.sys.enable_auth_method("cert")
    print("Certificate authentication enabled.")
else:
    print("Certificate authentication already enabled.")

# ============================
# CREATE POLICY
# ============================

policy_name = args.profile_name.lower().strip()
policy = f"""
path "secret/data/{policy_name}/*" {{
  capabilities = ["read", "list"]
}}
"""

client.sys.create_or_update_policy(name=policy_name, policy=policy)
print(f"Policy '{policy_name}' created or updated.")

# ============================
# REGISTER CERT FOR TLS AUTH
# ============================

with open(cert_file, "r") as f:
    public_cert = f.read()

client.write(
    f"auth/cert/certs/{policy_name}",
    display_name=policy_name,
    policies=[policy_name],
    certificate=public_cert
)

print(f"TLS profile '{policy_name}' registered and bound to policy '{policy_name}'.")
