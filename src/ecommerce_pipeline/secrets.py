"""Azure Key Vault secrets provider.

Fetches secrets from Azure Key Vault and injects them into os.environ
so that config.py's ${VAR} expansion can resolve them transparently.
"""
from __future__ import annotations

import os
from collections.abc import Iterable
from typing import TYPE_CHECKING

from ecommerce_pipeline.logging import get_logger

logger = get_logger(__name__)

if TYPE_CHECKING:
    from ecommerce_pipeline.config import AppConfig

# Mapping: environment variable name -> Key Vault secret name
# This allows the YAML to keep using ${AZURE_POSTGRES_PASSWORD}
# while the actual secret in Key Vault can be named "postgres-password".
_DEFAULT_SECRET_MAP: dict[str, str] = {
    "AZURE_POSTGRES_PASSWORD": "postgres-password",
    "AZURE_POSTGRES_USER": "postgres-user",
    "AZURE_STORAGE_ACCOUNT_KEY": "storage-account-key",
    "AZURE_STORAGE_SAS_TOKEN": "storage-sas-token",
    "AZURE_CLIENT_ID": "service-principal-client-id",
    "AZURE_CLIENT_SECRET": "service-principal-client-secret",
    "AZURE_TENANT_ID": "service-principal-tenant-id",
}


def _resolve_secret_name(env_var: str) -> str:
    """Allow override via AZURE_KV_<UPPER_NAME> env var.

    Example: AZURE_KV_AZURE_POSTGRES_PASSWORD=my-secret-name
    """
    override = os.getenv(f"AZURE_KV_{env_var}")
    if override:
        return override
    return _DEFAULT_SECRET_MAP.get(env_var, env_var.lower().replace("_", "-"))


def fetch_secrets_from_key_vault(
    key_vault_url: str,
    env_vars: Iterable[str] | None = None,
) -> dict[str, str]:
    """Fetch secrets from Azure Key Vault and set them in os.environ.

    Uses DefaultAzureCredential which supports:
    - Environment variables (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
    - Managed Identity (when running on Azure VM / AKS / App Service)
    - Azure CLI (az login)
    - Visual Studio Code credential
    - Interactive browser login

    Only sets env vars that are NOT already set, so local overrides take precedence.
    """
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError as exc:
        raise RuntimeError(
            "Azure Key Vault support requires: pip install 'ecommerce-pipeline[azure]'"
        ) from exc

    target_vars = list(env_vars) if env_vars else list(_DEFAULT_SECRET_MAP.keys())
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_url, credential=credential)

    resolved: dict[str, str] = {}
    for env_var in target_vars:
        # Skip if already set (local override wins)
        if os.getenv(env_var):
            logger.debug("Env var %s already set, skipping Key Vault lookup", env_var)
            continue

        secret_name = _resolve_secret_name(env_var)
        try:
            secret = client.get_secret(secret_name)
            value = secret.value
            if value is not None:
                os.environ[env_var] = value
                resolved[env_var] = "***"
                logger.info("Loaded secret %s from Key Vault", env_var)
        except Exception as exc:  # noqa: BLE001
            # Non-fatal: the variable might not be needed in all environments
            logger.warning(
                "Could not fetch secret '%s' for env var %s: %s",
                secret_name,
                env_var,
                exc,
            )

    return resolved


def get_secret(config: AppConfig, name: str, env_var: str | None = None) -> str | None:
    """Resolve a single secret value for code paths that need ad hoc lookup."""
    if env_var and os.getenv(env_var):
        return os.getenv(env_var)

    if config.secrets.provider == "env":
        return os.getenv(name)

    if config.secrets.provider == "azure_key_vault":
        fallback_name = env_var or name
        if not config.secrets.key_vault_url:
            return os.getenv(fallback_name)
        fetch_secrets_from_key_vault(config.secrets.key_vault_url, [fallback_name])
        return os.getenv(fallback_name)

    raise ValueError(f"Unsupported secrets provider: {config.secrets.provider}")


def bootstrap_secrets(provider: str, key_vault_url: str | None) -> None:
    """Entry point called by config loader before YAML expansion.

    Args:
        provider: "env" (default) or "azure_key_vault"
        key_vault_url: Vault URL when provider is azure_key_vault
    """
    if provider == "env":
        return
    if provider == "azure_key_vault":
        if not key_vault_url:
            logger.info("Azure Key Vault provider selected without URL; relying on existing environment variables")
            return
        fetch_secrets_from_key_vault(key_vault_url)
        return
    raise ValueError(f"Unsupported secrets provider: {provider}")
