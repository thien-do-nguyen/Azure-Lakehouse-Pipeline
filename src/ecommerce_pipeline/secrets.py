from __future__ import annotations

import os

from ecommerce_pipeline.config import AppConfig


def get_secret(config: AppConfig, name: str, env_var: str | None = None) -> str | None:
    if env_var and os.getenv(env_var):
        return os.getenv(env_var)

    if config.secrets.provider == "env":
        return os.getenv(name)

    if config.secrets.provider == "azure_key_vault":
        if not config.secrets.key_vault_url:
            return os.getenv(env_var or name)
        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient
        except ImportError as exc:  # pragma: no cover - optional cloud dependency
            raise RuntimeError("Install ecommerce-pipeline[azure] to use Azure Key Vault secrets.") from exc

        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=config.secrets.key_vault_url, credential=credential)
        return client.get_secret(name).value

    raise ValueError(f"Unsupported secrets provider: {config.secrets.provider}")
