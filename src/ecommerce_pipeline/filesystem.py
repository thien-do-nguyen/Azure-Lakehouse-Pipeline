from __future__ import annotations

import os
from typing import IO

import fsspec


def _storage_options(path: str) -> dict[str, str]:
    if not path.startswith(("abfs://", "abfss://")):
        return {}

    auth_type = os.getenv("AZURE_STORAGE_AUTH_TYPE", "account_key")
    options: dict[str, str] = {}
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    if account_name:
        options["account_name"] = account_name

    if auth_type == "account_key":
        account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "")
        if account_key:
            options["account_key"] = account_key
    elif auth_type == "sas":
        sas_token = os.getenv("AZURE_STORAGE_SAS_TOKEN", "")
        if sas_token:
            options["sas_token"] = sas_token
    elif auth_type == "service_principal":
        tenant_id = os.getenv("AZURE_TENANT_ID", "")
        client_id = os.getenv("AZURE_CLIENT_ID", "")
        client_secret = os.getenv("AZURE_CLIENT_SECRET", "")
        if tenant_id and client_id and client_secret:
            options.update(
                {
                    "tenant_id": tenant_id,
                    "client_id": client_id,
                    "client_secret": client_secret,
                }
            )

    return options


def exists(path: str) -> bool:
    fs, resolved_path = fsspec.core.url_to_fs(path, **_storage_options(path))
    return fs.exists(resolved_path)


def open_text(path: str, mode: str) -> IO[str]:
    return fsspec.open(path, mode=mode, encoding="utf-8", **_storage_options(path)).open()


def ensure_parent(path: str) -> None:
    fs, resolved_path = fsspec.core.url_to_fs(path, **_storage_options(path))
    parent = resolved_path.rsplit("/", 1)[0] if "/" in resolved_path else ""
    if parent:
        fs.mkdirs(parent, exist_ok=True)
