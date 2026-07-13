from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")


def _load_dotenv(path: str | Path = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    with env_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            name = name.strip()
            value = value.strip().strip('"').strip("'")
            if name and name not in os.environ:
                os.environ[name] = value


def _expand_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if not isinstance(value, str):
        return value

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        default = match.group(2)
        resolved = os.getenv(name, default)
        if resolved is None:
            raise ValueError(f"Missing required environment variable: {name}")
        return resolved

    return _ENV_PATTERN.sub(replace, value)


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    source_schema: str
    jdbc_driver: str
    sslmode: str | None = None

    @property
    def jdbc_url(self) -> str:
        url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        if self.sslmode:
            url = f"{url}?sslmode={self.sslmode}"
        return url

    @property
    def psycopg_dsn(self) -> str:
        dsn = (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )
        if self.sslmode:
            dsn = f"{dsn} sslmode={self.sslmode}"
        return dsn


@dataclass(frozen=True)
class LakehouseConfig:
    base_path: str
    format: str
    write_mode: str

    def layer_path(self, layer: str) -> str:
        return f"{self.base_path.rstrip('/')}/{layer.lower()}"

    def table_path(self, layer: str, table: str) -> str:
        return f"{self.layer_path(layer)}/{table}"


@dataclass(frozen=True)
class BatchConfig:
    load_type: str
    watermark_path: str
    lookback_minutes: int
    source_tables: list[str]
    incremental_tables: dict[str, str]


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    master: str | None
    config: dict[str, str]


@dataclass(frozen=True)
class AzureStorageConfig:
    """Azure Storage authentication settings (parsed from YAML)."""
    auth_type: str  # "sas", "account_key", "service_principal", "managed_identity", "default"
    account_name: str = ""
    container: str = ""
    tenant_id: str = ""
    client_id: str = ""


@dataclass(frozen=True)
class SecretsConfig:
    provider: str
    key_vault_url: str | None = None


@dataclass(frozen=True)
class AppConfig:
    environment: str
    postgres: PostgresConfig
    lakehouse: LakehouseConfig
    batch: BatchConfig
    spark: SparkConfig
    secrets: SecretsConfig
    azure_storage: AzureStorageConfig | None = None


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _build_azure_storage(raw: dict[str, Any]) -> AzureStorageConfig | None:
    """Build AzureStorageConfig from the optional azure_storage section."""
    section = raw.get("azure_storage")
    if not section:
        return None
    return AzureStorageConfig(
        auth_type=section.get("auth_type", "default"),
        account_name=section.get("account_name", ""),
        container=section.get("container", ""),
        tenant_id=section.get("tenant_id", ""),
        client_id=section.get("client_id", ""),
    )


def load_config(path: str | Path | None = None, env: str | None = None) -> AppConfig:
    _load_dotenv()

    if path is None:
        selected_env = env or os.getenv("PIPELINE_ENV", "local")
        path = Path("configs") / f"{selected_env}.yaml"

    config_path = Path(path)
    base_path = config_path.with_name("base.yaml")
    raw: dict[str, Any] = {}
    if base_path.exists():
        with base_path.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
    with config_path.open("r", encoding="utf-8") as handle:
        raw = _deep_merge(raw, yaml.safe_load(handle) or {})

    # Bootstrap secrets BEFORE expanding env vars, so Key Vault secrets
    # become available for ${VAR} expansion.
    secrets_section = raw.get("secrets", {})
    provider = secrets_section.get("provider", "env")
    kv_url = secrets_section.get("key_vault_url")
    if isinstance(provider, str):
        provider = _expand_env(provider)
    # Expand kv_url itself (it may reference ${AZURE_KEY_VAULT_URL})
    if isinstance(kv_url, str):
        kv_url = _expand_env(kv_url)

    # Lazy import to avoid hard dependency on azure packages for local runs
    from ecommerce_pipeline.secrets import bootstrap_secrets
    bootstrap_secrets(provider, kv_url)

    raw = _expand_env(raw)

    return AppConfig(
        environment=raw["environment"],
        postgres=PostgresConfig(**raw["postgres"]),
        lakehouse=LakehouseConfig(**raw["lakehouse"]),
        batch=BatchConfig(**raw["batch"]),
        spark=SparkConfig(**raw["spark"]),
        secrets=SecretsConfig(**raw["secrets"]),
        azure_storage=_build_azure_storage(raw),
    )
