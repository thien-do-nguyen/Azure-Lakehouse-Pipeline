from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")


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
    dwh_schema: str
    jdbc_driver: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def psycopg_dsn(self) -> str:
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"


@dataclass(frozen=True)
class LakehouseConfig:
    base_path: str
    format: str
    write_mode: str
    checkpoint_path: str

    def layer_path(self, layer: str) -> str:
        return f"{self.base_path.rstrip('/')}/{layer.lower()}"

    def table_path(self, layer: str, table: str) -> str:
        return f"{self.layer_path(layer)}/{table}"


@dataclass(frozen=True)
class BatchConfig:
    load_type: str
    run_id_prefix: str
    watermark_path: str
    lookback_minutes: int
    source_tables: list[str]
    incremental_column: str
    incremental_tables: dict[str, str]


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    master: str | None
    config: dict[str, str]


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


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: str | Path | None = None, env: str | None = None) -> AppConfig:
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
    raw = _expand_env(raw)

    return AppConfig(
        environment=raw["environment"],
        postgres=PostgresConfig(**raw["postgres"]),
        lakehouse=LakehouseConfig(**raw["lakehouse"]),
        batch=BatchConfig(**raw["batch"]),
        spark=SparkConfig(**raw["spark"]),
        secrets=SecretsConfig(**raw["secrets"]),
    )
