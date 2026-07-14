from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.logging import get_logger

logger = get_logger(__name__)


def _apply_delta_config(builder: Any) -> Any:
    return (
        builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )


def _apply_azure_storage_config(builder: Any, config: AppConfig) -> Any:
    """Inject Azure Storage (ABFS / ADLS Gen2) auth into Spark config.

    Supports four auth modes:
    - sas:              Shared Access Signature token
    - account_key:      Storage account access key
    - service_principal: OAuth2 via AAD Service Principal (client secret)
    - managed_identity: OAuth2 via Azure Managed Identity / DefaultAzureCredential
    - default:          rely on environment (e.g. Databricks cluster config)
    """
    az = config.azure_storage
    if az is None:
        return builder

    account = az.account_name
    if not account:
        # Try to extract from lakehouse base_path: abfss://container@account.dfs.core...
        base = config.lakehouse.base_path
        if "@" in base:
            account = base.split("@", 1)[1].split(".", 1)[0]
    if not account:
        logger.warning("azure_storage.account_name not set; skipping ABFS auth injection")
        return builder

    import os

    auth_type = az.auth_type

    if auth_type == "sas":
        sas = os.getenv("AZURE_STORAGE_SAS_TOKEN", "")
        if not sas:
            raise RuntimeError(
                "azure_storage.auth_type=sas but AZURE_STORAGE_SAS_TOKEN is empty"
            )
        builder = builder.config(
            f"fs.azure.sas.{az.container}.{account}.dfs.core.windows.net", sas
        )
        logger.info("Injected SAS token for %s", account)

    elif auth_type == "account_key":
        key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "")
        if not key:
            raise RuntimeError(
                "azure_storage.auth_type=account_key but AZURE_STORAGE_ACCOUNT_KEY is empty"
            )
        builder = builder.config("fs.azure.account.key." + account + ".dfs.core.windows.net", key)
        logger.info("Injected account key for %s", account)

    elif auth_type == "service_principal":
        tenant = az.tenant_id or os.getenv("AZURE_TENANT_ID", "")
        client = az.client_id or os.getenv("AZURE_CLIENT_ID", "")
        secret = os.getenv("AZURE_CLIENT_SECRET", "")
        if not (tenant and client and secret):
            raise RuntimeError(
                "azure_storage.auth_type=service_principal requires "
                "tenant_id, client_id and AZURE_CLIENT_SECRET"
            )
        base_url = "dfs.core.windows.net"
        builder = (
            builder.config(f"fs.azure.account.auth.type.{account}.{base_url}", "OAuth")
            .config(
                f"fs.azure.account.oauth.provider.type.{account}.{base_url}",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            )
            .config(f"fs.azure.account.oauth2.client.id.{account}.{base_url}", client)
            .config(f"fs.azure.account.oauth2.client.secret.{account}.{base_url}", secret)
            .config(f"fs.azure.account.oauth2.client.endpoint.{account}.{base_url}",
                    f"https://login.microsoftonline.com/{tenant}/oauth2/token")
        )
        logger.info("Injected Service Principal auth for %s", account)

    elif auth_type == "managed_identity":
        base_url = "dfs.core.windows.net"
        client = az.client_id or os.getenv("AZURE_CLIENT_ID", "")
        builder = (
            builder.config(f"fs.azure.account.auth.type.{account}.{base_url}", "OAuth")
            .config(
                f"fs.azure.account.oauth.provider.type.{account}.{base_url}",
                "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider",
            )
        )
        if client:
            builder = builder.config(
                f"fs.azure.account.oauth2.client.id.{account}.{base_url}", client
            )
        # For pure Managed Identity without client secret, Databricks/HDInsight
        # usually provides the token provider. For generic Spark, you may need
        # a custom token provider class.
        logger.info("Injected Managed Identity auth for %s", account)

    else:
        # "default": assume the runtime already has credentials configured
        # (e.g. Databricks cluster with Passthrough or UAMI attached)
        logger.info("azure_storage.auth_type=default; skipping auth injection")

    return builder


def build_spark(config: AppConfig) -> SparkSession:
    builder = SparkSession.builder.appName(config.spark.app_name)
    extra_packages = [
        package.strip()
        for package in config.spark.config.get("spark.jars.packages", "").split(",")
        if package.strip()
    ]

    uses_delta = config.lakehouse.format == "delta" or (
        config.streaming.enabled and config.streaming.storage_format == "delta"
    )

    if uses_delta:
        try:
            from delta import configure_spark_with_delta_pip
        except ImportError as exc:  # pragma: no cover - environment guard
            raise RuntimeError("Install delta-spark to run Delta Lake jobs.") from exc

        builder = _apply_delta_config(configure_spark_with_delta_pip(builder, extra_packages=extra_packages))

    if config.spark.master:
        builder = builder.master(config.spark.master)

    for key, value in config.spark.config.items():
        if uses_delta and key == "spark.jars.packages":
            continue
        builder = builder.config(key, value)

    # Inject Azure Storage credentials BEFORE getOrCreate()
    builder = _apply_azure_storage_config(builder, config)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
