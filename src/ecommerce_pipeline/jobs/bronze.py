from __future__ import annotations

from pyspark.sql import functions as F

from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.delta import upsert_to_delta
from ecommerce_pipeline.io import read_postgres_query, read_postgres_table, write_layer_table
from ecommerce_pipeline.watermark import get_watermark, update_watermark


def _read_incremental_table(config: AppConfig, spark, table_name: str, incremental_column: str):
    previous_watermark = get_watermark(config, table_name)
    if previous_watermark is None:
        return read_postgres_table(spark, config, table_name)

    qualified_table = f"{config.postgres.source_schema}.{table_name}"
    query = f"""
        SELECT *
        FROM {qualified_table}
        WHERE {incremental_column} > (
            TIMESTAMP '{previous_watermark}' - INTERVAL '{config.batch.lookback_minutes} minutes'
        )
    """
    return read_postgres_query(spark, config, query)


def _read_source_table(config: AppConfig, spark, table_name: str):
    if config.batch.load_type != "incremental":
        return read_postgres_table(spark, config, table_name)

    incremental_column = config.batch.incremental_tables.get(table_name)
    if incremental_column is None:
        return read_postgres_table(spark, config, table_name)

    return _read_incremental_table(config, spark, table_name, incremental_column)


def _save_incremental_watermark(config: AppConfig, table_name: str, df) -> None:
    if config.batch.load_type != "incremental":
        return
    incremental_column = config.batch.incremental_tables.get(table_name)
    if incremental_column is None or incremental_column not in df.columns:
        return
    max_value = df.agg(F.max(F.col(incremental_column)).alias("watermark")).collect()[0]["watermark"]
    update_watermark(config, table_name, max_value)


def run_bronze(config: AppConfig, spark) -> None:
    for table_name in config.batch.source_tables:
        df = (
            _read_source_table(config, spark, table_name)
            .withColumn("_bronze_ingested_at", F.current_timestamp())
            .withColumn("_source_schema", F.lit(config.postgres.source_schema))
            .withColumn("_source_table", F.lit(table_name))
        )
        if config.batch.load_type == "incremental" and config.lakehouse.format == "delta":
            keys = config.streaming.primary_keys.get(table_name)
            if not keys:
                raise ValueError(f"Missing primary key config for incremental table: {table_name}")
            upsert_to_delta(
                spark=spark,
                df=df,
                path=config.lakehouse.table_path("bronze", table_name),
                keys=keys,
            )
        else:
            write_mode = "append" if config.batch.load_type == "incremental" else config.lakehouse.write_mode
            write_layer_table(df, config, "bronze", table_name, mode=write_mode)
        _save_incremental_watermark(config, table_name, df)
