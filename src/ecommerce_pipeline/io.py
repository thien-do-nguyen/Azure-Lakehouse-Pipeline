from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ecommerce_pipeline.config import AppConfig


def read_postgres_table(spark: SparkSession, config: AppConfig, table_name: str) -> DataFrame:
    dbtable = f"{config.postgres.source_schema}.{table_name}"
    return (
        spark.read.format("jdbc")
        .option("url", config.postgres.jdbc_url)
        .option("dbtable", dbtable)
        .option("user", config.postgres.user)
        .option("password", config.postgres.password)
        .option("driver", config.postgres.jdbc_driver)
        .load()
    )


def read_postgres_query(spark: SparkSession, config: AppConfig, query: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", config.postgres.jdbc_url)
        .option("query", query)
        .option("user", config.postgres.user)
        .option("password", config.postgres.password)
        .option("driver", config.postgres.jdbc_driver)
        .load()
    )


def read_layer_table(spark: SparkSession, config: AppConfig, layer: str, table_name: str) -> DataFrame:
    return spark.read.format(config.lakehouse.format).load(config.lakehouse.table_path(layer, table_name))


def write_layer_table(df: DataFrame, config: AppConfig, layer: str, table_name: str, mode: str | None = None) -> None:
    (
        df.write.format(config.lakehouse.format)
        .mode(mode or config.lakehouse.write_mode)
        .option("overwriteSchema", "true")
        .save(config.lakehouse.table_path(layer, table_name))
    )
