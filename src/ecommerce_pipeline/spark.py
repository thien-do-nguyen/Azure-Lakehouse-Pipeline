from __future__ import annotations

from pyspark.sql import SparkSession

from ecommerce_pipeline.config import AppConfig


def build_spark(config: AppConfig) -> SparkSession:
    builder = SparkSession.builder.appName(config.spark.app_name)
    if config.lakehouse.format == "delta":
        try:
            from delta import configure_spark_with_delta_pip
        except ImportError as exc:  # pragma: no cover - environment guard
            raise RuntimeError("Install delta-spark to run Delta Lake jobs.") from exc

        builder = configure_spark_with_delta_pip(builder)

    if config.spark.master:
        builder = builder.master(config.spark.master)

    for key, value in config.spark.config.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
