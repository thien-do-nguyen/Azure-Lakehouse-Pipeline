from __future__ import annotations

from pyspark.sql import DataFrame

from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.jobs.gold_incremental import run_gold_incremental


def update_gold_after_silver_batch(config: AppConfig, spark, batch_df: DataFrame) -> None:
    if batch_df.limit(1).count() == 0:
        return
    run_gold_incremental(config, spark, batch_df)
