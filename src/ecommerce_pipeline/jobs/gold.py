from __future__ import annotations

from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.delta import scd2_merge, synchronize_to_delta
from ecommerce_pipeline.io import write_layer_table
from ecommerce_pipeline.jobs.gold_transforms import build_gold_tables


def run_gold(config: AppConfig, spark) -> None:
    gold_tables = build_gold_tables(config, spark)
    if config.lakehouse.format == "delta":
        _run_delta_gold(config, spark, gold_tables)
        return

    for table_name, df in gold_tables.items():
        write_layer_table(df, config, "gold", table_name)


def _run_delta_gold(config: AppConfig, spark, gold_tables) -> None:
    for table_name, df in gold_tables.items():
        if table_name == "fact_sales":
            synchronize_to_delta(
                spark=spark,
                df=df,
                path=config.lakehouse.table_path("gold", table_name),
                keys=["source_order_id", "source_order_item_id"],
            )
            continue

        if table_name == "dim_customer":
            scd2_merge(
                spark=spark,
                source_df=df,
                path=config.lakehouse.table_path("gold", table_name),
                natural_keys=["source_customer_id"],
                tracked_hash_column="scd_hash",
            )
            continue

        write_layer_table(df, config, "gold", table_name)
