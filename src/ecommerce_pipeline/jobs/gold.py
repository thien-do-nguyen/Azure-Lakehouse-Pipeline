from __future__ import annotations

from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.delta import scd2_merge, synchronize_to_delta
from ecommerce_pipeline.io import read_layer_table, write_layer_table
from ecommerce_pipeline.jobs.gold_transforms import build_dim_customer, build_gold_tables


def run_gold(config: AppConfig, spark) -> None:
    if config.lakehouse.format == "delta":
        customer_source = build_dim_customer(read_layer_table(spark, config, "silver", "app_users"))
        scd2_merge(
            spark=spark,
            source_df=customer_source,
            path=config.lakehouse.table_path("gold", "dim_customer"),
            natural_keys=["source_customer_id"],
            tracked_hash_column="scd_hash",
            effective_timestamp_col="source_updated_at",
            surrogate_key_col="customer_key",
            type1_columns=["last_login_at"],
        )
        customer_history = read_layer_table(spark, config, "gold", "dim_customer")
        gold_tables = build_gold_tables(config, spark, customer_history=customer_history)
        _run_delta_gold(config, spark, gold_tables)
        return

    gold_tables = build_gold_tables(config, spark)
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
            # dim_customer was committed before facts so temporal keys resolve
            # against the same SCD2 history used by this batch.
            continue

        write_layer_table(df, config, "gold", table_name)
