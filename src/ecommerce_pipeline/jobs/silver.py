from __future__ import annotations

from pyspark.sql import functions as F

from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.io import read_layer_table, write_layer_table


def _trim_strings(df):
    for column_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(column_name, F.trim(F.col(column_name)))
    return df


def _dedupe(df, keys: list[str]):
    return df.dropDuplicates(keys)


def _clean_table(df, keys: list[str]):
    system_columns = [name for name in df.columns if name.startswith("_")]
    cleaned = df.drop(*system_columns)
    return _dedupe(_trim_strings(cleaned), keys)


def run_silver(config: AppConfig, spark) -> None:
    table_keys = {
        "app_users": ["user_id"],
        "user_addresses": ["address_id"],
        "shops": ["shop_id"],
        "categories": ["category_id"],
        "products": ["product_id"],
        "product_variants": ["product_variant_id"],
        "vouchers": ["voucher_id"],
        "orders": ["order_id"],
        "order_items": ["order_item_id"],
        "order_vouchers": ["order_voucher_id"],
        "payments": ["payment_id"],
        "shipments": ["shipment_id"],
    }

    for table_name, keys in table_keys.items():
        df = read_layer_table(spark, config, "bronze", table_name)
        write_layer_table(_clean_table(df, keys), config, "silver", table_name)
