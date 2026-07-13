from __future__ import annotations

from pyspark.sql import functions as F

from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.io import read_layer_table, write_layer_table

SILVER_SOURCE_COLUMNS: dict[str, tuple[str, ...]] = {
    "app_users": (
        "user_id",
        "public_user_id",
        "username",
        "email",
        "first_name",
        "last_name",
        "phone_number",
        "status",
        "created_at",
        "last_login",
    ),
    "user_addresses": (
        "address_id",
        "user_id",
        "address_type",
        "recipient_name",
        "phone_number",
        "street",
        "ward",
        "district",
        "city",
        "state",
        "postal_code",
        "country",
    ),
    "shops": (
        "shop_id",
        "public_shop_id",
        "shop_name",
        "shop_slug",
        "status",
        "created_at",
    ),
    "categories": (
        "category_id",
        "parent_category_id",
        "category_name",
        "slug",
        "is_active",
        "created_at",
    ),
    "products": (
        "product_id",
        "category_id",
        "public_product_id",
        "product_sku",
        "product_slug",
        "product_name",
        "brand",
        "status",
        "is_featured",
        "attributes_json",
        "images_json",
        "created_at",
    ),
    "product_variants": (
        "product_variant_id",
        "product_id",
        "public_variant_id",
        "variant_sku",
        "variant_name",
        "status",
        "options_json",
        "is_default",
        "unit_price",
        "compare_at_price",
        "currency",
        "stock_quantity",
        "reserved_quantity",
        "weight_kg",
        "images_json",
        "created_at",
    ),
    "vouchers": (
        "voucher_id",
        "voucher_code",
        "voucher_name",
        "discount_type",
        "scope_json",
        "starts_at",
        "ends_at",
        "minimum_order_amount",
        "is_active",
    ),
    "orders": (
        "order_id",
        "customer_id",
        "shipping_address_id",
        "billing_address_id",
        "order_number",
        "order_status",
        "payment_status",
        "discount_amount",
        "shipping_amount",
        "created_at",
    ),
    "order_items": (
        "order_item_id",
        "order_id",
        "product_id",
        "product_variant_id",
        "shop_id",
        "currency",
        "quantity",
        "unit_price",
        "item_subtotal",
        "discount_amount",
        "tax_amount",
        "item_total",
        "created_at",
    ),
    "order_vouchers": (
        "order_voucher_id",
        "order_id",
        "voucher_id",
    ),
    "payments": (
        "payment_id",
        "order_id",
        "payment_method",
        "payment_status",
    ),
    "shipments": (
        "shipment_id",
        "order_id",
        "carrier",
        "shipment_status",
        "shipped_at",
        "delivered_at",
    ),
}


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


def _project_table(df, table_name: str):
    columns = SILVER_SOURCE_COLUMNS.get(table_name)
    if columns is None:
        return df
    available_columns = [column for column in columns if column in df.columns]
    return df.select(*available_columns)


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
        write_layer_table(_clean_table(_project_table(df, table_name), keys), config, "silver", table_name)
