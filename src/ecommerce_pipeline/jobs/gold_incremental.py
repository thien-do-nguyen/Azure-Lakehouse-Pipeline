from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.delta import replace_delta_scope, scd2_merge, upsert_to_delta
from ecommerce_pipeline.io import read_layer_table
from ecommerce_pipeline.jobs.gold_transforms import build_dim_customer, build_gold_tables

DIRECT_ORDER_TABLES = {"orders", "order_items", "order_vouchers", "payments", "shipments"}

DIMENSION_MERGE_KEYS: dict[str, list[str]] = {
    "dim_date": ["date_key"],
    "dim_time": ["time_key"],
    "dim_location": ["source_address_id"],
    "dim_shop": ["source_shop_id"],
    "dim_category": ["source_category_id"],
    "dim_product": ["source_product_variant_id"],
    "dim_promotion": ["natural_promotion_hash"],
    "dim_payment": ["natural_payment_hash"],
    "dim_shipping": ["natural_shipping_hash"],
}

SOURCE_DIMENSIONS: dict[str, set[str]] = {
    "app_users": {"dim_customer"},
    "user_addresses": {"dim_location"},
    "shops": {"dim_shop"},
    "categories": {"dim_category"},
    "products": {"dim_product"},
    "product_variants": {"dim_product"},
    "vouchers": {"dim_promotion"},
    "order_vouchers": {"dim_promotion"},
    "orders": {"dim_date", "dim_time", "dim_promotion"},
    "payments": {"dim_payment"},
    "shipments": {"dim_shipping"},
}

CUSTOMER_TYPE2_FIELDS = ["username", "email", "first_name", "last_name", "phone_number", "status"]


def _changed_values(events: DataFrame, table_name: str, field_name: str) -> DataFrame:
    selected = events.where(F.col("table_name") == F.lit(table_name))
    before = selected.select(F.get_json_object("before_json", f"$.{field_name}").alias(field_name))
    after = selected.select(F.get_json_object("after_json", f"$.{field_name}").alias(field_name))
    return (
        before.unionByName(after)
        .where(F.col(field_name).isNotNull())
        .select(F.col(field_name).cast("long").alias(field_name))
        .distinct()
    )


def _events_with_changed_fields(events: DataFrame, table_name: str, fields: list[str]) -> DataFrame:
    selected = events.where(F.col("table_name") == F.lit(table_name))
    changed = F.col("operation").isin("INSERT", "DELETE") if "operation" in events.columns else F.lit(False)
    for field_name in fields:
        before = F.get_json_object("before_json", f"$.{field_name}")
        after = F.get_json_object("after_json", f"$.{field_name}")
        changed = changed | ~F.coalesce(before.eqNullSafe(after), F.lit(False))
    return selected.where(changed)


def impacted_order_ids(config: AppConfig, spark: SparkSession, events: DataFrame) -> DataFrame:
    """Resolve changed source rows to the complete set of affected order ids."""
    valid = events.where(F.col("is_valid_event") == F.lit(True))
    impacted: list[DataFrame] = [
        _changed_values(valid, table_name, "order_id") for table_name in sorted(DIRECT_ORDER_TABLES)
    ]

    orders = read_layer_table(spark, config, "silver", "orders").alias("o")
    items = read_layer_table(spark, config, "silver", "order_items").alias("oi")

    customer_type2_events = _events_with_changed_fields(valid, "app_users", CUSTOMER_TYPE2_FIELDS)
    users = _changed_values(customer_type2_events, "app_users", "user_id").alias("c")
    impacted.append(users.join(orders, F.col("c.user_id") == F.col("o.customer_id")).select("o.order_id"))

    addresses = _changed_values(valid, "user_addresses", "address_id").alias("c")
    impacted.append(
        addresses.join(
            orders,
            (F.col("c.address_id") == F.col("o.shipping_address_id"))
            | (F.col("c.address_id") == F.col("o.billing_address_id")),
        ).select("o.order_id")
    )

    shops = _changed_values(valid, "shops", "shop_id").alias("c")
    impacted.append(shops.join(items, F.col("c.shop_id") == F.col("oi.shop_id")).select("oi.order_id"))

    products = read_layer_table(spark, config, "silver", "products").alias("p")
    categories = _changed_values(valid, "categories", "category_id").alias("c")
    impacted.append(
        categories.join(products, F.col("c.category_id") == F.col("p.category_id"))
        .join(items, F.col("p.product_id") == F.col("oi.product_id"))
        .select("oi.order_id")
    )

    changed_products = _changed_values(valid, "products", "product_id").alias("c")
    impacted.append(
        changed_products.join(items, F.col("c.product_id") == F.col("oi.product_id")).select("oi.order_id")
    )

    variants = _changed_values(valid, "product_variants", "product_variant_id").alias("c")
    impacted.append(
        variants.join(items, F.col("c.product_variant_id") == F.col("oi.product_variant_id")).select("oi.order_id")
    )

    order_vouchers = read_layer_table(spark, config, "silver", "order_vouchers").alias("ov")
    vouchers = _changed_values(valid, "vouchers", "voucher_id").alias("c")
    impacted.append(
        vouchers.join(order_vouchers, F.col("c.voucher_id") == F.col("ov.voucher_id")).select("ov.order_id")
    )

    result = impacted[0].select(F.col("order_id").cast("long").alias("order_id"))
    for candidate in impacted[1:]:
        result = result.unionByName(candidate.select(F.col("order_id").cast("long").alias("order_id")))
    return result.where(F.col("order_id").isNotNull()).distinct()


def run_gold_incremental(config: AppConfig, spark: SparkSession, events: DataFrame) -> None:
    """MERGE dimensions and replace facts only for orders affected by this CDC batch."""
    valid = events.where(F.col("is_valid_event") == F.lit(True))
    if valid.isEmpty():
        return

    changed_sources = {row["table_name"] for row in valid.select("table_name").distinct().collect()}
    changed_dimensions: set[str] = set()
    for source_table in changed_sources:
        changed_dimensions.update(SOURCE_DIMENSIONS.get(source_table, set()))

    if "dim_customer" in changed_dimensions:
        changed_customer_ids = _changed_values(valid, "app_users", "user_id")
        customer_users = read_layer_table(spark, config, "silver", "app_users").join(
            changed_customer_ids, "user_id", "left_semi"
        )
        scd2_merge(
            spark=spark,
            source_df=build_dim_customer(customer_users),
            path=config.lakehouse.table_path("gold", "dim_customer"),
            natural_keys=["source_customer_id"],
            tracked_hash_column="scd_hash",
            effective_timestamp_col="source_updated_at",
            surrogate_key_col="customer_key",
            type1_columns=["last_login_at"],
        )

    affected_orders = impacted_order_ids(config, spark, valid).cache()
    customer_history = read_layer_table(spark, config, "gold", "dim_customer")
    gold_tables = build_gold_tables(config, spark, customer_history=customer_history)
    affected_facts = gold_tables["fact_sales"].join(
        affected_orders.select(F.col("order_id").alias("source_order_id")),
        "source_order_id",
        "inner",
    )

    for table_name, keys in DIMENSION_MERGE_KEYS.items():
        if table_name not in changed_dimensions:
            continue
        dimension = _impacted_dimension(table_name, gold_tables[table_name], valid, affected_facts)
        upsert_to_delta(
            spark,
            dimension,
            config.lakehouse.table_path("gold", table_name),
            keys,
        )

    if not affected_orders.isEmpty():
        fact_scope = affected_orders.select(F.col("order_id").alias("source_order_id"))
        replace_delta_scope(
            spark=spark,
            df=affected_facts,
            path=config.lakehouse.table_path("gold", "fact_sales"),
            scope_df=fact_scope,
            scope_key="source_order_id",
            keys=["source_order_id", "source_order_item_id"],
        )
    affected_orders.unpersist()


def _impacted_dimension(
    table_name: str,
    dimension: DataFrame,
    events: DataFrame,
    affected_facts: DataFrame,
) -> DataFrame:
    source_scopes = {
        "dim_location": ("user_addresses", "address_id", "source_address_id"),
        "dim_shop": ("shops", "shop_id", "source_shop_id"),
        "dim_category": ("categories", "category_id", "source_category_id"),
    }
    if table_name in source_scopes:
        source_table, source_key, dimension_key = source_scopes[table_name]
        changed = _changed_values(events, source_table, source_key).select(
            F.col(source_key).alias(dimension_key)
        )
        return dimension.join(changed, dimension_key, "left_semi")

    if table_name == "dim_product":
        product_ids = _changed_values(events, "products", "product_id").select(
            F.col("product_id").alias("source_product_id")
        )
        variant_ids = _changed_values(events, "product_variants", "product_variant_id").select(
            F.col("product_variant_id").alias("source_product_variant_id")
        )
        return dimension.join(product_ids, "source_product_id", "left_semi").unionByName(
            dimension.join(variant_ids, "source_product_variant_id", "left_semi")
        ).dropDuplicates(["source_product_variant_id"])

    fact_keys = {
        "dim_date": ("order_date_key", "date_key"),
        "dim_time": ("order_time_key", "time_key"),
        "dim_promotion": ("promotion_key", "promotion_key"),
        "dim_payment": ("payment_key", "payment_key"),
        "dim_shipping": ("shipping_key", "shipping_key"),
    }
    fact_key, dimension_key = fact_keys[table_name]
    keys = affected_facts.where(F.col(fact_key).isNotNull()).select(F.col(fact_key).alias(dimension_key)).distinct()
    return dimension.join(keys, dimension_key, "left_semi")
