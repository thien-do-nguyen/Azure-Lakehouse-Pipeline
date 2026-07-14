from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.io import read_layer_table


def _keyed(df: DataFrame, key_name: str, order_columns: list[str]) -> DataFrame:
    if key_name == "sales_key" and "source_order_item_id" in df.columns:
        return df.withColumn(key_name, F.col("source_order_item_id").cast("long"))

    if len(order_columns) == 1:
        source_key = F.col(order_columns[0]).cast("long")
        return df.withColumn(key_name, F.coalesce(source_key, _stable_hash(*order_columns)))

    return df.withColumn(key_name, _stable_hash(*order_columns))


def _stable_hash(*columns: str):
    return (
        F.pmod(F.xxhash64(*[F.col(column).cast("string") for column in columns]), F.lit(9223372036854775806)) + F.lit(1)
    ).cast("long")


def _natural_hash(*columns: str):
    return F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(column).cast("string"), F.lit("NA")) for column in columns]), 256
    )


def _date_key(column_name: str):
    return F.date_format(F.to_date(F.col(column_name)), "yyyyMMdd").cast("int")


def _time_key(column_name: str):
    return F.date_format(F.col(column_name), "HHmmss").cast("int")


def _season(month_col):
    return (
        F.when(month_col.isin(12, 1, 2), F.lit("Winter"))
        .when(month_col.isin(3, 4, 5), F.lit("Spring"))
        .when(month_col.isin(6, 7, 8), F.lit("Summer"))
        .otherwise(F.lit("Autumn"))
    )


def _payment_method_group(column_name: str):
    value = F.lower(F.col(column_name))
    return (
        F.when(value.isin("visa", "mastercard", "credit_card", "debit_card"), F.lit("card"))
        .when(value.isin("momo", "zalopay", "vnpay", "paypal"), F.lit("wallet"))
        .when(value.isin("cod", "cash_on_delivery"), F.lit("cod"))
        .otherwise(F.lit("other"))
    )


def _read_gold_source_table(spark, config: AppConfig, table_name: str) -> DataFrame:
    return read_layer_table(spark, config, "silver", table_name)


def build_dim_customer(users: DataFrame) -> DataFrame:
    return users.select(
        F.col("user_id").alias("source_customer_id"),
        F.col("public_user_id").alias("public_customer_id"),
        "username",
        "email",
        "first_name",
        "last_name",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "phone_number",
        F.col("status").cast("string").alias("customer_status"),
        F.col("created_at").alias("registered_at"),
        F.col("updated_at").alias("source_updated_at"),
        F.col("last_login").alias("last_login_at"),
        F.lit(True).alias("is_current"),
        F.col("created_at").cast("timestamp").alias("start_date"),
        F.lit("9999-12-31 00:00:00").cast("timestamp").alias("end_date"),
        _natural_hash("username", "email", "first_name", "last_name", "phone_number", "status").alias("scd_hash"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )


def build_gold_tables(
    config: AppConfig,
    spark,
    customer_history: DataFrame | None = None,
) -> dict[str, DataFrame]:
    users = _read_gold_source_table(spark, config, "app_users")
    addresses = _read_gold_source_table(spark, config, "user_addresses")
    shops = _read_gold_source_table(spark, config, "shops")
    categories = _read_gold_source_table(spark, config, "categories")
    products = _read_gold_source_table(spark, config, "products")
    variants = _read_gold_source_table(spark, config, "product_variants")
    vouchers = _read_gold_source_table(spark, config, "vouchers")
    orders = _read_gold_source_table(spark, config, "orders")
    order_items = _read_gold_source_table(spark, config, "order_items")
    order_vouchers = _read_gold_source_table(spark, config, "order_vouchers")
    payments = _read_gold_source_table(spark, config, "payments")
    shipments = _read_gold_source_table(spark, config, "shipments")

    dates = orders.select(F.to_date("created_at").alias("full_date")).distinct()
    dim_date = dates.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.dayofweek("full_date").cast("smallint").alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofmonth("full_date").cast("smallint").alias("day_of_month"),
        F.dayofyear("full_date").cast("smallint").alias("day_of_year"),
        F.weekofyear("full_date").cast("smallint").alias("week_of_year"),
        F.month("full_date").cast("smallint").alias("month_number"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.quarter("full_date").cast("smallint").alias("quarter_number"),
        F.year("full_date").cast("smallint").alias("year_number"),
        (F.dayofweek("full_date").isin(1, 7)).alias("is_weekend"),
        (F.last_day("full_date") == F.col("full_date")).alias("is_month_end"),
        _season(F.month("full_date")).alias("season_name"),
        F.year("full_date").cast("smallint").alias("fiscal_year"),
        F.quarter("full_date").cast("smallint").alias("fiscal_quarter"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )

    times = orders.select(F.date_format("created_at", "HH:mm:ss").alias("full_time")).distinct()
    dim_time = times.select(
        F.regexp_replace("full_time", ":", "").cast("int").alias("time_key"),
        F.col("full_time").cast("string").alias("full_time"),
        F.substring("full_time", 1, 2).cast("smallint").alias("hour_24"),
        F.substring("full_time", 4, 2).cast("smallint").alias("minute_number"),
        F.substring("full_time", 7, 2).cast("smallint").alias("second_number"),
        F.when(F.substring("full_time", 1, 2).cast("int") < 12, F.lit("AM")).otherwise(F.lit("PM")).alias("am_pm"),
        F.when(F.substring("full_time", 1, 2).cast("int").between(5, 11), F.lit("morning"))
        .when(F.substring("full_time", 1, 2).cast("int").between(12, 16), F.lit("afternoon"))
        .when(F.substring("full_time", 1, 2).cast("int").between(17, 20), F.lit("evening"))
        .otherwise(F.lit("night"))
        .alias("day_part"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )

    dim_customer = build_dim_customer(users)
    fact_customer_dimension = (
        customer_history
        if customer_history is not None
        else dim_customer.withColumn(
            "customer_key",
            _stable_hash("source_customer_id", "scd_hash", "start_date"),
        )
    )

    dim_location = _keyed(
        addresses.withColumn(
            "natural_location_hash",
            _natural_hash("address_id", "user_id", "street", "ward", "district", "city", "country"),
        ).select(
            F.col("address_id").alias("source_address_id"),
            F.col("user_id").alias("source_customer_id"),
            "natural_location_hash",
            F.col("address_type").cast("string").alias("address_type"),
            "recipient_name",
            "phone_number",
            "street",
            "ward",
            "district",
            "city",
            "state",
            "postal_code",
            "country",
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        ),
        "location_key",
        ["source_address_id"],
    )

    dim_shop = _keyed(
        shops.select(
            F.col("shop_id").alias("source_shop_id"),
            F.col("public_shop_id"),
            "shop_name",
            "shop_slug",
            F.col("status").cast("string").alias("shop_status"),
            F.col("created_at").alias("shop_created_at"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        ),
        "shop_key",
        ["source_shop_id"],
    )

    parent_categories = categories.select(
        F.col("category_id").alias("parent_id"),
        F.col("category_name").alias("parent_category_name"),
    )
    dim_category = _keyed(
        categories.join(parent_categories, categories.parent_category_id == parent_categories.parent_id, "left").select(
            F.col("category_id").alias("source_category_id"),
            F.col("parent_category_id").alias("source_parent_category_id"),
            "category_name",
            F.col("slug").alias("category_slug"),
            "parent_category_name",
            "is_active",
            F.col("created_at").alias("category_created_at"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        ),
        "category_key",
        ["source_category_id"],
    )

    dim_product = _keyed(
        products.alias("p")
        .join(variants.alias("v"), "product_id")
        .select(
            F.col("p.product_id").alias("source_product_id"),
            F.col("v.product_variant_id").alias("source_product_variant_id"),
            F.col("p.public_product_id").alias("public_product_id"),
            F.col("v.public_variant_id").alias("public_variant_id"),
            F.col("p.product_sku").alias("product_sku"),
            F.col("p.product_slug").alias("product_slug"),
            F.col("p.product_name").alias("product_name"),
            F.col("p.brand").alias("brand"),
            F.col("p.status").cast("string").alias("product_status"),
            F.col("p.is_featured").alias("is_featured"),
            F.col("v.variant_sku").alias("variant_sku"),
            F.col("v.variant_name").alias("variant_name"),
            F.col("v.status").cast("string").alias("variant_status"),
            F.col("v.options_json").alias("variant_options_json"),
            F.col("v.is_default").alias("is_default_variant"),
            F.col("v.unit_price").alias("current_unit_price"),
            F.col("v.compare_at_price").alias("compare_at_price"),
            F.col("v.currency").alias("currency"),
            F.col("v.stock_quantity").alias("stock_quantity"),
            F.col("v.reserved_quantity").alias("reserved_quantity"),
            F.col("v.weight_kg").alias("weight_kg"),
            F.col("p.attributes_json").alias("product_attributes_json"),
            F.col("p.images_json").alias("product_images_json"),
            F.col("v.images_json").alias("variant_images_json"),
            F.col("p.created_at").alias("product_created_at"),
            F.col("v.created_at").alias("variant_created_at"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        ),
        "product_key",
        ["source_product_variant_id"],
    )

    voucher_rollup = (
        order_vouchers.join(vouchers, "voucher_id", "left")
        .groupBy("order_id")
        .agg(
            F.count("voucher_id").cast("int").alias("voucher_count"),
            F.concat_ws(",", F.sort_array(F.collect_set("voucher_code"))).alias("voucher_codes"),
            F.concat_ws(",", F.sort_array(F.collect_set("voucher_name"))).alias("voucher_names"),
            F.concat_ws(",", F.sort_array(F.collect_set(F.col("discount_type").cast("string")))).alias(
                "discount_types"
            ),
            F.first(F.col("scope_json").cast("string"), ignorenulls=True).alias("promotion_scope"),
            F.min("starts_at").alias("promotion_start_at"),
            F.max("ends_at").alias("promotion_end_at"),
            F.min("minimum_order_amount").alias("minimum_order_amount"),
            F.max(F.col("is_active").cast("int")).cast("boolean").alias("is_active"),
        )
        .withColumn("promotion_type", F.lit("voucher"))
    )
    no_voucher = orders.join(voucher_rollup.select("order_id"), "order_id", "left_anti").select(
        "order_id",
        F.lit(0).cast("int").alias("voucher_count"),
        F.lit("NO_VOUCHER").alias("voucher_codes"),
        F.lit(None).cast("string").alias("voucher_names"),
        F.lit(None).cast("string").alias("discount_types"),
        F.lit("none").alias("promotion_scope"),
        F.lit(None).cast("timestamp").alias("promotion_start_at"),
        F.lit(None).cast("timestamp").alias("promotion_end_at"),
        F.lit(None).cast("decimal(12,2)").alias("minimum_order_amount"),
        F.lit(False).alias("is_active"),
        F.lit("none").alias("promotion_type"),
    )
    promotion_by_order = voucher_rollup.unionByName(no_voucher).withColumn(
        "natural_promotion_hash",
        _natural_hash("promotion_type", "voucher_codes", "discount_types", "promotion_scope"),
    )
    dim_promotion = _keyed(
        promotion_by_order.drop("order_id")
        .dropDuplicates(["natural_promotion_hash"])
        .select(
            "natural_promotion_hash",
            "promotion_type",
            "voucher_count",
            "voucher_codes",
            "voucher_names",
            "discount_types",
            "promotion_scope",
            "promotion_start_at",
            "promotion_end_at",
            "minimum_order_amount",
            "is_active",
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        ),
        "promotion_key",
        ["natural_promotion_hash"],
    )

    payment_by_order = payments.withColumn(
        "natural_payment_hash", _natural_hash("payment_method", "payment_status")
    ).select(
        "order_id",
        "natural_payment_hash",
        "payment_method",
        _payment_method_group("payment_method").alias("payment_method_group"),
        F.col("payment_status").cast("string").alias("payment_status"),
        (F.col("payment_status").cast("string") == F.lit("paid")).alias("paid_flag"),
    )
    dim_payment = _keyed(
        payment_by_order.drop("order_id")
        .dropDuplicates(["natural_payment_hash"])
        .select(
            "natural_payment_hash",
            "payment_method",
            "payment_method_group",
            "payment_status",
            "paid_flag",
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        ),
        "payment_key",
        ["natural_payment_hash"],
    )

    shipping_by_order = shipments.withColumn(
        "natural_shipping_hash", _natural_hash("carrier", "shipment_status")
    ).select(
        "order_id",
        "natural_shipping_hash",
        "carrier",
        F.col("shipment_status").cast("string").alias("shipment_status"),
        F.col("shipped_at").isNotNull().alias("shipped_flag"),
        F.col("delivered_at").isNotNull().alias("delivered_flag"),
    )
    dim_shipping = _keyed(
        shipping_by_order.drop("order_id")
        .dropDuplicates(["natural_shipping_hash"])
        .select(
            "natural_shipping_hash",
            "carrier",
            "shipment_status",
            "shipped_flag",
            "delivered_flag",
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        ),
        "shipping_key",
        ["natural_shipping_hash"],
    )

    customer_by_order = (
        orders.alias("customer_order")
        .join(
            fact_customer_dimension.select(
                "customer_key", "source_customer_id", "start_date", "end_date"
            ).alias("customer_version"),
            (F.col("customer_order.customer_id") == F.col("customer_version.source_customer_id"))
            & (F.col("customer_order.created_at") >= F.col("customer_version.start_date"))
            & (F.col("customer_order.created_at") < F.col("customer_version.end_date")),
            "left",
        )
        .select(
            F.col("customer_order.order_id").alias("order_id"),
            F.col("customer_version.customer_key").alias("customer_key"),
            F.row_number()
            .over(
                Window.partitionBy(F.col("customer_order.order_id")).orderBy(
                    F.col("customer_version.start_date").desc_nulls_last(),
                    F.col("customer_version.customer_key").desc_nulls_last(),
                )
            )
            .alias("_customer_version_rank"),
        )
        .where(F.col("_customer_version_rank") == 1)
        .drop("_customer_version_rank")
    )

    item_totals = order_items.groupBy("order_id").agg(F.sum("item_subtotal").alias("order_item_subtotal"))
    fact_base = (
        order_items.alias("oi")
        .join(orders.alias("o"), "order_id")
        .join(item_totals, "order_id")
        .join(
            products.select("product_id", "category_id").alias("p"),
            F.col("oi.product_id") == F.col("p.product_id"),
            "left",
        )
        .join(customer_by_order, "order_id", "left")
        .join(
            dim_product.select("product_key", "source_product_variant_id"),
            F.col("oi.product_variant_id") == F.col("source_product_variant_id"),
            "left",
        )
        .join(dim_shop.select("shop_key", "source_shop_id"), F.col("oi.shop_id") == F.col("source_shop_id"), "left")
        .join(
            dim_category.select("category_key", "source_category_id"),
            F.col("p.category_id") == F.col("source_category_id"),
            "left",
        )
        .join(promotion_by_order.select("order_id", "natural_promotion_hash").alias("pbo"), "order_id", "left")
        .join(dim_promotion.select("promotion_key", "natural_promotion_hash"), "natural_promotion_hash", "left")
        .join(payment_by_order.select("order_id", "natural_payment_hash").alias("payo"), "order_id", "left")
        .join(dim_payment.select("payment_key", "natural_payment_hash"), "natural_payment_hash", "left")
        .join(shipping_by_order.select("order_id", "natural_shipping_hash").alias("ship"), "order_id", "left")
        .join(dim_shipping.select("shipping_key", "natural_shipping_hash"), "natural_shipping_hash", "left")
        .join(
            dim_location.select(
                F.col("location_key").alias("ship_to_location_key"),
                F.col("source_address_id").alias("ship_address_id"),
            ),
            F.col("o.shipping_address_id") == F.col("ship_address_id"),
            "left",
        )
        .join(
            dim_location.select(
                F.col("location_key").alias("bill_to_location_key"),
                F.col("source_address_id").alias("bill_address_id"),
            ),
            F.col("o.billing_address_id") == F.col("bill_address_id"),
            "left",
        )
    )

    ratio = F.when(
        F.col("order_item_subtotal") > 0, F.col("oi.item_subtotal") / F.col("order_item_subtotal")
    ).otherwise(F.lit(0))
    order_discount_allocated = F.round(F.col("o.discount_amount") * ratio, 2)
    shipping_allocated = F.round(F.col("o.shipping_amount") * ratio, 2)
    fact_sales = _keyed(
        fact_base.select(
            _date_key("o.created_at").alias("order_date_key"),
            _time_key("o.created_at").alias("order_time_key"),
            F.col("customer_key"),
            F.col("product_key"),
            F.col("shop_key"),
            F.col("category_key"),
            F.coalesce(F.col("promotion_key"), F.lit(0)).alias("promotion_key"),
            F.coalesce(F.col("payment_key"), F.lit(0)).alias("payment_key"),
            F.coalesce(F.col("shipping_key"), F.lit(0)).alias("shipping_key"),
            F.col("ship_to_location_key"),
            F.col("bill_to_location_key"),
            F.col("o.order_id").alias("source_order_id"),
            F.col("oi.order_item_id").alias("source_order_item_id"),
            "order_number",
            F.col("o.order_status").cast("string").alias("order_status"),
            F.col("o.payment_status").cast("string").alias("payment_status"),
            F.col("o.customer_id").alias("source_customer_id"),
            F.col("oi.product_id").alias("source_product_id"),
            F.col("oi.product_variant_id").alias("source_product_variant_id"),
            F.col("oi.shop_id").alias("source_shop_id"),
            F.col("p.category_id").alias("source_category_id"),
            F.col("oi.currency").alias("currency"),
            F.col("oi.quantity").alias("quantity"),
            F.col("oi.unit_price").alias("unit_price_amount"),
            F.col("oi.item_subtotal").alias("gross_sales_amount"),
            F.col("oi.discount_amount").alias("line_discount_amount"),
            order_discount_allocated.alias("order_discount_amount_allocated"),
            (F.col("oi.discount_amount") + order_discount_allocated).alias("total_discount_amount"),
            F.col("oi.tax_amount").alias("tax_amount"),
            shipping_allocated.alias("shipping_amount_allocated"),
            (F.col("oi.item_total") - order_discount_allocated + shipping_allocated).alias("net_sales_amount"),
            F.col("o.created_at").alias("order_created_at"),
            F.col("oi.created_at").alias("order_item_created_at"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        ),
        "sales_key",
        ["source_order_id", "source_order_item_id"],
    )

    return {
        "dim_date": dim_date,
        "dim_time": dim_time,
        "dim_customer": dim_customer,
        "dim_location": dim_location,
        "dim_shop": dim_shop,
        "dim_category": dim_category,
        "dim_product": dim_product,
        "dim_promotion": dim_promotion,
        "dim_payment": dim_payment,
        "dim_shipping": dim_shipping,
        "fact_sales": fact_sales,
    }
