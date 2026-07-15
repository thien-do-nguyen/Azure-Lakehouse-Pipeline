from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecommerce_pipeline.config import load_config
from ecommerce_pipeline.io import read_layer_table
from ecommerce_pipeline.spark import build_spark


@dataclass(frozen=True)
class ValidationResult:
    name: str
    passed: bool
    expected: str
    actual: str


def _count(df: DataFrame) -> int:
    return int(df.count())


def _active_bronze(df: DataFrame) -> DataFrame:
    if "is_deleted" not in df.columns:
        return df
    return df.where(F.coalesce(F.col("is_deleted"), F.lit(False)) == F.lit(False))


def _duplicate_count(df: DataFrame, keys: list[str]) -> int:
    return int(df.groupBy(*keys).count().where(F.col("count") > 1).count())


def _null_count(df: DataFrame, columns: list[str]) -> int:
    condition = None
    for column in columns:
        expression = F.col(column).isNull()
        condition = expression if condition is None else condition | expression
    return int(df.where(condition).count()) if condition is not None else 0


def _orphan_count(
    fact: DataFrame,
    dimension: DataFrame,
    fact_key: str,
    dimension_key: str,
    allow_unknown_zero: bool = False,
) -> int:
    candidates = fact.select(F.col(fact_key).alias("foreign_key")).where(F.col("foreign_key").isNotNull()).distinct()
    if allow_unknown_zero:
        candidates = candidates.where(F.col("foreign_key") != F.lit(0))
    dimension_keys = dimension.select(F.col(dimension_key).alias("dimension_key")).distinct()
    return int(candidates.join(dimension_keys, F.col("foreign_key") == F.col("dimension_key"), "left_anti").count())


def _optional_count(spark, config, layer: str, table_name: str, condition=None) -> int:
    try:
        df = read_layer_table(spark, config, layer, table_name)
    except Exception:
        return 0
    if condition is not None:
        df = df.where(condition)
    return _count(df)


def _sum_decimal(df: DataFrame, column: str) -> Decimal:
    value = df.agg(F.sum(F.col(column)).alias("value")).collect()[0]["value"]
    return Decimal("0") if value is None else Decimal(str(value))


def _money_close(left: Decimal, right: Decimal, tolerance: Decimal = Decimal("0.01")) -> bool:
    return abs(left - right) <= tolerance


def _result(name: str, passed: bool, expected: object, actual: object) -> ValidationResult:
    return ValidationResult(name=name, passed=passed, expected=str(expected), actual=str(actual))


def run_validations(config, spark) -> list[ValidationResult]:
    bronze_orders = read_layer_table(spark, config, "bronze", "orders")
    bronze_order_items = read_layer_table(spark, config, "bronze", "order_items")
    silver_users = read_layer_table(spark, config, "silver", "app_users")
    silver_orders = read_layer_table(spark, config, "silver", "orders")
    silver_order_items = read_layer_table(spark, config, "silver", "order_items")
    silver_payments = read_layer_table(spark, config, "silver", "payments")
    silver_shipments = read_layer_table(spark, config, "silver", "shipments")
    gold_fact_sales = read_layer_table(spark, config, "gold", "fact_sales")
    gold_dim_customer = read_layer_table(spark, config, "gold", "dim_customer")
    gold_dim_product = read_layer_table(spark, config, "gold", "dim_product")
    gold_dimensions = {
        "customer_key": (gold_dim_customer, "customer_key", False),
        "product_key": (gold_dim_product, "product_key", False),
        "shop_key": (read_layer_table(spark, config, "gold", "dim_shop"), "shop_key", False),
        "category_key": (read_layer_table(spark, config, "gold", "dim_category"), "category_key", False),
        "ship_to_location_key": (
            read_layer_table(spark, config, "gold", "dim_location"),
            "location_key",
            False,
        ),
        "bill_to_location_key": (
            read_layer_table(spark, config, "gold", "dim_location"),
            "location_key",
            False,
        ),
        "order_date_key": (read_layer_table(spark, config, "gold", "dim_date"), "date_key", False),
        "order_time_key": (read_layer_table(spark, config, "gold", "dim_time"), "time_key", False),
        "promotion_key": (
            read_layer_table(spark, config, "gold", "dim_promotion"),
            "promotion_key",
            True,
        ),
        "payment_key": (read_layer_table(spark, config, "gold", "dim_payment"), "payment_key", True),
        "shipping_key": (read_layer_table(spark, config, "gold", "dim_shipping"), "shipping_key", True),
    }

    results: list[ValidationResult] = []

    bronze_order_count = _count(_active_bronze(bronze_orders))
    silver_order_count = _count(silver_orders)
    results.append(
        _result(
            "bronze_to_silver_orders_count",
            bronze_order_count == silver_order_count,
            bronze_order_count,
            silver_order_count,
        )
    )

    bronze_item_count = _count(_active_bronze(bronze_order_items))
    silver_item_count = _count(silver_order_items)
    fact_count = _count(gold_fact_sales)
    results.append(
        _result(
            "bronze_to_silver_order_items_count",
            bronze_item_count == silver_item_count,
            bronze_item_count,
            silver_item_count,
        )
    )
    results.append(
        _result(
            "silver_order_items_to_fact_sales_count", silver_item_count == fact_count, silver_item_count, fact_count
        )
    )

    source_order_count = int(silver_order_items.select("order_id").distinct().count())
    fact_order_count = int(gold_fact_sales.select("source_order_id").distinct().count())
    results.append(
        _result(
            "distinct_orders_reconciled", source_order_count == fact_order_count, source_order_count, fact_order_count
        )
    )

    fact_duplicate_count = _duplicate_count(gold_fact_sales, ["source_order_id", "source_order_item_id"])
    results.append(_result("fact_sales_business_key_unique", fact_duplicate_count == 0, 0, fact_duplicate_count))

    critical_fact_nulls = _null_count(
        gold_fact_sales,
        [
            "order_date_key",
            "order_time_key",
            "customer_key",
            "product_key",
            "shop_key",
            "category_key",
            "ship_to_location_key",
            "bill_to_location_key",
            "source_order_id",
            "source_order_item_id",
        ],
    )
    results.append(_result("fact_sales_critical_keys_not_null", critical_fact_nulls == 0, 0, critical_fact_nulls))

    silver_gross_sales = _sum_decimal(silver_order_items, "item_subtotal")
    fact_gross_sales = _sum_decimal(gold_fact_sales, "gross_sales_amount")
    results.append(
        _result(
            "gross_sales_reconciled",
            _money_close(silver_gross_sales, fact_gross_sales),
            silver_gross_sales,
            fact_gross_sales,
        )
    )

    silver_tax = _sum_decimal(silver_order_items, "tax_amount")
    fact_tax = _sum_decimal(gold_fact_sales, "tax_amount")
    results.append(_result("tax_amount_reconciled", _money_close(silver_tax, fact_tax), silver_tax, fact_tax))

    customer_count = _count(silver_users)
    current_customers = gold_dim_customer.where(F.col("is_current") == F.lit(True))
    dim_customer_count = _count(current_customers)
    results.append(
        _result(
            "dim_customer_count_reconciled", customer_count == dim_customer_count, customer_count, dim_customer_count
        )
    )

    customer_current_duplicates = int(
        gold_dim_customer.groupBy("source_customer_id")
        .agg(F.sum(F.col("is_current").cast("int")).alias("current_count"))
        .where(F.col("current_count") != 1)
        .count()
    )
    results.append(
        _result("dim_customer_exactly_one_current", customer_current_duplicates == 0, 0, customer_current_duplicates)
    )
    customer_interval_overlaps = int(
        gold_dim_customer.alias("left")
        .join(
            gold_dim_customer.alias("right"),
            (F.col("left.source_customer_id") == F.col("right.source_customer_id"))
            & (F.col("left.customer_key") < F.col("right.customer_key"))
            & (F.col("left.start_date") < F.col("right.end_date"))
            & (F.col("right.start_date") < F.col("left.end_date")),
            "inner",
        )
        .count()
    )
    results.append(
        _result("dim_customer_intervals_do_not_overlap", customer_interval_overlaps == 0, 0, customer_interval_overlaps)
    )

    for name, df, keys in [
        ("dim_customer_surrogate_key_unique", gold_dim_customer, ["customer_key"]),
        ("dim_customer_current_business_key_unique", current_customers, ["source_customer_id"]),
        ("dim_product_business_key_unique", gold_dim_product, ["source_product_variant_id"]),
    ]:
        duplicates = _duplicate_count(df, keys)
        nulls = _null_count(df, keys)
        results.append(_result(name, duplicates == 0, 0, duplicates))
        results.append(_result(f"{name}_not_null", nulls == 0, 0, nulls))

    for fact_key, (dimension, dimension_key, allow_zero) in gold_dimensions.items():
        orphans = _orphan_count(gold_fact_sales, dimension, fact_key, dimension_key, allow_zero)
        results.append(_result(f"fact_sales_{fact_key}_orphan_count", orphans == 0, 0, orphans))

    source_latest_row = silver_orders.agg(F.max("created_at").alias("value")).first()
    gold_latest_row = gold_fact_sales.agg(F.max("updated_at").alias("value")).first()
    source_latest = source_latest_row["value"] if source_latest_row is not None else None
    gold_latest = gold_latest_row["value"] if gold_latest_row is not None else None
    freshness_ok = source_latest is None or (gold_latest is not None and gold_latest >= source_latest)
    results.append(_result("fact_sales_freshness", freshness_ok, f">={source_latest}", gold_latest))

    dlq_count = _optional_count(spark, config, "bronze", "cdc_dead_letters")
    results.append(_result("cdc_dead_letter_count", dlq_count == 0, 0, dlq_count))
    failed_batches = _optional_count(
        spark,
        config,
        "_control",
        "cdc_batch_commits",
        F.col("status") == F.lit("FAILED"),
    )
    results.append(_result("cdc_failed_batch_count", failed_batches == 0, 0, failed_batches))

    product_variant_count = int(
        read_layer_table(spark, config, "silver", "product_variants").select("product_variant_id").distinct().count()
    )
    dim_product_count = _count(gold_dim_product)
    results.append(
        _result(
            "dim_product_variant_count_reconciled",
            product_variant_count == dim_product_count,
            product_variant_count,
            dim_product_count,
        )
    )

    for name, df, keys in [
        ("silver_orders_key_unique", silver_orders, ["order_id"]),
        ("silver_order_items_key_unique", silver_order_items, ["order_item_id"]),
        ("silver_payments_key_unique", silver_payments, ["payment_id"]),
        ("silver_shipments_key_unique", silver_shipments, ["shipment_id"]),
    ]:
        duplicates = _duplicate_count(df, keys)
        nulls = _null_count(df, keys)
        results.append(_result(name, duplicates == 0, 0, duplicates))
        results.append(_result(f"{name}_not_null", nulls == 0, 0, nulls))

    return results


def print_results(results: list[ValidationResult]) -> None:
    print(f"{'STATUS':7} {'CHECK':42} {'EXPECTED':24} ACTUAL")
    print("-" * 110)
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"{status:7} {result.name:42} {result.expected[:24]:24} {result.actual}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate and reconcile lakehouse output.")
    parser.add_argument("--config", default="configs/local.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    spark = build_spark(config)
    try:
        results = run_validations(config, spark)
        print_results(results)
        if any(not result.passed for result in results):
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
