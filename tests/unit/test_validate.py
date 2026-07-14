from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from ecommerce_pipeline.jobs import validate
from ecommerce_pipeline.jobs.validate import _duplicate_count, _money_close, _null_count, _orphan_count


def test_validation_helpers(spark) -> None:
    df = spark.createDataFrame([(1, "x"), (1, None), (2, "z")], ["id", "value"])

    assert _duplicate_count(df, ["id"]) == 1
    assert _null_count(df, ["value"]) == 1
    assert _money_close(10, 10)
    assert not _money_close(10, 11)

    fact = spark.createDataFrame([(1,), (2,), (0,)], ["customer_key"])
    dimension = spark.createDataFrame([(1,)], ["customer_key"])
    assert _orphan_count(fact, dimension, "customer_key", "customer_key") == 2
    assert _orphan_count(fact, dimension, "customer_key", "customer_key", allow_unknown_zero=True) == 1


def test_active_bronze_excludes_soft_delete_tombstones(spark) -> None:
    df = spark.createDataFrame([(1, False), (2, True), (3, None)], ["id", "is_deleted"])

    assert [row["id"] for row in validate._active_bronze(df).orderBy("id").collect()] == [1, 3]


def test_run_validations_returns_reconciliation_results(monkeypatch, spark, local_config) -> None:
    tables = {
        ("bronze", "orders"): spark.createDataFrame([(100,), (101,)], ["order_id"]),
        ("bronze", "order_items"): spark.createDataFrame([(1000,), (1001,)], ["order_item_id"]),
        ("silver", "app_users"): spark.createDataFrame([(1,), (2,)], ["user_id"]),
        ("silver", "orders"): spark.createDataFrame(
            [(100, datetime(2026, 1, 1)), (101, datetime(2026, 1, 2))],
            ["order_id", "created_at"],
        ),
        ("silver", "order_items"): spark.createDataFrame(
            [
                (1000, 100, Decimal("10.00"), Decimal("1.00")),
                (1001, 101, Decimal("5.00"), Decimal("0.50")),
            ],
            ["order_item_id", "order_id", "item_subtotal", "tax_amount"],
        ),
        ("silver", "payments"): spark.createDataFrame([(2000,), (2001,)], ["payment_id"]),
        ("silver", "shipments"): spark.createDataFrame([(3000,), (3001,)], ["shipment_id"]),
        ("silver", "product_variants"): spark.createDataFrame([(10,), (11,)], ["product_variant_id"]),
        ("gold", "fact_sales"): spark.createDataFrame(
            [
                (100, 1000, 20260101, 120000, 1, 10, 1, 1, 1, 1, 0, 0, 0, Decimal("10.00"), Decimal("1.00"), "2026-01-03"),
                (101, 1001, 20260101, 120000, 2, 11, 1, 1, 1, 1, 0, 0, 0, Decimal("5.00"), Decimal("0.50"), "2026-01-03"),
            ],
            [
                "source_order_id",
                "source_order_item_id",
                "order_date_key",
                "order_time_key",
                "customer_key",
                "product_key",
                "shop_key",
                "category_key",
                "ship_to_location_key",
                "bill_to_location_key",
                "promotion_key",
                "payment_key",
                "shipping_key",
                "gross_sales_amount",
                "tax_amount",
                "updated_at",
            ],
        ).withColumn("updated_at", validate.F.col("updated_at").cast("timestamp")),
        ("gold", "dim_customer"): spark.createDataFrame(
            [
                (1, 1, True, datetime(2026, 1, 1), datetime(9999, 12, 31)),
                (2, 2, True, datetime(2026, 1, 1), datetime(9999, 12, 31)),
            ],
            ["customer_key", "source_customer_id", "is_current", "start_date", "end_date"],
        ),
        ("gold", "dim_product"): spark.createDataFrame(
            [(10, 10), (11, 11)], ["product_key", "source_product_variant_id"]
        ),
        ("gold", "dim_shop"): spark.createDataFrame([(1,)], ["shop_key"]),
        ("gold", "dim_category"): spark.createDataFrame([(1,)], ["category_key"]),
        ("gold", "dim_location"): spark.createDataFrame([(1,)], ["location_key"]),
        ("gold", "dim_date"): spark.createDataFrame([(20260101,)], ["date_key"]),
        ("gold", "dim_time"): spark.createDataFrame([(120000,)], ["time_key"]),
        ("gold", "dim_promotion"): spark.createDataFrame([(1,)], ["promotion_key"]),
        ("gold", "dim_payment"): spark.createDataFrame([(1,)], ["payment_key"]),
        ("gold", "dim_shipping"): spark.createDataFrame([(1,)], ["shipping_key"]),
        ("bronze", "cdc_dead_letters"): spark.createDataFrame([], "error_reason string"),
        ("_control", "cdc_batch_commits"): spark.createDataFrame([], "status string"),
    }

    monkeypatch.setattr(validate, "read_layer_table", lambda _spark, _config, layer, table: tables[(layer, table)])

    results = validate.run_validations(local_config, spark)

    assert results
    assert all(result.passed for result in results)


def test_print_results_outputs_status(capsys) -> None:
    validate.print_results([validate.ValidationResult("check", True, "1", "1")])

    assert "PASS" in capsys.readouterr().out
