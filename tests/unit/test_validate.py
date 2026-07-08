from __future__ import annotations

from decimal import Decimal

from ecommerce_pipeline.jobs import validate
from ecommerce_pipeline.jobs.validate import _duplicate_count, _money_close, _null_count


def test_validation_helpers(spark) -> None:
    df = spark.createDataFrame([(1, "x"), (1, None), (2, "z")], ["id", "value"])

    assert _duplicate_count(df, ["id"]) == 1
    assert _null_count(df, ["value"]) == 1
    assert _money_close(10, 10)
    assert not _money_close(10, 11)


def test_run_validations_returns_reconciliation_results(monkeypatch, spark, local_config) -> None:
    tables = {
        ("bronze", "orders"): spark.createDataFrame([(100,), (101,)], ["order_id"]),
        ("bronze", "order_items"): spark.createDataFrame([(1000,), (1001,)], ["order_item_id"]),
        ("silver", "app_users"): spark.createDataFrame([(1,), (2,)], ["user_id"]),
        ("silver", "orders"): spark.createDataFrame([(100,), (101,)], ["order_id"]),
        ("silver", "order_items"): spark.createDataFrame(
            [(1000, Decimal("10.00"), Decimal("1.00")), (1001, Decimal("5.00"), Decimal("0.50"))],
            ["order_item_id", "item_subtotal", "tax_amount"],
        ),
        ("silver", "payments"): spark.createDataFrame([(2000,), (2001,)], ["payment_id"]),
        ("silver", "shipments"): spark.createDataFrame([(3000,), (3001,)], ["shipment_id"]),
        ("silver", "product_variants"): spark.createDataFrame([(10,), (11,)], ["product_variant_id"]),
        ("gold", "fact_sales"): spark.createDataFrame(
            [
                (100, 1000, 20260101, 1, 1, 10, 1, 1, 1, Decimal("10.00"), Decimal("1.00")),
                (101, 1001, 20260101, 1, 2, 11, 1, 1, 1, Decimal("5.00"), Decimal("0.50")),
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
                "gross_sales_amount",
                "tax_amount",
            ],
        ).withColumn("bill_to_location_key", validate.F.lit(1)),
        ("gold", "dim_customer"): spark.createDataFrame([(1,), (2,)], ["customer_key"]),
        ("gold", "dim_product"): spark.createDataFrame([(10,), (11,)], ["product_key"]),
    }

    monkeypatch.setattr(validate, "read_layer_table", lambda _spark, _config, layer, table: tables[(layer, table)])

    results = validate.run_validations(local_config, spark)

    assert results
    assert all(result.passed for result in results)


def test_print_results_outputs_status(capsys) -> None:
    validate.print_results([validate.ValidationResult("check", True, "1", "1")])

    assert "PASS" in capsys.readouterr().out
