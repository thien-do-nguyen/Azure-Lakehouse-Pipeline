from __future__ import annotations

from ecommerce_pipeline.jobs.gold_transforms import _keyed


def test_keyed_uses_source_key_for_single_numeric_column(spark) -> None:
    df = spark.createDataFrame([(10, "a"), (20, "b")], ["source_customer_id", "name"])

    rows = _keyed(df, "customer_key", ["source_customer_id"]).orderBy("customer_key").collect()

    assert [row["customer_key"] for row in rows] == [10, 20]


def test_keyed_uses_source_order_item_id_for_sales_key(spark) -> None:
    df = spark.createDataFrame([(1, 100), (2, 200)], ["source_order_id", "source_order_item_id"])

    rows = _keyed(df, "sales_key", ["source_order_id", "source_order_item_id"]).orderBy("sales_key").collect()

    assert [row["sales_key"] for row in rows] == [100, 200]
