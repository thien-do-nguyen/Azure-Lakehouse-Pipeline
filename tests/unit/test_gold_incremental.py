from __future__ import annotations

from ecommerce_pipeline.jobs import gold_incremental


def test_impacted_order_ids_includes_before_and_after_relationships(monkeypatch, spark, local_config) -> None:
    tables = {
        "orders": spark.createDataFrame([(1, 10, 100, 100), (2, 20, 200, 200)], ["order_id", "customer_id", "shipping_address_id", "billing_address_id"]),
        "order_items": spark.createDataFrame([(1, 1, 1000, 5000, 50), (2, 2, 2000, 6000, 60)], ["order_item_id", "order_id", "product_id", "product_variant_id", "shop_id"]),
        "products": spark.createDataFrame([(1000, 100), (2000, 200)], ["product_id", "category_id"]),
        "order_vouchers": spark.createDataFrame([(1, 1, 700)], ["order_voucher_id", "order_id", "voucher_id"]),
    }
    monkeypatch.setattr(
        gold_incremental,
        "read_layer_table",
        lambda _spark, _config, _layer, table: tables[table],
    )
    events = spark.createDataFrame(
        [("order_items", '{"order_id":1}', '{"order_id":2}', True)],
        ["table_name", "before_json", "after_json", "is_valid_event"],
    )

    rows = gold_incremental.impacted_order_ids(local_config, spark, events).orderBy("order_id").collect()

    assert [row["order_id"] for row in rows] == [1, 2]


def test_run_gold_incremental_replaces_only_affected_fact_scope(monkeypatch, spark, local_config) -> None:
    affected = spark.createDataFrame([(2,)], ["order_id"])
    tables = {
        "dim_customer": spark.createDataFrame([(1, "h")], ["source_customer_id", "scd_hash"]),
        "dim_date": spark.createDataFrame([(20260101,)], ["date_key"]),
        "dim_time": spark.createDataFrame([(120000,)], ["time_key"]),
        "dim_location": spark.createDataFrame([(1,)], ["source_address_id"]),
        "dim_shop": spark.createDataFrame([(1,)], ["source_shop_id"]),
        "dim_category": spark.createDataFrame([(1,)], ["source_category_id"]),
        "dim_product": spark.createDataFrame([(1,)], ["source_product_variant_id"]),
        "dim_promotion": spark.createDataFrame([("p",)], ["natural_promotion_hash"]),
        "dim_payment": spark.createDataFrame([("pay",)], ["natural_payment_hash"]),
        "dim_shipping": spark.createDataFrame([("ship",)], ["natural_shipping_hash"]),
        "fact_sales": spark.createDataFrame(
            [(1, 10, 20260101, 120000, 1), (2, 20, 20260101, 120000, 1)],
            ["source_order_id", "source_order_item_id", "order_date_key", "order_time_key", "promotion_key"],
        ),
    }
    calls = []
    monkeypatch.setattr(gold_incremental, "impacted_order_ids", lambda *_args: affected)
    monkeypatch.setattr(gold_incremental, "build_gold_tables", lambda *_args, **_kwargs: tables)
    monkeypatch.setattr(gold_incremental, "read_layer_table", lambda *_args: tables["dim_customer"])
    monkeypatch.setattr(gold_incremental, "_impacted_dimension", lambda _name, df, *_args: df)
    monkeypatch.setattr(gold_incremental, "scd2_merge", lambda **_kwargs: calls.append("scd2"))
    monkeypatch.setattr(gold_incremental, "upsert_to_delta", lambda *_args, **_kwargs: calls.append("dimension"))
    monkeypatch.setattr(
        gold_incremental,
        "replace_delta_scope",
        lambda **kwargs: calls.append(("facts", [row["source_order_id"] for row in kwargs["df"].collect()])),
    )
    events = spark.createDataFrame([("orders", True)], ["table_name", "is_valid_event"])

    gold_incremental.run_gold_incremental(local_config, spark, events)

    assert calls.count("dimension") == 3
    assert ("facts", [2]) in calls


def test_impacted_product_dimension_filters_by_product_and_variant_keys(spark) -> None:
    dimension = spark.createDataFrame(
        [(10, 100), (20, 200), (30, 300)],
        ["source_product_id", "source_product_variant_id"],
    )
    events = spark.createDataFrame(
        [
            ("products", '{"product_id":10}', '{"product_id":10}'),
            ("product_variants", '{"product_variant_id":200}', '{"product_variant_id":200}'),
        ],
        ["table_name", "before_json", "after_json"],
    )
    affected_facts = spark.createDataFrame([], "product_key long")

    rows = gold_incremental._impacted_dimension(
        "dim_product", dimension, events, affected_facts
    ).orderBy("source_product_id").collect()

    assert [(row["source_product_id"], row["source_product_variant_id"]) for row in rows] == [
        (10, 100),
        (20, 200),
    ]
