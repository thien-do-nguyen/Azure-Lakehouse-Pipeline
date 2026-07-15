from __future__ import annotations

from dataclasses import replace

from ecommerce_pipeline.jobs import gold


def test_run_gold_writes_built_tables(monkeypatch, spark, local_config) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    config = replace(local_config, lakehouse=replace(local_config.lakehouse, format="parquet"))
    writes = []

    monkeypatch.setattr(gold, "build_gold_tables", lambda _config, _spark: {"dim_test": df})
    monkeypatch.setattr(gold, "write_layer_table", lambda _df, _config, layer, table: writes.append((layer, table)))

    gold.run_gold(config, spark)

    assert writes == [("gold", "dim_test")]


def test_run_gold_uses_delta_merge_for_incremental_tables(monkeypatch, spark, local_config) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    config = replace(local_config, lakehouse=replace(local_config.lakehouse, format="delta"))
    calls = []

    monkeypatch.setattr(
        gold,
        "build_gold_tables",
        lambda _config, _spark, customer_history=None: {
            "fact_sales": df,
            "dim_customer": df,
            "dim_product": df,
        },
    )
    monkeypatch.setattr(
        gold,
        "synchronize_to_delta",
        lambda **kwargs: calls.append(("synchronize", kwargs["path"], kwargs["keys"])),
    )
    monkeypatch.setattr(
        gold,
        "scd2_merge",
        lambda **kwargs: calls.append(("scd2", kwargs["path"], kwargs["natural_keys"])),
    )
    monkeypatch.setattr(
        gold,
        "write_layer_table",
        lambda _df, _config, layer, table: calls.append(("write", layer, table)),
    )
    customer_source = spark.createDataFrame([(1,)], ["user_id"])
    customer_history = spark.createDataFrame([(1,)], ["customer_key"])
    monkeypatch.setattr(
        gold,
        "read_layer_table",
        lambda *_args: customer_source if _args[-1] == "app_users" else customer_history,
    )
    monkeypatch.setattr(gold, "build_dim_customer", lambda _users, _orders: df)

    gold.run_gold(config, spark)

    assert calls == [
        ("scd2", config.lakehouse.table_path("gold", "dim_customer"), ["source_customer_id"]),
        ("synchronize", config.lakehouse.table_path("gold", "fact_sales"), ["source_order_id", "source_order_item_id"]),
        ("write", "gold", "dim_product"),
    ]
