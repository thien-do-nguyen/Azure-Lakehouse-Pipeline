from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from ecommerce_pipeline.jobs import bronze
from ecommerce_pipeline.watermark import get_watermark, update_watermark


def test_run_bronze_adds_metadata_and_writes(monkeypatch, spark, local_config) -> None:
    source_df = spark.createDataFrame([(1, "paid")], ["order_id", "status"])
    written = {}

    monkeypatch.setattr(bronze, "read_postgres_table", lambda _spark, _config, _table: source_df)
    monkeypatch.setattr(
        bronze,
        "write_layer_table",
        lambda df, _config, layer, table, mode=None: written.update(
            {
                "columns": df.columns,
                "layer": layer,
                "table": table,
                "mode": mode,
            }
        ),
    )

    bronze.run_bronze(local_config, spark)

    assert written["layer"] == "bronze"
    assert written["table"] == local_config.batch.source_tables[-1]
    assert "_bronze_ingested_at" in written["columns"]
    assert "_source_schema" in written["columns"]
    assert "_source_table" in written["columns"]


def test_incremental_source_reads_full_without_watermark(monkeypatch, spark, local_config) -> None:
    config = replace(local_config, batch=replace(local_config.batch, load_type="incremental"))
    source_df = spark.createDataFrame([(1,)], ["id"])
    calls = []

    monkeypatch.setattr(bronze, "read_postgres_table", lambda _spark, _config, table: calls.append(table) or source_df)

    result = bronze._read_source_table(config, spark, "orders")

    assert result.count() == 1
    assert calls == ["orders"]


def test_incremental_source_uses_watermark_query(monkeypatch, spark, local_config) -> None:
    config = replace(local_config, batch=replace(local_config.batch, load_type="incremental"))
    update_watermark(config, "orders", datetime(2026, 1, 1, 0, 0, 0))
    source_df = spark.createDataFrame([(1,)], ["id"])
    captured = {}

    def fake_query(_spark, _config, query):
        captured["query"] = query
        return source_df

    monkeypatch.setattr(bronze, "read_postgres_query", fake_query)

    result = bronze._read_source_table(config, spark, "orders")

    assert result.count() == 1
    assert "customer_app.orders" in captured["query"]
    assert "updated_at" in captured["query"]


def test_save_incremental_watermark(spark, local_config) -> None:
    config = replace(local_config, batch=replace(local_config.batch, load_type="incremental"))
    df = spark.createDataFrame([(datetime(2026, 1, 2, 0, 0, 0),)], ["updated_at"])

    bronze._save_incremental_watermark(config, "orders", df)

    assert get_watermark(config, "orders") == "2026-01-02 00:00:00"
