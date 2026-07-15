from __future__ import annotations

from dataclasses import replace
from datetime import datetime

import pytest
from pyspark.sql.types import DateType, LongType, StructField, StructType, TimestampType

from ecommerce_pipeline.cdc import merge_handler
from ecommerce_pipeline.cdc.merge_handler import CdcMergeHandler, latest_event_per_key
from ecommerce_pipeline.cdc.schema_registry import CdcSchemaRegistry, SchemaCompatibilityError


def test_merge_handler_prepares_latest_event_per_primary_key(spark, local_config, tmp_path) -> None:
    config = replace(
        local_config,
        streaming=replace(
            local_config.streaming,
            schema_registry_path=str(tmp_path / "schemas.json"),
        ),
    )
    events = spark.createDataFrame(
        [
            (
                "orders",
                "UPDATE",
                '{"order_id":1}',
                '{"order_id":1,"order_status":"pending"}',
                datetime(2026, 1, 1, 0, 0, 0),
                "event-1",
                "topic",
                0,
                1,
                True,
            ),
            (
                "orders",
                "UPDATE",
                '{"order_id":1}',
                '{"order_id":1,"order_status":"paid"}',
                datetime(2026, 1, 1, 0, 0, 1),
                "event-2",
                "topic",
                0,
                2,
                True,
            ),
        ],
        [
            "table_name",
            "operation",
            "primary_key_json",
            "record_json",
            "event_ts",
            "_cdc_event_id",
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            "is_valid_event",
        ],
    )

    row = CdcMergeHandler(config, spark)._prepare_table_events("orders", events).collect()[0]

    assert row["order_id"] == "1"
    assert row["order_status"] == "paid"
    assert row["_kafka_offset"] == 2
    assert row["is_deleted"] is False


def test_merge_handler_requires_primary_key_config(spark, local_config, tmp_path) -> None:
    config = replace(
        local_config,
        streaming=replace(
            local_config.streaming,
            primary_keys={},
            schema_registry_path=str(tmp_path / "schemas.json"),
        ),
    )

    with pytest.raises(ValueError, match="Missing CDC primary key"):
        CdcMergeHandler(config, spark)._primary_keys("orders")


def test_merge_handler_fails_clearly_before_merging_breaking_schema(spark, local_config, tmp_path) -> None:
    registry = CdcSchemaRegistry(str(tmp_path / "schemas.json"))
    registry.resolve_table_schema(
        "orders",
        spark.createDataFrame([('{"order_id":1,"order_status":"pending"}',)], ["record_json"]),
        ["order_id"],
    )
    events = spark.createDataFrame(
        [
            (
                "orders",
                "UPDATE",
                '{"order_id":1}',
                '{"order_id":"invalid-type","order_status":"pending"}',
                datetime(2026, 1, 1),
                "event-1",
                "topic",
                0,
                1,
                True,
            )
        ],
        [
            "table_name",
            "operation",
            "primary_key_json",
            "record_json",
            "event_ts",
            "_cdc_event_id",
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            "is_valid_event",
        ],
    )

    with pytest.raises(SchemaCompatibilityError, match="datatype changed for order_id"):
        CdcMergeHandler(local_config, spark, registry=registry)._prepare_table_events("orders", events)


def test_merge_handler_rejects_non_delta_silver_merge(spark, local_config, tmp_path) -> None:
    config = replace(
        local_config,
        streaming=replace(
            local_config.streaming,
            storage_format="parquet",
            schema_registry_path=str(tmp_path / "schemas.json"),
        ),
    )
    source = spark.createDataFrame([("1",)], ["order_id"])

    with pytest.raises(RuntimeError, match="requires streaming.storage_format=delta"):
        CdcMergeHandler(config, spark)._merge_table("orders", source)


def test_align_with_bootstrap_schema_casts_debezium_temporal_values(spark, local_config) -> None:
    source = spark.createDataFrame([("7", 1_767_225_600_000, 20_454)], ["order_id", "created_at", "event_date"])
    target_schema = StructType(
        [
            StructField("order_id", LongType()),
            StructField("created_at", TimestampType()),
            StructField("event_date", DateType()),
        ]
    )

    class FakeRead:
        def format(self, _value):
            return self

        def load(self, _path):
            class Target:
                schema = target_schema

            return Target()

    class FakeSpark:
        read = FakeRead()

    row = CdcMergeHandler(local_config, FakeSpark())._align_with_target(source, "/bronze/orders").collect()[0]

    assert row["order_id"] == 7
    assert row["created_at"] == datetime(2026, 1, 1)
    assert row["event_date"].isoformat() == "2026-01-01"


def test_merge_batch_processes_only_non_empty_configured_tables(monkeypatch, spark, local_config) -> None:
    config = replace(
        local_config,
        streaming=replace(local_config.streaming, topics=["orders", "payments"]),
    )
    handler = CdcMergeHandler(config, spark)
    events = spark.createDataFrame([("orders", True)], ["table_name", "is_valid_event"])
    calls = []
    prepared = spark.createDataFrame([(1,)], ["order_id"])

    monkeypatch.setattr(handler, "_prepare_table_events", lambda table, _events: calls.append(("prepare", table)) or prepared)
    monkeypatch.setattr(
        handler,
        "_merge_table",
        lambda table, _source: calls.append(("merge", table)) or prepared,
    )
    monkeypatch.setattr(handler, "_merge_silver_table", lambda table, _source: calls.append(("silver", table)))

    handler.merge_batch(events, 3)

    assert calls == [("prepare", "orders"), ("merge", "orders"), ("silver", "orders")]


def test_merge_table_requires_batch_bootstrap(monkeypatch, spark, local_config) -> None:
    from delta.tables import DeltaTable

    monkeypatch.setattr(DeltaTable, "isDeltaTable", lambda *_args: False)
    handler = CdcMergeHandler(local_config, spark)

    with pytest.raises(RuntimeError, match="Run the full batch pipeline"):
        handler._merge_table("orders", spark.createDataFrame([(1,)], ["order_id"]))


def test_merge_silver_deletes_tombstone_and_upserts_active_row(monkeypatch, spark, local_config) -> None:
    from delta.tables import DeltaTable

    calls = []

    class FakeMerge:
        def whenMatchedDelete(self):
            calls.append("delete")
            return self

        def execute(self):
            calls.append("execute")

    class FakeTable:
        def alias(self, _name):
            return self

        def merge(self, source, condition):
            calls.append((condition, [row["order_id"] for row in source.collect()]))
            return FakeMerge()

    monkeypatch.setattr(DeltaTable, "isDeltaTable", lambda *_args: True)
    monkeypatch.setattr(DeltaTable, "forPath", lambda *_args: FakeTable())
    monkeypatch.setattr(
        merge_handler,
        "upsert_to_delta",
        lambda _spark, df, _path, _keys: calls.append(("upsert", [row["order_id"] for row in df.collect()])),
    )
    source = spark.createDataFrame(
        [(1, "paid", False), (2, "cancelled", True)],
        ["order_id", "order_status", "is_deleted"],
    )

    CdcMergeHandler(local_config, spark)._merge_silver_table("orders", source)

    assert calls[0][1] == [2]
    assert calls[-1] == ("upsert", [1])


def test_latest_event_per_key_drops_null_keys_and_keeps_latest(spark) -> None:
    df = spark.createDataFrame(
        [
            ("1", datetime(2026, 1, 1), 1),
            ("1", datetime(2026, 1, 2), 2),
            (None, datetime(2026, 1, 3), 3),
        ],
        ["order_id", "_cdc_event_ts", "_kafka_offset"],
    )

    rows = latest_event_per_key(df, ["order_id"]).collect()

    assert [(row["order_id"], row["_kafka_offset"]) for row in rows] == [("1", 2)]
