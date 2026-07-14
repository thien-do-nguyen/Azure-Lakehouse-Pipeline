from __future__ import annotations

from datetime import datetime

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from ecommerce_pipeline.cdc.event_parser import EventQualityRules, parse_cdc_events


def test_parse_cdc_events_maps_operations_and_metadata(spark) -> None:
    df = spark.createDataFrame(
        [
            (
                "ecommerce.customer_app.orders",
                0,
                10,
                datetime(2026, 1, 1, 0, 0, 0),
                '{"order_id":1}',
                '{"op":"u"}',
                "u",
                1760000000000,
                "ecommerce",
                "customer_app",
                "orders",
                '{"order_id":1,"status":"pending"}',
                '{"order_id":1,"status":"paid"}',
                datetime(2026, 1, 1, 0, 0, 1),
            )
        ],
        [
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
            "_debezium_key",
            "_debezium_value",
            "op",
            "source_ts_ms",
            "source_db",
            "source_schema",
            "source_table",
            "before_json",
            "after_json",
            "_bronze_ingested_at",
        ],
    )

    row = parse_cdc_events(df, EventQualityRules(allowed_tables=["orders"])).collect()[0]

    assert row["operation"] == "UPDATE"
    assert row["table_name"] == "orders"
    assert row["record_json"] == '{"order_id":1,"status":"paid"}'
    assert row["primary_key_json"] == '{"order_id":1}'
    assert row["is_valid_event"] is True
    assert row["_cdc_event_id"]


def test_parse_cdc_events_sends_bad_events_to_dlq_shape(spark) -> None:
    schema = StructType(
        [
            StructField("_kafka_topic", StringType(), True),
            StructField("_kafka_partition", LongType(), True),
            StructField("_kafka_offset", LongType(), True),
            StructField("_kafka_timestamp", TimestampType(), True),
            StructField("_debezium_key", StringType(), True),
            StructField("_debezium_value", StringType(), True),
            StructField("op", StringType(), True),
            StructField("source_ts_ms", LongType(), True),
            StructField("source_db", StringType(), True),
            StructField("source_schema", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("before_json", StringType(), True),
            StructField("after_json", StringType(), True),
            StructField("_bronze_ingested_at", TimestampType(), True),
        ]
    )
    df = spark.createDataFrame(
        [("topic", 0, 11, None, "{}", "{}", "x", None, None, None, "orders", None, None, None)],
        schema,
    )

    row = parse_cdc_events(df, EventQualityRules(allowed_tables=["orders"])).collect()[0]

    assert row["is_valid_event"] is False
    assert row["error_reason"] == "unsupported_operation"
