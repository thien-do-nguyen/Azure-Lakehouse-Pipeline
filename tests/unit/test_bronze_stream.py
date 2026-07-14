from __future__ import annotations

from dataclasses import replace
from datetime import datetime

import pytest

from ecommerce_pipeline.jobs.bronze_stream import (
    _source_topics,
    build_bronze_cdc_events,
    normalize_debezium_events,
    read_kafka_cdc_stream,
    run_bronze_stream,
)


def test_source_topics_use_debezium_topic_contract(local_config) -> None:
    assert "ecommerce.customer_app.orders" in _source_topics(local_config)


def test_normalize_debezium_events_extracts_metadata(spark) -> None:
    source_df = spark.createDataFrame(
        [
            (
                "ecommerce.customer_app.orders",
                0,
                42,
                datetime(2026, 1, 1, 0, 0, 0),
                '{"order_id": 1}',
                (
                    '{"op":"c","ts_ms":1760000000000,'
                    '"source":{"db":"ecommerce","schema":"customer_app","table":"orders"},'
                    '"before":null,"after":{"order_id":1,"status":"confirmed"}}'
                ),
            )
        ],
        ["topic", "partition", "offset", "timestamp", "key", "value"],
    )

    row = normalize_debezium_events(source_df).collect()[0]

    assert row["_kafka_topic"] == "ecommerce.customer_app.orders"
    assert row["_kafka_offset"] == 42
    assert row["op"] == "c"
    assert row["source_ts_ms"] == 1760000000000
    assert row["source_schema"] == "customer_app"
    assert row["source_table"] == "orders"
    assert '"status":"confirmed"' in row["after_json"]


def test_build_bronze_cdc_events_marks_invalid_events(spark, local_config) -> None:
    source_df = spark.createDataFrame(
        [("ecommerce.customer_app.unknown", 0, 42, datetime(2026, 1, 1, 0, 0, 0), "{}", '{"op":"c"}')],
        ["topic", "partition", "offset", "timestamp", "key", "value"],
    )

    row = build_bronze_cdc_events(local_config, source_df).collect()[0]

    assert row["operation"] == "INSERT"
    assert row["is_valid_event"] is False
    assert row["error_reason"] == "missing_payload"


def test_read_kafka_cdc_stream_requires_topics(spark, local_config) -> None:
    config = replace(local_config, streaming=replace(local_config.streaming, topics=[]))

    with pytest.raises(ValueError, match="streaming.topics"):
        read_kafka_cdc_stream(config, spark)


def test_read_kafka_cdc_stream_requires_bootstrap_servers(spark, local_config) -> None:
    config = replace(local_config, streaming=replace(local_config.streaming, bootstrap_servers=""))

    with pytest.raises(ValueError, match="bootstrap_servers"):
        read_kafka_cdc_stream(config, spark)


def test_run_bronze_stream_requires_enabled(spark, local_config) -> None:
    config = replace(local_config, streaming=replace(local_config.streaming, enabled=False))

    with pytest.raises(RuntimeError, match="Streaming is disabled"):
        run_bronze_stream(config, spark)
