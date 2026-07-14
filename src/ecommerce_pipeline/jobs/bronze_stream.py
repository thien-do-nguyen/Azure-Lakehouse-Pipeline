from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecommerce_pipeline.cdc.event_parser import EventQualityRules, parse_cdc_events
from ecommerce_pipeline.config import AppConfig


def _source_topics(config: AppConfig) -> list[str]:
    return [
        config.streaming.topic_for_table(config.postgres.source_schema, table_name)
        for table_name in config.streaming.topics
    ]


def normalize_debezium_events(df: DataFrame) -> DataFrame:
    raw_value = F.col("value").cast("string")
    raw_key = F.col("key").cast("string")
    return df.select(
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").cast("long").alias("_kafka_partition"),
        F.col("offset").cast("long").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        raw_key.alias("_debezium_key"),
        raw_value.alias("_debezium_value"),
        F.get_json_object(raw_value, "$.op").alias("op"),
        F.get_json_object(raw_value, "$.ts_ms").cast("long").alias("source_ts_ms"),
        F.get_json_object(raw_value, "$.source.db").alias("source_db"),
        F.get_json_object(raw_value, "$.source.schema").alias("source_schema"),
        F.get_json_object(raw_value, "$.source.table").alias("source_table"),
        F.get_json_object(raw_value, "$.before").alias("before_json"),
        F.get_json_object(raw_value, "$.after").alias("after_json"),
        F.current_timestamp().alias("_bronze_ingested_at"),
    )


def build_bronze_cdc_events(config: AppConfig, kafka_df: DataFrame) -> DataFrame:
    normalized = normalize_debezium_events(kafka_df)
    return parse_cdc_events(normalized, EventQualityRules(allowed_tables=config.streaming.topics))


def read_kafka_cdc_stream(config: AppConfig, spark) -> DataFrame:
    topics = ",".join(_source_topics(config))
    if not topics:
        raise ValueError("streaming.topics must contain at least one source table")
    if not config.streaming.bootstrap_servers:
        raise ValueError("streaming.bootstrap_servers is required for Kafka CDC streaming")

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.streaming.bootstrap_servers)
        .option("subscribe", topics)
        .option("startingOffsets", config.streaming.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def run_bronze_stream(config: AppConfig, spark) -> list:
    if not config.streaming.enabled:
        raise RuntimeError("Streaming is disabled. Set STREAMING_ENABLED=true or enable streaming in config.")

    events = build_bronze_cdc_events(config, read_kafka_cdc_stream(config, spark))
    valid_events = events.where(F.col("is_valid_event") == F.lit(True))
    dead_letters = events.where(F.col("is_valid_event") == F.lit(False))

    bronze_query = (
        valid_events.writeStream.format(config.streaming.storage_format)
        .outputMode("append")
        .option("checkpointLocation", config.streaming.checkpoint_path)
        .trigger(processingTime=config.streaming.trigger_processing_time)
        .start(config.lakehouse.table_path("bronze", "cdc_events"))
    )
    dead_letter_query = (
        dead_letters.writeStream.format(config.streaming.storage_format)
        .outputMode("append")
        .option("checkpointLocation", f"{config.streaming.checkpoint_path}_dead_letters")
        .trigger(processingTime=config.streaming.trigger_processing_time)
        .start(config.streaming.dead_letter_path)
    )
    return [bronze_query, dead_letter_query]
