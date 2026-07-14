from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType, TimestampType

from ecommerce_pipeline.cdc.ledger import DeltaBatchLedger
from ecommerce_pipeline.cdc.merge_handler import CdcMergeHandler
from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.jobs.gold_stream import update_gold_after_silver_batch
from ecommerce_pipeline.logging import get_logger

logger = get_logger(__name__)

BRONZE_CDC_SCHEMA = StructType(
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
        StructField("operation", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("record_json", StringType(), True),
        StructField("primary_key_json", StringType(), True),
        StructField("_cdc_event_id", StringType(), True),
        StructField("is_valid_event", BooleanType(), True),
        StructField("error_reason", StringType(), True),
    ]
)


def ensure_bronze_cdc_table(config: AppConfig, spark: SparkSession) -> None:
    if config.streaming.storage_format != "delta":
        return

    from delta.tables import DeltaTable

    path = config.lakehouse.table_path("bronze", "cdc_events")
    if DeltaTable.isDeltaTable(spark, path):
        return
    spark.createDataFrame([], BRONZE_CDC_SCHEMA).write.format("delta").mode("overwrite").save(path)


def read_bronze_cdc_stream(config: AppConfig, spark: SparkSession):
    ensure_bronze_cdc_table(config, spark)
    return spark.readStream.format(config.streaming.storage_format).load(config.lakehouse.table_path("bronze", "cdc_events"))


def process_cdc_batch(
    config: AppConfig,
    spark: SparkSession,
    batch_df,
    batch_id: int,
    handler: CdcMergeHandler,
    ledger: DeltaBatchLedger,
    update_gold: bool,
) -> None:
    batch_key = ledger.batch_key(batch_df)
    if ledger.is_committed(batch_key):
        logger.info("Skipping committed CDC batch %s (%s) for %s", batch_id, batch_key[:12], ledger.pipeline_id)
        return

    event_count = batch_df.count()
    ledger.mark_processing(batch_key, batch_id, event_count)
    try:
        handler.merge_batch(batch_df, batch_id)
        if update_gold:
            update_gold_after_silver_batch(config, spark, batch_df)
        ledger.record_watermarks(batch_df, batch_id)
        ledger.mark_committed(batch_key, batch_id, event_count)
    except Exception as exc:
        ledger.mark_failed(batch_key, batch_id, event_count, exc)
        raise


def run_silver_stream(config: AppConfig, spark: SparkSession, update_gold: bool = False):
    if not config.streaming.enabled:
        raise RuntimeError("Streaming is disabled. Set STREAMING_ENABLED=true or enable streaming in config.")

    handler = CdcMergeHandler(config, spark)
    ledger = DeltaBatchLedger(config, spark, include_gold=update_gold)

    def process_batch(batch_df, batch_id: int) -> None:
        process_cdc_batch(config, spark, batch_df, batch_id, handler, ledger, update_gold)

    return (
        read_bronze_cdc_stream(config, spark)
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", config.streaming.silver_checkpoint_path)
        .trigger(processingTime=config.streaming.trigger_processing_time)
        .start()
    )
