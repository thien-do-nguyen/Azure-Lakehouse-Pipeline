from __future__ import annotations

import hashlib
import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.delta import upsert_to_delta

LEDGER_SCHEMA = StructType(
    [
        StructField("pipeline_id", StringType(), False),
        StructField("batch_key", StringType(), False),
        StructField("batch_id", LongType(), False),
        StructField("status", StringType(), False),
        StructField("event_count", LongType(), False),
        StructField("started_at", TimestampType(), True),
        StructField("committed_at", TimestampType(), True),
        StructField("error_message", StringType(), True),
        StructField("updated_at", TimestampType(), False),
    ]
)

WATERMARK_SCHEMA = StructType(
    [
        StructField("pipeline_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("topic", StringType(), False),
        StructField("partition", LongType(), False),
        StructField("last_offset", LongType(), False),
        StructField("last_event_ts", TimestampType(), True),
        StructField("batch_id", LongType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)


class DeltaBatchLedger:
    """Delta-backed micro-batch commit ledger and per-partition CDC watermark."""

    def __init__(self, config: AppConfig, spark: SparkSession, include_gold: bool) -> None:
        self.config = config
        self.spark = spark
        checkpoint_fingerprint = hashlib.sha256(config.streaming.silver_checkpoint_path.encode()).hexdigest()[:12]
        suffix = "silver_gold" if include_gold else "silver"
        self.pipeline_id = f"cdc_{suffix}_{checkpoint_fingerprint}"
        self.ledger_path = config.lakehouse.table_path("_control", "cdc_batch_commits")
        self.watermark_path = config.lakehouse.table_path("_control", "cdc_table_watermarks")
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        from delta.tables import DeltaTable

        if not DeltaTable.isDeltaTable(self.spark, self.ledger_path):
            self.spark.createDataFrame([], LEDGER_SCHEMA).write.format("delta").mode("overwrite").save(self.ledger_path)
        elif "batch_key" not in self.spark.read.format("delta").load(self.ledger_path).columns:
            # In-place upgrade from the batch_id-only ledger used by older local deployments.
            self.spark.sql(f"ALTER TABLE delta.`{self.ledger_path}` ADD COLUMNS (batch_key STRING)")
        if not DeltaTable.isDeltaTable(self.spark, self.watermark_path):
            self.spark.createDataFrame([], WATERMARK_SCHEMA).write.format("delta").mode("overwrite").save(
                self.watermark_path
            )

    def batch_key(self, events: DataFrame) -> str:
        offsets = (
            events.groupBy("_kafka_topic", "_kafka_partition")
            .agg(
                F.min("_kafka_offset").alias("min_offset"),
                F.max("_kafka_offset").alias("max_offset"),
                F.count(F.lit(1)).alias("event_count"),
            )
            .orderBy("_kafka_topic", "_kafka_partition")
            .collect()
        )
        contract = [row.asDict(recursive=True) for row in offsets]
        return hashlib.sha256(json.dumps(contract, sort_keys=True, default=str).encode()).hexdigest()

    def is_committed(self, batch_key: str) -> bool:
        rows = (
            self.spark.read.format("delta")
            .load(self.ledger_path)
            .where(
                (F.col("pipeline_id") == F.lit(self.pipeline_id))
                & (F.col("batch_key") == F.lit(batch_key))
                & (F.col("status") == F.lit("COMMITTED"))
            )
            .limit(1)
        )
        return bool(rows.take(1))

    def mark_processing(self, batch_key: str, batch_id: int, event_count: int) -> None:
        self._write_status(batch_key, batch_id, "PROCESSING", event_count, None)

    def mark_committed(self, batch_key: str, batch_id: int, event_count: int) -> None:
        self._write_status(batch_key, batch_id, "COMMITTED", event_count, None)

    def mark_failed(self, batch_key: str, batch_id: int, event_count: int, error: Exception) -> None:
        self._write_status(batch_key, batch_id, "FAILED", event_count, str(error)[:4000])

    def _write_status(
        self,
        batch_key: str,
        batch_id: int,
        status: str,
        event_count: int,
        error_message: str | None,
    ) -> None:
        now = F.current_timestamp()
        row = (
            self.spark.range(1)
            .select(
                F.lit(self.pipeline_id).alias("pipeline_id"),
                F.lit(batch_key).alias("batch_key"),
                F.lit(batch_id).cast("long").alias("batch_id"),
                F.lit(status).alias("status"),
                F.lit(event_count).cast("long").alias("event_count"),
                now.alias("started_at"),
                F.when(F.lit(status == "COMMITTED"), now).cast("timestamp").alias("committed_at"),
                F.lit(error_message).cast("string").alias("error_message"),
                now.alias("updated_at"),
            )
        )
        upsert_to_delta(self.spark, row, self.ledger_path, ["pipeline_id", "batch_key"])

    def record_watermarks(self, events: DataFrame, batch_id: int) -> None:
        watermarks = (
            events.where(F.col("is_valid_event") == F.lit(True))
            .groupBy("table_name", "_kafka_topic", "_kafka_partition")
            .agg(
                F.max("_kafka_offset").cast("long").alias("last_offset"),
                F.max("event_ts").alias("last_event_ts"),
            )
            .select(
                F.lit(self.pipeline_id).alias("pipeline_id"),
                "table_name",
                F.col("_kafka_topic").alias("topic"),
                F.col("_kafka_partition").cast("long").alias("partition"),
                "last_offset",
                "last_event_ts",
                F.lit(batch_id).cast("long").alias("batch_id"),
                F.current_timestamp().alias("updated_at"),
            )
        )
        upsert_to_delta(
            self.spark,
            watermarks,
            self.watermark_path,
            ["pipeline_id", "table_name", "topic", "partition"],
        )
