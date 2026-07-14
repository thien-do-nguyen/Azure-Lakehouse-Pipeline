from __future__ import annotations

from functools import reduce
from operator import and_

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

from ecommerce_pipeline.cdc.event_parser import DELETE
from ecommerce_pipeline.cdc.schema_registry import CdcSchemaRegistry
from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.delta import upsert_to_delta
from ecommerce_pipeline.jobs.silver import transform_silver_table


class CdcMergeHandler:
    def __init__(self, config: AppConfig, spark: SparkSession, registry: CdcSchemaRegistry | None = None) -> None:
        self.config = config
        self.spark = spark
        self.registry = registry or CdcSchemaRegistry(config.streaming.schema_registry_path)

    def merge_batch(self, events: DataFrame, batch_id: int) -> None:
        _ = batch_id
        valid_events = events.where(F.col("is_valid_event") == F.lit(True))
        for table_name in self.config.streaming.topics:
            table_events = valid_events.where(F.col("table_name") == F.lit(table_name))
            if table_events.limit(1).count() == 0:
                continue
            prepared = self._prepare_table_events(table_name, table_events)
            aligned = self._merge_table(table_name, prepared)
            self._merge_silver_table(table_name, aligned)

    def _prepare_table_events(self, table_name: str, events: DataFrame) -> DataFrame:
        keys = self._primary_keys(table_name)
        schema = self.registry.resolve_table_schema(table_name, events, primary_keys=keys)
        parsed = events.withColumn("_record", F.from_json(F.col("record_json"), schema))

        key_columns = [self._primary_key_column(key).alias(key) for key in keys]
        payload_columns = [
            F.col(f"_record.`{field.name}`").alias(field.name)
            for field in schema.fields
            if field.name not in keys
        ]
        metadata_columns = [
            F.col("operation"),
            F.col("event_ts").alias("_cdc_event_ts"),
            F.col("_cdc_event_id"),
            F.col("_kafka_topic"),
            F.col("_kafka_partition"),
            F.col("_kafka_offset"),
            (F.col("operation") == F.lit(DELETE)).alias("is_deleted"),
            F.current_timestamp().alias("_bronze_ingested_at"),
            F.lit(self.config.postgres.source_schema).alias("_source_schema"),
            F.lit(table_name).alias("_source_table"),
        ]

        selected = parsed.select(*key_columns, *payload_columns, *metadata_columns)
        deduped = selected.dropDuplicates(["_cdc_event_id"])
        window = Window.partitionBy(*keys).orderBy(F.col("_cdc_event_ts").desc_nulls_last(), F.col("_kafka_offset").desc())
        return deduped.withColumn("_row_number", F.row_number().over(window)).where(F.col("_row_number") == 1).drop(
            "_row_number"
        )

    def _primary_key_column(self, key: str) -> F.Column:
        from_record = F.col(f"_record.`{key}`").cast("string")
        from_key = F.get_json_object(F.col("primary_key_json"), f"$.{key}")
        from_key_payload = F.get_json_object(F.col("primary_key_json"), f"$.payload.{key}")
        return F.coalesce(from_record, from_key, from_key_payload)

    def _merge_table(self, table_name: str, source: DataFrame) -> DataFrame:
        if self.config.streaming.storage_format != "delta":
            raise RuntimeError("CDC Bronze merge requires streaming.storage_format=delta")

        path = self.config.lakehouse.table_path("bronze", table_name)
        keys = self._primary_keys(table_name)

        from delta.tables import DeltaTable

        if not DeltaTable.isDeltaTable(self.spark, path):
            raise RuntimeError(
                f"Missing bootstrap table bronze/{table_name}. Run the full batch pipeline before starting CDC."
            )

        delta_table = DeltaTable.forPath(self.spark, path)
        source = self._align_with_target(source, path)
        merge = delta_table.alias("target").merge(source.alias("source"), self._merge_condition(keys))
        if self.config.streaming.delete_mode == "hard":
            (
                merge.whenMatchedDelete(condition="source.operation = 'DELETE'")
                .whenMatchedUpdateAll(condition="source.operation <> 'DELETE'")
                .whenNotMatchedInsertAll(condition="source.operation <> 'DELETE'")
                .execute()
            )
            return source

        merge.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        return source

    def _align_with_target(self, source: DataFrame, path: str) -> DataFrame:
        """Cast Debezium JSON values to the JDBC-created Delta schema."""
        target_schema = self.spark.read.format("delta").load(path).schema
        aligned = source
        source_types = dict(source.dtypes)
        for field in target_schema.fields:
            if field.name not in aligned.columns:
                aligned = aligned.withColumn(field.name, F.lit(None).cast(field.dataType))
                continue
            if isinstance(field.dataType, TimestampType) and source_types.get(field.name) in {
                "tinyint", "smallint", "int", "bigint", "long", "float", "double"
            }:
                aligned = aligned.withColumn(
                    field.name,
                    (F.col(field.name).cast("double") / F.lit(1000)).cast("timestamp"),
                )
            elif isinstance(field.dataType, DateType) and source_types.get(field.name) in {
                "tinyint", "smallint", "int", "bigint", "long"
            }:
                aligned = aligned.withColumn(
                    field.name,
                    F.date_add(F.lit("1970-01-01").cast("date"), F.col(field.name).cast("int")),
                )
            else:
                aligned = aligned.withColumn(field.name, F.col(field.name).cast(field.dataType))
        target_names = [field.name for field in target_schema.fields]
        evolved_names = [name for name in aligned.columns if name not in target_names]
        return aligned.select(*target_names, *evolved_names)

    def _merge_silver_table(self, table_name: str, source: DataFrame) -> None:
        from delta.tables import DeltaTable

        path = self.config.lakehouse.table_path("silver", table_name)
        keys = self._primary_keys(table_name)
        if not DeltaTable.isDeltaTable(self.spark, path):
            raise RuntimeError(
                f"Missing bootstrap table silver/{table_name}. Run the full batch pipeline before starting CDC."
            )

        deletes = source.where(F.col("is_deleted") == F.lit(True)).select(*keys).dropDuplicates(keys)
        if deletes.take(1):
            (
                DeltaTable.forPath(self.spark, path)
                .alias("target")
                .merge(deletes.alias("source"), self._merge_condition(keys))
                .whenMatchedDelete()
                .execute()
            )

        active = transform_silver_table(source, table_name, keys)
        if active.take(1):
            upsert_to_delta(self.spark, active, path, keys)

    def _merge_condition(self, keys: list[str]) -> str:
        return " AND ".join(f"target.{key} = source.{key}" for key in keys)

    def _primary_keys(self, table_name: str) -> list[str]:
        keys = self.config.streaming.primary_keys.get(table_name, [])
        if not keys:
            raise ValueError(f"Missing CDC primary key config for table: {table_name}")
        return keys

def latest_event_per_key(df: DataFrame, keys: list[str]) -> DataFrame:
    condition = reduce(and_, [F.col(key).isNotNull() for key in keys])
    window = Window.partitionBy(*keys).orderBy(F.col("_cdc_event_ts").desc_nulls_last(), F.col("_kafka_offset").desc())
    return df.where(condition).withColumn("_row_number", F.row_number().over(window)).where(F.col("_row_number") == 1)
