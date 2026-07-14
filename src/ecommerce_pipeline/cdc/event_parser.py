from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

INSERT = "INSERT"
UPDATE = "UPDATE"
DELETE = "DELETE"

_OPERATION_SQL = """
CASE op
  WHEN 'c' THEN 'INSERT'
  WHEN 'r' THEN 'INSERT'
  WHEN 'u' THEN 'UPDATE'
  WHEN 'd' THEN 'DELETE'
  ELSE 'UNKNOWN'
END
"""


@dataclass(frozen=True)
class EventQualityRules:
    allowed_tables: list[str]


def _payload_json() -> F.Column:
    return F.when(F.col("op") == F.lit("d"), F.col("before_json")).otherwise(F.col("after_json"))


def parse_cdc_events(df: DataFrame, rules: EventQualityRules) -> DataFrame:
    parsed = (
        df.withColumn("operation", F.expr(_OPERATION_SQL))
        .withColumn("table_name", F.col("source_table"))
        .withColumn("event_ts", (F.col("source_ts_ms") / F.lit(1000)).cast("timestamp"))
        .withColumn("record_json", _payload_json())
        .withColumn("primary_key_json", F.col("_debezium_key"))
        .withColumn(
            "_cdc_event_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("_kafka_topic"),
                    F.col("_kafka_partition").cast("string"),
                    F.col("_kafka_offset").cast("string"),
                ),
                256,
            ),
        )
    )
    return validate_cdc_events(parsed, rules)


def validate_cdc_events(df: DataFrame, rules: EventQualityRules) -> DataFrame:
    table_is_allowed = F.col("table_name").isin(rules.allowed_tables) if rules.allowed_tables else F.lit(False)
    valid_operation = F.col("operation").isin([INSERT, UPDATE, DELETE])
    has_payload = F.col("record_json").isNotNull()
    has_table = F.col("table_name").isNotNull()
    has_offset = F.col("_kafka_offset").isNotNull()

    return (
        df.withColumn("is_valid_event", valid_operation & has_payload & has_table & has_offset & table_is_allowed)
        .withColumn(
            "error_reason",
            F.when(~valid_operation, F.lit("unsupported_operation"))
            .when(~has_payload, F.lit("missing_payload"))
            .when(~has_table, F.lit("missing_table"))
            .when(~has_offset, F.lit("missing_offset"))
            .when(~table_is_allowed, F.concat(F.lit("unmonitored_table:"), F.col("table_name")))
            .otherwise(F.lit(None).cast("string")),
        )
        .drop(*[column for column in ["_allowed_tables"] if column in df.columns])
    )
