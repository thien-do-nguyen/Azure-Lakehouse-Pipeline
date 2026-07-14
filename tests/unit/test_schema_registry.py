from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

from ecommerce_pipeline.cdc.schema_registry import CdcSchemaRegistry


def test_schema_registry_tracks_added_and_removed_columns(tmp_path) -> None:
    registry = CdcSchemaRegistry(str(tmp_path / "schemas.json"))
    first_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("status", StringType(), True),
        ]
    )
    second_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("total_amount", StringType(), True),
        ]
    )

    first_change = registry.register_or_update("orders", first_schema)
    second_change = registry.validate_compatibility("orders", second_schema)

    assert first_change.added_columns == ["order_id", "status"]
    assert second_change.added_columns == ["total_amount"]
    assert second_change.removed_columns == ["status"]
    assert second_change.is_compatible is True


def test_schema_registry_infers_payload_schema(spark, tmp_path) -> None:
    registry = CdcSchemaRegistry(str(tmp_path / "schemas.json"))
    events = spark.createDataFrame(
        [('{"order_id":1,"status":"paid"}',), ('{"order_id":2,"new_col":"x"}',)],
        ["record_json"],
    )

    schema = registry.resolve_table_schema("orders", events)

    assert set(schema.fieldNames()) == {"order_id", "status", "new_col"}
    assert registry.load()["orders"]["columns"] == ["new_col", "order_id", "status"]
