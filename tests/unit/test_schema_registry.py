from __future__ import annotations

import pytest
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from ecommerce_pipeline.cdc.schema_registry import CdcSchemaRegistry, SchemaCompatibilityError


def test_schema_registry_classifies_additive_and_widening_changes_as_compatible(tmp_path) -> None:
    registry = CdcSchemaRegistry(str(tmp_path / "schemas.json"))
    first_schema = StructType(
        [StructField("order_id", IntegerType(), False), StructField("status", StringType(), True)]
    )
    second_schema = StructType(
        [
            StructField("order_id", LongType(), False),
            StructField("status", StringType(), True),
            StructField("note", StringType(), True),
        ]
    )

    first_change = registry.register_or_update("orders", first_schema, ["order_id"])
    second_change = registry.validate_compatibility("orders", second_schema, ["order_id"])

    assert first_change.added_columns == ["order_id", "status"]
    assert second_change.added_columns == ["note"]
    assert second_change.type_changes[0].compatible is True
    assert second_change.classification == "compatible"


@pytest.mark.parametrize(
    ("schema", "primary_keys", "message"),
    [
        (StructType([StructField("order_id", LongType(), False)]), ["order_id"], "removed columns"),
        (
            StructType(
                [StructField("order_id", StringType(), False), StructField("status", StringType(), True)]
            ),
            ["order_id"],
            "datatype changed",
        ),
        (
            StructType(
                [StructField("order_id", LongType(), False), StructField("status", StringType(), True)]
            ),
            ["status"],
            "primary key changed",
        ),
    ],
)
def test_schema_registry_rejects_breaking_removed_type_and_primary_key_changes(
    tmp_path, schema, primary_keys, message
) -> None:
    registry = CdcSchemaRegistry(str(tmp_path / "schemas.json"))
    initial = StructType(
        [StructField("order_id", LongType(), False), StructField("status", StringType(), True)]
    )
    registry.register_or_update("orders", initial, ["order_id"])

    with pytest.raises(SchemaCompatibilityError, match=message):
        registry.register_or_update("orders", schema, primary_keys)


def test_schema_registry_infers_payload_schema_and_persists_primary_key(spark, tmp_path) -> None:
    registry = CdcSchemaRegistry(str(tmp_path / "schemas.json"))
    events = spark.createDataFrame(
        [('{"order_id":1,"status":"paid"}',), ('{"order_id":2,"new_col":"x"}',)],
        ["record_json"],
    )

    schema = registry.resolve_table_schema("orders", events, ["order_id"])

    assert set(schema.fieldNames()) == {"order_id", "status", "new_col"}
    assert registry.load()["orders"]["primary_keys"] == ["order_id"]


def test_schema_registry_preserves_present_all_null_column(spark, tmp_path) -> None:
    registry = CdcSchemaRegistry(str(tmp_path / "schemas.json"))
    first = spark.createDataFrame(
        [('{"order_id":1,"total_amount":10.5}',)], ["record_json"]
    )
    registry.resolve_table_schema("orders", first, ["order_id"])
    all_null = spark.createDataFrame(
        [('{"order_id":2,"total_amount":null}',)], ["record_json"]
    )

    change = registry.validate_compatibility(
        "orders", registry.infer_schema(all_null), ["order_id"]
    )

    assert change.removed_columns == []
    assert change.is_compatible is True


def test_event_resolution_keeps_registered_column_omitted_by_schemaless_event(spark, tmp_path) -> None:
    registry = CdcSchemaRegistry(str(tmp_path / "schemas.json"))
    first = spark.createDataFrame(
        [('{"order_id":1,"total_amount":10.5}',)], ["record_json"]
    )
    registry.resolve_table_schema("orders", first, ["order_id"])
    generated_column_omitted = spark.createDataFrame(
        [('{"order_id":2}',)], ["record_json"]
    )

    resolved = registry.resolve_table_schema(
        "orders", generated_column_omitted, ["order_id"]
    )

    assert set(resolved.fieldNames()) == {"order_id", "total_amount"}
