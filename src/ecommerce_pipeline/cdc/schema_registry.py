from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from ecommerce_pipeline.filesystem import ensure_parent, exists, open_text


@dataclass(frozen=True)
class SchemaChange:
    table_name: str
    added_columns: list[str]
    removed_columns: list[str]

    @property
    def is_compatible(self) -> bool:
        return True


class CdcSchemaRegistry:
    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> dict[str, dict[str, Any]]:
        if not exists(self.path):
            return {}
        with open_text(self.path, "r") as handle:
            return json.load(handle)

    def save(self, schemas: dict[str, dict[str, Any]]) -> None:
        ensure_parent(self.path)
        with open_text(self.path, "w") as handle:
            json.dump(schemas, handle, indent=2, sort_keys=True)
            handle.write("\n")

    def infer_schema(self, events: DataFrame) -> StructType:
        payloads = events.select("record_json").where(F.col("record_json").isNotNull())
        if payloads.limit(1).count() == 0:
            return StructType([])
        return events.sparkSession.read.json(payloads.rdd.map(lambda row: row["record_json"])).schema

    def register_or_update(self, table_name: str, schema: StructType) -> SchemaChange:
        schemas = self.load()
        current_columns = [field.name for field in schema.fields]
        previous_columns = schemas.get(table_name, {}).get("columns", [])

        added = [column for column in current_columns if column not in previous_columns]
        removed = [column for column in previous_columns if column not in current_columns]
        merged_columns = [*previous_columns, *added]

        schemas[table_name] = {
            "columns": merged_columns,
            "spark_schema": schema.jsonValue(),
        }
        self.save(schemas)
        return SchemaChange(table_name=table_name, added_columns=added, removed_columns=removed)

    def validate_compatibility(self, table_name: str, schema: StructType) -> SchemaChange:
        schemas = self.load()
        previous_columns = schemas.get(table_name, {}).get("columns", [])
        current_columns = [field.name for field in schema.fields]
        return SchemaChange(
            table_name=table_name,
            added_columns=[column for column in current_columns if column not in previous_columns],
            removed_columns=[column for column in previous_columns if column not in current_columns],
        )

    def resolve_table_schema(self, table_name: str, events: DataFrame) -> StructType:
        schema = self.infer_schema(events)
        self.register_or_update(table_name, schema)
        registered = self.load().get(table_name, {})
        fields = {field.name: field for field in schema.fields}
        merged_fields = [
            fields.get(column, StructField(column, schema[column].dataType, True))
            for column in registered.get("columns", [])
            if column in fields
        ]
        return StructType(merged_fields or schema.fields)
