from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, DecimalType, NullType, StructField, StructType

from ecommerce_pipeline.filesystem import ensure_parent, exists, open_text

_NUMERIC_RANK = {
    "byte": 0,
    "short": 1,
    "integer": 2,
    "long": 3,
    "float": 4,
    "double": 5,
}


class SchemaCompatibilityError(RuntimeError):
    """Raised before a breaking CDC schema can be merged into current tables."""


@dataclass(frozen=True)
class TypeChange:
    column: str
    previous_type: str
    current_type: str
    compatible: bool


@dataclass(frozen=True)
class SchemaChange:
    table_name: str
    added_columns: list[str]
    removed_columns: list[str]
    type_changes: list[TypeChange] = field(default_factory=list)
    previous_primary_keys: list[str] = field(default_factory=list)
    current_primary_keys: list[str] = field(default_factory=list)
    added_required_columns: list[str] = field(default_factory=list)

    @property
    def breaking_reasons(self) -> list[str]:
        reasons = [f"removed columns: {', '.join(self.removed_columns)}"] if self.removed_columns else []
        reasons.extend(
            f"datatype changed for {change.column}: {change.previous_type} -> {change.current_type}"
            for change in self.type_changes
            if not change.compatible
        )
        if self.previous_primary_keys and self.previous_primary_keys != self.current_primary_keys:
            reasons.append(
                "primary key changed: "
                f"{','.join(self.previous_primary_keys)} -> {','.join(self.current_primary_keys)}"
            )
        if self.added_required_columns:
            reasons.append(f"added non-nullable columns: {', '.join(self.added_required_columns)}")
        return reasons

    @property
    def is_compatible(self) -> bool:
        return not self.breaking_reasons

    @property
    def classification(self) -> str:
        return "compatible" if self.is_compatible else "breaking"


def _type_is_compatible(previous: DataType, current: DataType) -> bool:
    if previous == current or isinstance(previous, NullType) or isinstance(current, NullType):
        return True
    previous_name = previous.typeName()
    current_name = current.typeName()
    if previous_name in _NUMERIC_RANK and current_name in _NUMERIC_RANK:
        return _NUMERIC_RANK[current_name] >= _NUMERIC_RANK[previous_name]
    if isinstance(previous, DecimalType) and isinstance(current, DecimalType):
        previous_integer_digits = previous.precision - previous.scale
        current_integer_digits = current.precision - current.scale
        return current.scale >= previous.scale and current_integer_digits >= previous_integer_digits
    return False


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

    def _hydrate_registered_types(self, table_name: str, schema: StructType) -> StructType:
        registered = self.load().get(table_name)
        if not registered:
            return schema
        previous = {
            item.name: item for item in StructType.fromJson(registered["spark_schema"]).fields
        }
        return StructType(
            [
                previous.get(field.name, field)
                if isinstance(field.dataType, NullType)
                else field
                for field in schema.fields
            ]
        )

    def _merge_observed_schema(self, table_name: str, observed: StructType) -> StructType:
        """Keep registered fields that a schemaless Debezium row may omit.

        PostgreSQL logical events can omit generated columns even though snapshot
        rows contain them. Removal detection therefore belongs to validation of a
        complete source contract, while row-event resolution is an additive union.
        """
        registered = self.load().get(table_name)
        if not registered:
            return observed
        previous = StructType.fromJson(registered["spark_schema"])
        observed_names = set(observed.fieldNames())
        return StructType(
            [*observed.fields, *[field for field in previous.fields if field.name not in observed_names]]
        )

    def infer_schema(self, events: DataFrame) -> StructType:
        payloads = events.select("record_json").where(F.col("record_json").isNotNull())
        if payloads.isEmpty():
            return StructType([])
        inferred = events.sparkSession.read.json(payloads.rdd.map(lambda row: row["record_json"])).schema
        observed = (
            payloads.select(
                F.explode(
                    F.map_entries(F.from_json("record_json", "map<string,string>"))
                ).alias("entry")
            )
            .select(F.col("entry.key").alias("column_name"), F.col("entry.value").alias("column_value"))
            .groupBy("column_name")
            .agg(F.max(F.col("column_value").isNotNull().cast("int")).alias("has_non_null_value"))
            .collect()
        )
        observed_columns = {row["column_name"] for row in observed}
        all_null_columns = {
            row["column_name"] for row in observed if row["has_non_null_value"] == 0
        }
        inferred_fields = [
            StructField(field.name, NullType(), True)
            if field.name in all_null_columns
            else field
            for field in inferred.fields
        ]
        inferred_names = {field.name for field in inferred_fields}
        missing_fields = [
            StructField(column, NullType(), True)
            for column in sorted(observed_columns - inferred_names)
        ]
        return StructType([*inferred_fields, *missing_fields])

    def validate_compatibility(
        self,
        table_name: str,
        schema: StructType,
        primary_keys: list[str] | None = None,
    ) -> SchemaChange:
        registered = self.load().get(table_name)
        current_keys = list(primary_keys or [])
        if not registered:
            return SchemaChange(
                table_name=table_name,
                added_columns=schema.fieldNames(),
                removed_columns=[],
                current_primary_keys=current_keys,
            )

        previous_schema = StructType.fromJson(registered["spark_schema"])
        previous_fields = {item.name: item for item in previous_schema.fields}
        current_fields = {item.name: item for item in schema.fields}
        added = [name for name in current_fields if name not in previous_fields]
        removed = [name for name in previous_fields if name not in current_fields]
        changes = [
            TypeChange(
                column=name,
                previous_type=previous_fields[name].dataType.simpleString(),
                current_type=current_fields[name].dataType.simpleString(),
                compatible=_type_is_compatible(previous_fields[name].dataType, current_fields[name].dataType),
            )
            for name in previous_fields.keys() & current_fields.keys()
            if previous_fields[name].dataType != current_fields[name].dataType
        ]
        previous_keys = list(registered.get("primary_keys", []))
        return SchemaChange(
            table_name=table_name,
            added_columns=added,
            removed_columns=removed,
            type_changes=sorted(changes, key=lambda item: item.column),
            previous_primary_keys=previous_keys,
            current_primary_keys=current_keys or previous_keys,
            added_required_columns=[name for name in added if not current_fields[name].nullable],
        )

    def register_or_update(
        self,
        table_name: str,
        schema: StructType,
        primary_keys: list[str] | None = None,
    ) -> SchemaChange:
        schema = self._hydrate_registered_types(table_name, schema)
        change = self.validate_compatibility(table_name, schema, primary_keys)
        if not change.is_compatible:
            raise SchemaCompatibilityError(
                f"Breaking CDC schema for {table_name}: {'; '.join(change.breaking_reasons)}"
            )
        schemas = self.load()
        schemas[table_name] = {
            "columns": schema.fieldNames(),
            "primary_keys": list(primary_keys or []),
            "spark_schema": schema.jsonValue(),
        }
        self.save(schemas)
        return change

    def resolve_table_schema(
        self,
        table_name: str,
        events: DataFrame,
        primary_keys: list[str] | None = None,
    ) -> StructType:
        observed = self.infer_schema(events)
        schema = self._hydrate_registered_types(
            table_name,
            self._merge_observed_schema(table_name, observed),
        )
        self.register_or_update(table_name, schema, primary_keys)
        return schema
