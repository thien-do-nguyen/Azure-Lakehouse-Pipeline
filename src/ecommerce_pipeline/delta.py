from __future__ import annotations

from collections.abc import Sequence
from functools import reduce
from operator import and_

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def _delta_table_for_path(spark: SparkSession, path: str):
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:  # pragma: no cover - environment guard
        raise RuntimeError("Install delta-spark to use Delta Lake merge operations.") from exc

    return DeltaTable.forPath(spark, path)


def _ensure_delta_table(spark: SparkSession, df: DataFrame, path: str) -> bool:
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:  # pragma: no cover - environment guard
        raise RuntimeError("Install delta-spark to use Delta Lake merge operations.") from exc

    if DeltaTable.isDeltaTable(spark, path):
        return False

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    return True


def _join_condition(keys: Sequence[str], target_alias: str = "target", source_alias: str = "source") -> str:
    return " AND ".join(f"{target_alias}.{key} = {source_alias}.{key}" for key in keys)


def upsert_to_delta(spark: SparkSession, df: DataFrame, path: str, keys: Sequence[str]) -> None:
    if not keys:
        raise ValueError("upsert_to_delta requires at least one merge key.")
    if _ensure_delta_table(spark, df, path):
        return

    delta_table = _delta_table_for_path(spark, path)
    (
        delta_table.alias("target")
        .merge(df.alias("source"), _join_condition(keys))
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def synchronize_to_delta(spark: SparkSession, df: DataFrame, path: str, keys: Sequence[str]) -> None:
    """Make a Delta table match a complete source snapshot, including deletes."""
    if not keys:
        raise ValueError("synchronize_to_delta requires at least one merge key.")
    if _ensure_delta_table(spark, df, path):
        return
    delta_table = _delta_table_for_path(spark, path)
    (
        delta_table.alias("target")
        .merge(df.alias("source"), _join_condition(keys))
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )


def replace_delta_scope(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    scope_df: DataFrame,
    scope_key: str,
    keys: Sequence[str],
) -> None:
    """Replace only target rows in an explicitly supplied business-key scope."""
    if not keys:
        raise ValueError("replace_delta_scope requires at least one merge key.")
    if _ensure_delta_table(spark, df, path):
        return

    delta_table = _delta_table_for_path(spark, path)
    scoped_keys = scope_df.select(scope_key).where(F.col(scope_key).isNotNull()).distinct()
    (
        delta_table.alias("target")
        .merge(scoped_keys.alias("scope"), f"target.{scope_key} = scope.{scope_key}")
        .whenMatchedDelete()
        .execute()
    )
    upsert_to_delta(spark, df, path, keys)


def add_scd2_hash(df: DataFrame, tracked_columns: Sequence[str], hash_column: str = "scd_hash") -> DataFrame:
    return df.withColumn(
        hash_column,
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(column).cast("string"), F.lit("NA")) for column in tracked_columns]), 256),
    )


def scd2_merge(
    spark: SparkSession,
    source_df: DataFrame,
    path: str,
    natural_keys: Sequence[str],
    tracked_hash_column: str = "scd_hash",
    start_date_col: str = "start_date",
    end_date_col: str = "end_date",
    current_col: str = "is_current",
    effective_timestamp_col: str | None = None,
    surrogate_key_col: str | None = None,
    type1_columns: Sequence[str] = (),
) -> None:
    if not natural_keys:
        raise ValueError("scd2_merge requires at least one natural key.")

    effective_timestamp = (
        F.coalesce(F.col(effective_timestamp_col), F.current_timestamp())
        if effective_timestamp_col
        else F.current_timestamp()
    )
    prepared_source = source_df.withColumn("_scd_effective_at", effective_timestamp)
    if start_date_col not in prepared_source.columns:
        prepared_source = prepared_source.withColumn(start_date_col, F.col("_scd_effective_at"))
    prepared_source = prepared_source.withColumn(
        end_date_col, F.lit("9999-12-31 00:00:00").cast("timestamp")
    ).withColumn(current_col, F.lit(True))

    if surrogate_key_col and surrogate_key_col not in prepared_source.columns:
        prepared_source = prepared_source.withColumn(
            surrogate_key_col,
            (
                F.pmod(
                    F.xxhash64(
                        *[F.col(key).cast("string") for key in natural_keys],
                        F.col(tracked_hash_column),
                        F.col(start_date_col).cast("string"),
                    ),
                    F.lit(9223372036854775806),
                )
                + F.lit(1)
            ).cast("long"),
        )

    initial_source = prepared_source.drop("_scd_effective_at")
    if _ensure_delta_table(spark, initial_source, path):
        return

    delta_table = _delta_table_for_path(spark, path)
    current_target = spark.read.format("delta").load(path).where(F.col(current_col) == F.lit(True))
    target_columns = set(current_target.columns)
    change_condition = (
        " AND ".join(f"target.{key} = source.{key}" for key in natural_keys)
        + f" AND target.{current_col} = true"
        + f" AND target.{tracked_hash_column} <> source.{tracked_hash_column}"
    )
    close_updates = {
        current_col: "false",
        end_date_col: "source._scd_effective_at",
    }
    if "updated_at" in target_columns:
        close_updates["updated_at"] = "current_timestamp()"

    (
        delta_table.alias("target")
        .merge(prepared_source.alias("source"), change_condition)
        .whenMatchedUpdate(set=close_updates)
        .execute()
    )

    target_history = spark.read.format("delta").load(path)
    latest_window = Window.partitionBy(*natural_keys).orderBy(
        F.col(start_date_col).desc_nulls_last(),
        F.col(end_date_col).desc_nulls_last(),
    )
    latest_target = (
        target_history.withColumn("_scd_rank", F.row_number().over(latest_window))
        .where(F.col("_scd_rank") == 1)
        .drop("_scd_rank")
    )
    natural_key_condition = reduce(
        and_,
        [F.col(f"source.{key}") == F.col(f"target.{key}") for key in natural_keys],
    )
    target_projection = [*natural_keys, tracked_hash_column, start_date_col]
    joined = prepared_source.alias("source").join(
        latest_target.select(*target_projection).alias("target"), natural_key_condition, "left"
    )
    is_new = F.col(f"target.{tracked_hash_column}").isNull()
    is_changed = ~is_new & (
        F.col(f"target.{tracked_hash_column}") != F.col(f"source.{tracked_hash_column}")
    )
    new_versions = joined.where(is_new | is_changed).select(
        *[
            (
                F.when(is_new, F.col(f"source.{start_date_col}"))
                .otherwise(F.col("source._scd_effective_at"))
                .alias(start_date_col)
                if column == start_date_col
                else F.col(f"source.{column}").alias(column)
            )
            for column in prepared_source.columns
            if column != "_scd_effective_at"
        ]
    )
    if surrogate_key_col:
        new_versions = new_versions.withColumn(
            surrogate_key_col,
            (
                F.pmod(
                    F.xxhash64(
                        *[F.col(key).cast("string") for key in natural_keys],
                        F.col(tracked_hash_column),
                        F.col(start_date_col).cast("string"),
                    ),
                    F.lit(9223372036854775806),
                )
                + F.lit(1)
            ).cast("long"),
        )
    new_versions.write.format("delta").mode("append").save(path)

    if type1_columns:
        updates = {column: f"source.{column}" for column in type1_columns if column in target_columns}
        if "updated_at" in target_columns:
            updates["updated_at"] = "current_timestamp()"
        if updates:
            same_version_condition = (
                " AND ".join(f"target.{key} = source.{key}" for key in natural_keys)
                + f" AND target.{current_col} = true"
                + f" AND target.{tracked_hash_column} = source.{tracked_hash_column}"
            )
            (
                delta_table.alias("target")
                .merge(prepared_source.alias("source"), same_version_condition)
                .whenMatchedUpdate(set=updates)
                .execute()
            )
