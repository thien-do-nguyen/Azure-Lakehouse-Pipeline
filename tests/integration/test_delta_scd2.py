from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from ecommerce_pipeline.delta import add_scd2_hash, scd2_merge

pytest.importorskip("delta")


@pytest.mark.integration
def test_scd2_merge_closes_old_record_and_inserts_new(tmp_path) -> None:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("delta-scd2-test")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.jars.ivy", "/tmp/pyspark-ivy")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    path = str(tmp_path / "dim_customer")
    try:
        registered = datetime(2026, 1, 1)
        columns = [
            "source_customer_id",
            "email",
            "customer_status",
            "start_date",
            "source_updated_at",
            "last_login_at",
        ]
        initial = spark.createDataFrame(
            [(1, "alice@example.com", "active", registered, registered, registered)],
            columns,
        )
        initial = add_scd2_hash(initial, ["email", "customer_status"])
        merge_options = {
            "natural_keys": ["source_customer_id"],
            "effective_timestamp_col": "source_updated_at",
            "surrogate_key_col": "customer_key",
            "type1_columns": ["last_login_at"],
        }
        scd2_merge(spark, initial, path, **merge_options)

        login_at = registered + timedelta(hours=1)
        type1 = spark.createDataFrame(
            [(1, "alice@example.com", "active", registered, login_at, login_at)],
            columns,
        )
        type1 = add_scd2_hash(type1, ["email", "customer_status"])
        scd2_merge(spark, type1, path, **merge_options)

        changed_at = registered + timedelta(days=1)
        changed = spark.createDataFrame(
            [(1, "alice.new@example.com", "active", registered, changed_at, login_at)],
            columns,
        )
        changed = add_scd2_hash(changed, ["email", "customer_status"])
        scd2_merge(spark, changed, path, **merge_options)
        # Replaying the same latest state must be idempotent and must not reopen
        # the original interval or create another surrogate key.
        scd2_merge(spark, changed, path, **merge_options)

        rows = spark.read.format("delta").load(path).orderBy("is_current", ascending=False).collect()

        assert len(rows) == 2
        assert len({row["customer_key"] for row in rows}) == 2
        assert {"is_current", "start_date", "end_date"}.issubset(set(rows[0].asDict()))
        assert sum(1 for row in rows if row["is_current"]) == 1
        assert any(
            row["email"] == "alice@example.com"
            and not row["is_current"]
            and row["end_date"] == changed_at
            and row["last_login_at"] == login_at
            for row in rows
        )
        assert any(row["email"] == "alice.new@example.com" and row["is_current"] for row in rows)
    finally:
        spark.stop()
