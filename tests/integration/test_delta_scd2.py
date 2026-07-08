from __future__ import annotations

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
        initial = spark.createDataFrame(
            [(1, "alice@example.com", "active")],
            ["source_customer_id", "email", "customer_status"],
        )
        initial = add_scd2_hash(initial, ["email", "customer_status"])
        scd2_merge(spark, initial, path, natural_keys=["source_customer_id"])

        changed = spark.createDataFrame(
            [(1, "alice.new@example.com", "active")],
            ["source_customer_id", "email", "customer_status"],
        )
        changed = add_scd2_hash(changed, ["email", "customer_status"])
        scd2_merge(spark, changed, path, natural_keys=["source_customer_id"])

        rows = spark.read.format("delta").load(path).orderBy("is_current", ascending=False).collect()

        assert len(rows) == 2
        assert {"is_current", "start_date", "end_date"}.issubset(set(rows[0].asDict()))
        assert sum(1 for row in rows if row["is_current"]) == 1
        assert any(row["email"] == "alice@example.com" and not row["is_current"] and row["end_date"] for row in rows)
        assert any(row["email"] == "alice.new@example.com" and row["is_current"] for row in rows)
    finally:
        spark.stop()
