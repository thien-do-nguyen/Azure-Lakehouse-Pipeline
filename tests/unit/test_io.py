from __future__ import annotations

from ecommerce_pipeline.io import read_layer_table, write_layer_table


def test_write_and_read_layer_table(spark, local_config) -> None:
    df = spark.createDataFrame([(1, "ok")], ["id", "status"])

    write_layer_table(df, local_config, "bronze", "unit_table")
    rows = read_layer_table(spark, local_config, "bronze", "unit_table").collect()

    assert rows[0]["status"] == "ok"
