from __future__ import annotations

from ecommerce_pipeline.jobs.silver import _clean_table


def test_clean_table_trims_strings_drops_metadata_and_dedupes(spark) -> None:
    df = spark.createDataFrame(
        [
            (1, " alice ", "bronze"),
            (1, " alice ", "bronze"),
            (2, " bob ", "bronze"),
        ],
        ["id", "name", "_source_table"],
    )

    result = _clean_table(df, ["id"])

    assert result.count() == 2
    assert "_source_table" not in result.columns
    assert [row["name"] for row in result.orderBy("id").collect()] == ["alice", "bob"]
