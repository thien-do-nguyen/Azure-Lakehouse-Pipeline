from __future__ import annotations

from ecommerce_pipeline.jobs.silver import _clean_table, _project_table


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


def test_project_table_keeps_only_silver_contract_columns(spark) -> None:
    df = spark.createDataFrame(
        [(1, 10, "ORD-1", "{}", "unused")],
        ["order_id", "customer_id", "order_number", "shipping_address_snapshot", "customer_note"],
    )

    result = _project_table(df, "orders")

    assert "order_id" in result.columns
    assert "customer_id" in result.columns
    assert "order_number" in result.columns
    assert "shipping_address_snapshot" not in result.columns
    assert "customer_note" not in result.columns
