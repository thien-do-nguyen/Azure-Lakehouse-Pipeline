from __future__ import annotations

from ecommerce_pipeline.delta import _join_condition, add_scd2_hash


def test_join_condition() -> None:
    assert _join_condition(["id", "line_id"]) == "target.id = source.id AND target.line_id = source.line_id"


def test_add_scd2_hash_is_stable(spark) -> None:
    df = spark.createDataFrame([(1, "alice", "active"), (1, "alice", "active")], ["id", "name", "status"])

    rows = add_scd2_hash(df, ["name", "status"]).select("scd_hash").collect()

    assert rows[0]["scd_hash"] == rows[1]["scd_hash"]
