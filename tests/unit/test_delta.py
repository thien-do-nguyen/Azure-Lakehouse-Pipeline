from __future__ import annotations

from ecommerce_pipeline import delta
from ecommerce_pipeline.delta import _join_condition, add_scd2_hash, synchronize_to_delta


def test_join_condition() -> None:
    assert _join_condition(["id", "line_id"]) == "target.id = source.id AND target.line_id = source.line_id"


def test_add_scd2_hash_is_stable(spark) -> None:
    df = spark.createDataFrame([(1, "alice", "active"), (1, "alice", "active")], ["id", "name", "status"])

    rows = add_scd2_hash(df, ["name", "status"]).select("scd_hash").collect()

    assert rows[0]["scd_hash"] == rows[1]["scd_hash"]


def test_synchronize_to_delta_deletes_rows_missing_from_complete_source(monkeypatch, spark) -> None:
    calls = []

    class FakeMerge:
        def whenMatchedUpdateAll(self):
            calls.append("update")
            return self

        def whenNotMatchedInsertAll(self):
            calls.append("insert")
            return self

        def whenNotMatchedBySourceDelete(self):
            calls.append("delete")
            return self

        def execute(self):
            calls.append("execute")

    class FakeTable:
        def alias(self, _name):
            return self

        def merge(self, _source, condition):
            calls.append(condition)
            return FakeMerge()

    monkeypatch.setattr(delta, "_ensure_delta_table", lambda *_args: False)
    monkeypatch.setattr(delta, "_delta_table_for_path", lambda *_args: FakeTable())

    synchronize_to_delta(spark, spark.createDataFrame([(1,)], ["id"]), "/lake/fact", ["id"])

    assert calls == ["target.id = source.id", "update", "insert", "delete", "execute"]
