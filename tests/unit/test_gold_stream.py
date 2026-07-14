from __future__ import annotations

from ecommerce_pipeline.jobs import gold_stream


def test_update_gold_after_silver_batch_skips_empty_batch(monkeypatch, spark, local_config) -> None:
    calls = []
    monkeypatch.setattr(gold_stream, "run_gold_incremental", lambda *_args: calls.append("gold"))

    gold_stream.update_gold_after_silver_batch(local_config, spark, spark.createDataFrame([], "id int"))

    assert calls == []


def test_update_gold_after_silver_batch_runs_unified_gold(monkeypatch, spark, local_config) -> None:
    calls = []
    monkeypatch.setattr(gold_stream, "run_gold_incremental", lambda *_args: calls.append("gold"))

    gold_stream.update_gold_after_silver_batch(local_config, spark, spark.createDataFrame([(1,)], ["id"]))

    assert calls == ["gold"]
