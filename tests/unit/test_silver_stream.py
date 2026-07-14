from __future__ import annotations

from dataclasses import replace

import pytest

from ecommerce_pipeline.jobs import silver_stream
from ecommerce_pipeline.jobs.silver_stream import ensure_bronze_cdc_table, process_cdc_batch, run_silver_stream


def test_run_silver_stream_requires_enabled(spark, local_config) -> None:
    config = replace(local_config, streaming=replace(local_config.streaming, enabled=False))

    with pytest.raises(RuntimeError, match="Streaming is disabled"):
        run_silver_stream(config, spark)


def test_ensure_bronze_cdc_table_skips_non_delta(spark, local_config) -> None:
    config = replace(local_config, streaming=replace(local_config.streaming, storage_format="parquet"))

    assert ensure_bronze_cdc_table(config, spark) is None


def test_process_cdc_batch_commits_all_layers(monkeypatch, spark, local_config) -> None:
    calls = []

    class Handler:
        def merge_batch(self, _df, batch_id):
            calls.append(("silver", batch_id))

    class Ledger:
        pipeline_id = "pipeline"

        def batch_key(self, _df):
            return "fingerprint"

        def is_committed(self, _key):
            return False

        def mark_processing(self, *args):
            calls.append(("processing", *args))

        def record_watermarks(self, _df, batch_id):
            calls.append(("watermarks", batch_id))

        def mark_committed(self, *args):
            calls.append(("committed", *args))

        def mark_failed(self, *args):
            calls.append(("failed", *args))

    monkeypatch.setattr(silver_stream, "update_gold_after_silver_batch", lambda *_args: calls.append("gold"))
    batch = spark.createDataFrame([(1,)], ["id"])

    process_cdc_batch(local_config, spark, batch, 4, Handler(), Ledger(), True)

    assert calls == [
        ("processing", "fingerprint", 4, 1),
        ("silver", 4),
        "gold",
        ("watermarks", 4),
        ("committed", "fingerprint", 4, 1),
    ]


def test_process_cdc_batch_skips_committed_fingerprint(spark, local_config) -> None:
    calls = []

    class Handler:
        def merge_batch(self, *_args):
            calls.append("unexpected")

    class Ledger:
        pipeline_id = "pipeline"

        def batch_key(self, _df):
            return "fingerprint"

        def is_committed(self, _key):
            return True

    process_cdc_batch(local_config, spark, spark.createDataFrame([(1,)], ["id"]), 4, Handler(), Ledger(), True)

    assert calls == []
