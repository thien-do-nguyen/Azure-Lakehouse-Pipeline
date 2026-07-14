from __future__ import annotations

from datetime import datetime

from ecommerce_pipeline.cdc import ledger as ledger_module
from ecommerce_pipeline.cdc.ledger import DeltaBatchLedger


def test_delta_ledger_writes_status_and_partition_watermarks(monkeypatch, spark, local_config) -> None:
    writes = []
    monkeypatch.setattr(DeltaBatchLedger, "_ensure_tables", lambda _self: None)
    monkeypatch.setattr(
        ledger_module,
        "upsert_to_delta",
        lambda _spark, df, path, keys: writes.append((df.collect(), path, keys)),
    )
    ledger = DeltaBatchLedger(local_config, spark, include_gold=True)

    events = spark.createDataFrame(
        [
            ("orders", "topic.orders", 0, 10, datetime(2026, 1, 1), True),
            ("orders", "topic.orders", 0, 12, datetime(2026, 1, 2), True),
        ],
        ["table_name", "_kafka_topic", "_kafka_partition", "_kafka_offset", "event_ts", "is_valid_event"],
    )
    batch_key = ledger.batch_key(events)
    ledger.mark_processing(batch_key, 7, 2)
    ledger.record_watermarks(events, 7)
    ledger.mark_committed(batch_key, 7, 2)

    assert writes[0][0][0]["status"] == "PROCESSING"
    assert writes[1][0][0]["last_offset"] == 12
    assert writes[1][2] == ["pipeline_id", "table_name", "topic", "partition"]
    assert writes[2][0][0]["status"] == "COMMITTED"
    assert writes[2][0][0]["batch_key"] == batch_key
    assert "silver_gold" in ledger.pipeline_id


def test_delta_ledger_records_truncated_failure(monkeypatch, spark, local_config) -> None:
    rows = []
    monkeypatch.setattr(DeltaBatchLedger, "_ensure_tables", lambda _self: None)
    monkeypatch.setattr(ledger_module, "upsert_to_delta", lambda _spark, df, _path, _keys: rows.extend(df.collect()))
    ledger = DeltaBatchLedger(local_config, spark, include_gold=False)

    ledger.mark_failed("batch-key", 8, 1, RuntimeError("x" * 5000))

    assert rows[0]["status"] == "FAILED"
    assert len(rows[0]["error_message"]) == 4000


def test_delta_ledger_detects_committed_fingerprint(spark) -> None:
    committed = spark.createDataFrame(
        [("pipeline", "fingerprint", "COMMITTED")],
        ["pipeline_id", "batch_key", "status"],
    )

    class FakeRead:
        def format(self, _value):
            return self

        def load(self, _path):
            return committed

    class FakeSpark:
        read = FakeRead()

    ledger = object.__new__(DeltaBatchLedger)
    ledger.spark = FakeSpark()
    ledger.pipeline_id = "pipeline"
    ledger.ledger_path = "/control/ledger"

    assert ledger.is_committed("fingerprint")
    assert not ledger.is_committed("different")
