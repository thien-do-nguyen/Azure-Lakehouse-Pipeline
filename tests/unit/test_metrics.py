from __future__ import annotations

import json

from ecommerce_pipeline.metrics import MetricsCollector


def test_metrics_collector_writes_jsonl(tmp_path) -> None:
    path = tmp_path / "metrics.jsonl"
    collector = MetricsCollector(str(path))

    with collector.timer("unit_test", layer="gold"):
        pass

    event = json.loads(path.read_text(encoding="utf-8").strip())
    assert event["name"] == "unit_test"
    assert event["tags"]["status"] == "success"
    assert event["tags"]["layer"] == "gold"
