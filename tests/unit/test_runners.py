from __future__ import annotations

from argparse import Namespace
from contextlib import contextmanager

import pytest

from ecommerce_pipeline.jobs import run_batch, run_streaming
from ecommerce_pipeline.jobs.validate import ValidationResult


class _Spark:
    def __init__(self) -> None:
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


class _Metrics:
    @contextmanager
    def timer(self, *_args, **_kwargs):
        yield


class _Query:
    def __init__(self) -> None:
        self.isActive = True
        self.processed = False

    def processAllAvailable(self) -> None:
        self.processed = True

    def stop(self) -> None:
        self.isActive = False


def test_batch_runner_executes_layers_and_validation(monkeypatch, local_config) -> None:
    spark = _Spark()
    calls: list[str] = []
    monkeypatch.setattr(run_batch, "parse_args", lambda: Namespace(config="local", env=None, layers="bronze,silver,gold", validate=True))
    monkeypatch.setattr(run_batch, "load_config", lambda *_args, **_kwargs: local_config)
    monkeypatch.setattr(run_batch, "build_spark", lambda _config: spark)
    monkeypatch.setattr(run_batch, "MetricsCollector", _Metrics)
    monkeypatch.setattr(run_batch, "run_bronze", lambda *_args: calls.append("bronze"))
    monkeypatch.setattr(run_batch, "run_silver", lambda *_args: calls.append("silver"))
    monkeypatch.setattr(run_batch, "run_gold", lambda *_args: calls.append("gold"))
    monkeypatch.setattr(run_batch, "run_validations", lambda *_args: [ValidationResult("ok", True, "0", "0")])
    monkeypatch.setattr(run_batch, "print_results", lambda _results: calls.append("validate"))

    run_batch.main()

    assert calls == ["bronze", "silver", "gold", "validate"]
    assert spark.stopped is True


def test_batch_runner_fails_on_validation_error_and_stops_spark(monkeypatch, local_config) -> None:
    spark = _Spark()
    monkeypatch.setattr(run_batch, "parse_args", lambda: Namespace(config="local", env=None, layers="", validate=True))
    monkeypatch.setattr(run_batch, "load_config", lambda *_args, **_kwargs: local_config)
    monkeypatch.setattr(run_batch, "build_spark", lambda _config: spark)
    monkeypatch.setattr(run_batch, "MetricsCollector", _Metrics)
    monkeypatch.setattr(run_batch, "run_validations", lambda *_args: [ValidationResult("broken", False, "0", "1")])
    monkeypatch.setattr(run_batch, "print_results", lambda _results: None)

    with pytest.raises(RuntimeError, match="Reconciliation failed: broken"):
        run_batch.main()
    assert spark.stopped is True


def test_streaming_runner_once_processes_and_stops_queries(monkeypatch, local_config) -> None:
    spark = _Spark()
    bronze = [_Query(), _Query()]
    silver = _Query()
    monkeypatch.setattr(run_streaming, "parse_args", lambda: Namespace(config="local", layers="bronze,silver,gold", once=True))
    monkeypatch.setattr(run_streaming, "load_config", lambda _path: local_config)
    monkeypatch.setattr(run_streaming, "build_spark", lambda _config: spark)
    monkeypatch.setattr(run_streaming, "MetricsCollector", _Metrics)
    monkeypatch.setattr(run_streaming, "configure_logging", lambda: None)
    monkeypatch.setattr(run_streaming.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(run_streaming, "ensure_bronze_cdc_table", lambda *_args: None)
    monkeypatch.setattr(run_streaming, "run_bronze_stream", lambda *_args: bronze)
    monkeypatch.setattr(run_streaming, "run_silver_stream", lambda *_args, **_kwargs: silver)

    run_streaming.main()

    assert all(query.processed and not query.isActive for query in [*bronze, silver])
    assert spark.stopped is True


def test_streaming_runner_rejects_gold_without_silver(monkeypatch, local_config) -> None:
    spark = _Spark()
    monkeypatch.setattr(run_streaming, "parse_args", lambda: Namespace(config="local", layers="gold", once=True))
    monkeypatch.setattr(run_streaming, "load_config", lambda _path: local_config)
    monkeypatch.setattr(run_streaming, "build_spark", lambda _config: spark)
    monkeypatch.setattr(run_streaming, "MetricsCollector", _Metrics)
    monkeypatch.setattr(run_streaming.signal, "signal", lambda *_args: None)

    with pytest.raises(RuntimeError, match="requires the silver layer"):
        run_streaming.main()
    assert spark.stopped is True
