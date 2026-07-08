from __future__ import annotations

from dataclasses import replace

import pytest

from ecommerce_pipeline import spark as spark_module


class _FakeSparkContext:
    def __init__(self) -> None:
        self.level = None

    def setLogLevel(self, level: str) -> None:
        self.level = level


class _FakeSession:
    def __init__(self) -> None:
        self.sparkContext = _FakeSparkContext()


class _FakeBuilder:
    def __init__(self) -> None:
        self.calls = []
        self.session = _FakeSession()

    def appName(self, value: str):
        self.calls.append(("appName", value))
        return self

    def master(self, value: str):
        self.calls.append(("master", value))
        return self

    def config(self, key: str, value: str):
        self.calls.append(("config", key, value))
        return self

    def getOrCreate(self):
        self.calls.append(("getOrCreate",))
        return self.session


def test_build_spark_applies_config(monkeypatch, local_config) -> None:
    fake_builder = _FakeBuilder()
    monkeypatch.setattr(spark_module.SparkSession, "builder", fake_builder)

    session = spark_module.build_spark(local_config)

    assert session.sparkContext.level == "WARN"
    assert ("appName", local_config.spark.app_name) in fake_builder.calls
    assert ("master", local_config.spark.master) in fake_builder.calls


def test_build_spark_requires_delta_dependency(monkeypatch, local_config) -> None:
    config = replace(local_config, lakehouse=replace(local_config.lakehouse, format="delta"))
    fake_builder = _FakeBuilder()
    monkeypatch.setattr(spark_module.SparkSession, "builder", fake_builder)
    monkeypatch.setitem(__import__("sys").modules, "delta", None)

    with pytest.raises(RuntimeError, match="Install delta-spark"):
        spark_module.build_spark(config)
