from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ecommerce_pipeline.config import load_config

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("ecommerce-pipeline-tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.jars.ivy", "/tmp/pyspark-ivy")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def local_config(tmp_path: Path):
    config = load_config("configs/local.yaml")
    return replace(
        config,
        lakehouse=replace(
            config.lakehouse,
            base_path=str(tmp_path / "lake"),
        ),
        batch=replace(config.batch, watermark_path=str(tmp_path / "state" / "watermarks.json")),
    )
