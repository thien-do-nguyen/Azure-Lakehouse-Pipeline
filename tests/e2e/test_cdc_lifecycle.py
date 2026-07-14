from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from ecommerce_pipeline.config import load_config
from ecommerce_pipeline.spark import build_spark

ROOT = Path(__file__).resolve().parents[2]
CONFIG = "configs/local.yaml"


def _run(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    command_env = os.environ.copy()
    command_env["PYTHONPATH"] = str(ROOT / "src")
    if env:
        command_env.update(env)
    return subprocess.run(
        args,
        cwd=ROOT,
        env=command_env,
        check=True,
        text=True,
        capture_output=True,
        timeout=600,
    )


def _python(module: str, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return _run(sys.executable, "-m", module, *args, env=env)


@pytest.mark.e2e
def test_cdc_insert_update_delete_restart_and_ledger() -> None:
    if os.getenv("RUN_CDC_E2E") != "1":
        pytest.skip("Set RUN_CDC_E2E=1 to reset and run the Docker CDC lifecycle test.")

    generated_paths = [
        ROOT / "data/lake/bronze",
        ROOT / "data/lake/silver",
        ROOT / "data/lake/gold",
        ROOT / "data/lake/_control",
        ROOT / "data/checkpoints",
        ROOT / "data/state",
    ]
    _run("docker", "compose", "--profile", "cdc", "down", "-v")
    for path in generated_paths:
        shutil.rmtree(path, ignore_errors=True)

    try:
        _run("docker", "compose", "--profile", "cdc", "up", "-d", "--wait")
        _python(
            "ecommerce_pipeline.seed.synthetic_data",
            "--config",
            CONFIG,
            "--customers",
            "10",
            "--orders",
            "20",
            "--reset",
        )
        _python("ecommerce_pipeline.jobs.run_batch", "--config", CONFIG, "--validate")
        _python("ecommerce_pipeline.cdc.register_connector", "--config", CONFIG, "--wait")
        stream_env = {"STREAMING_ENABLED": "true"}
        _python("ecommerce_pipeline.jobs.run_streaming", "--config", CONFIG, "--once", env=stream_env)

        _python(
            "ecommerce_pipeline.seed.synthetic_data",
            "--config",
            CONFIG,
            "--continuous",
            "--orders-per-batch",
            "2",
            "--interval-seconds",
            "0",
            "--max-batches",
            "1",
        )
        config = load_config(CONFIG)
        with psycopg.connect(config.postgres.psycopg_dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM customer_app.order_items "
                    "WHERE order_item_id = (SELECT max(order_item_id) FROM customer_app.order_items) "
                    "RETURNING order_item_id"
                )
                assert cursor.fetchone() is not None
            connection.commit()

        _python("ecommerce_pipeline.jobs.run_streaming", "--config", CONFIG, "--once", env=stream_env)
        # A clean restart with no new events must preserve the exact table state.
        _python("ecommerce_pipeline.jobs.run_streaming", "--config", CONFIG, "--once", env=stream_env)
        # Losing the Silver checkpoint replays Bronze with a different micro-batch
        # boundary. The offset fingerprint must process it safely instead of
        # confusing the new batch_id=0 with an older committed batch.
        shutil.rmtree(ROOT / "data/checkpoints/silver_unified", ignore_errors=True)
        _python("ecommerce_pipeline.jobs.run_streaming", "--config", CONFIG, "--once", env=stream_env)
        _python("ecommerce_pipeline.jobs.run_batch", "--config", CONFIG, "--layers", "", "--validate")

        spark = build_spark(config)
        try:
            ledger = spark.read.format("delta").load(config.lakehouse.table_path("_control", "cdc_batch_commits"))
            assert ledger.where(F.col("status") == "FAILED").count() == 0
            assert ledger.where(F.col("status") == "COMMITTED").count() >= 3

            silver_items = spark.read.format("delta").load(config.lakehouse.table_path("silver", "order_items"))
            facts = spark.read.format("delta").load(config.lakehouse.table_path("gold", "fact_sales"))
            assert silver_items.count() == facts.count()

            silver_history = DeltaTable.forPath(
                spark, config.lakehouse.table_path("silver", "order_items")
            ).history()
            operations = [row["operation"] for row in silver_history.where(F.col("version") > 0).collect()]
            assert "WRITE" not in operations
            assert "MERGE" in operations
        finally:
            spark.stop()
    finally:
        _run("docker", "compose", "--profile", "cdc", "down", "-v")
