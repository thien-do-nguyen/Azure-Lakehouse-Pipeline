from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
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
    result = subprocess.run(
        args,
        cwd=ROOT,
        env=command_env,
        check=False,
        text=True,
        capture_output=True,
        timeout=600,
    )
    if result.returncode != 0:
        command = " ".join(args)
        raise RuntimeError(
            f"Command failed ({result.returncode}): {command}\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{result.stderr}"
        )
    return result


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
        config = load_config(CONFIG)

        with psycopg.connect(config.postgres.psycopg_dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT customer_id FROM customer_app.orders ORDER BY created_at, order_id LIMIT 1"
                )
                customer_id = cursor.fetchone()[0]
                type1_at = datetime.now(timezone.utc).replace(tzinfo=None)
                cursor.execute(
                    "UPDATE customer_app.app_users SET last_login = %s, updated_at = %s WHERE user_id = %s",
                    (type1_at, type1_at, customer_id),
                )
            connection.commit()
        _python("ecommerce_pipeline.jobs.run_streaming", "--config", CONFIG, "--once", env=stream_env)

        with psycopg.connect(config.postgres.psycopg_dsn) as connection:
            with connection.cursor() as cursor:
                type2_at = datetime.now(timezone.utc).replace(tzinfo=None)
                cursor.execute(
                    "UPDATE customer_app.app_users "
                    "SET email = %s, updated_at = %s WHERE user_id = %s",
                    (f"scd2-{customer_id}@example.com", type2_at, customer_id),
                )
            connection.commit()
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

            customer_versions = (
                spark.read.format("delta")
                .load(config.lakehouse.table_path("gold", "dim_customer"))
                .where(F.col("source_customer_id") == customer_id)
            )
            version_rows = customer_versions.orderBy("start_date").collect()
            assert len(version_rows) == 2
            assert len({row["customer_key"] for row in version_rows}) == 2
            assert sum(1 for row in version_rows if row["is_current"]) == 1
            expected_type1_at = type1_at.replace(microsecond=(type1_at.microsecond // 1000) * 1000)
            expected_type2_at = type2_at.replace(microsecond=(type2_at.microsecond // 1000) * 1000)
            assert version_rows[0]["last_login_at"] == expected_type1_at
            assert version_rows[0]["end_date"] == expected_type2_at
            historical_key = version_rows[0]["customer_key"]
            fact_customer_keys = {
                row["customer_key"]
                for row in facts.where(
                    (F.col("source_customer_id") == customer_id) & (F.col("order_created_at") < F.lit(type2_at))
                )
                .select("customer_key")
                .distinct()
                .collect()
            }
            assert fact_customer_keys == {historical_key}

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
