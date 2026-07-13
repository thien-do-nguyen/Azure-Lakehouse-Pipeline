from __future__ import annotations

from pathlib import Path

from prefect import flow, task
from pyspark.sql import SparkSession

from ecommerce_pipeline.config import load_config
from ecommerce_pipeline.jobs.bronze import run_bronze
from ecommerce_pipeline.jobs.gold import run_gold
from ecommerce_pipeline.jobs.silver import run_silver
from ecommerce_pipeline.jobs.validate import run_validations
from ecommerce_pipeline.logging import configure_logging, get_logger
from ecommerce_pipeline.spark import build_spark

logger = get_logger(__name__)


# NOTE: SparkSession is NOT serializable by Prefect, so we cannot pass it
# directly as a task argument. Instead, we use Prefect's Task Run Context
# to share a single session across tasks within the same flow run.
#
# A simpler and more robust approach: build spark ONCE in the flow function
# and call the underlying job functions directly (not as Prefect tasks),
# OR use a module-level singleton keyed by config_path.

_SPARK_SESSIONS: dict[str, SparkSession] = {}


def _get_or_create_spark(config_path: str) -> SparkSession:
    """Singleton SparkSession per config_path within this process."""
    if config_path not in _SPARK_SESSIONS:
        config = load_config(config_path)
        _SPARK_SESSIONS[config_path] = build_spark(config)
        logger.info("Created shared SparkSession for config=%s", config_path)
    return _SPARK_SESSIONS[config_path]


def _stop_all_spark() -> None:
    for key, spark in list(_SPARK_SESSIONS.items()):
        try:
            spark.stop()
            logger.info("Stopped SparkSession for config=%s", key)
        except Exception:  # noqa: BLE001
            logger.exception("Error stopping SparkSession for %s", key)
    _SPARK_SESSIONS.clear()


@task(name="bronze")
def bronze_task(config_path: str) -> None:
    config = load_config(config_path)
    spark = _get_or_create_spark(config_path)
    run_bronze(config, spark)


@task(name="silver")
def silver_task(config_path: str) -> None:
    config = load_config(config_path)
    spark = _get_or_create_spark(config_path)
    run_silver(config, spark)


@task(name="gold")
def gold_task(config_path: str) -> None:
    config = load_config(config_path)
    spark = _get_or_create_spark(config_path)
    run_gold(config, spark)


@task(name="validate")
def validate_task(config_path: str) -> None:
    config = load_config(config_path)
    spark = _get_or_create_spark(config_path)
    results = run_validations(config, spark)
    failed = [result for result in results if not result.passed]
    if failed:
        raise RuntimeError(f"Validation failed: {[result.name for result in failed]}")


@flow(name="ecommerce-batch-medallion")
def batch_medallion_flow(config_path: str = "configs/local.yaml") -> None:
    configure_logging()
    logger.info("Starting Prefect medallion flow with config=%s", config_path)
    try:
        bronze_task(config_path)
        silver_task(config_path)
        gold_task(config_path)
        validate_task(config_path)
    finally:
        _stop_all_spark()


def deployment_name(config_path: str) -> str:
    return f"ecommerce-batch-{Path(config_path).stem}"


if __name__ == "__main__":
    batch_medallion_flow()
