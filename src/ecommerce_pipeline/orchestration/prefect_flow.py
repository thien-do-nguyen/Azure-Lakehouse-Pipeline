from __future__ import annotations

from pathlib import Path

from prefect import flow, task

from ecommerce_pipeline.config import load_config
from ecommerce_pipeline.jobs.bronze import run_bronze
from ecommerce_pipeline.jobs.gold import run_gold
from ecommerce_pipeline.jobs.silver import run_silver
from ecommerce_pipeline.jobs.validate import run_validations
from ecommerce_pipeline.logging import configure_logging, get_logger
from ecommerce_pipeline.spark import build_spark

logger = get_logger(__name__)


@task(name="bronze")
def bronze_task(config_path: str) -> None:
    config = load_config(config_path)
    spark = build_spark(config)
    try:
        run_bronze(config, spark)
    finally:
        spark.stop()


@task(name="silver")
def silver_task(config_path: str) -> None:
    config = load_config(config_path)
    spark = build_spark(config)
    try:
        run_silver(config, spark)
    finally:
        spark.stop()


@task(name="gold")
def gold_task(config_path: str) -> None:
    config = load_config(config_path)
    spark = build_spark(config)
    try:
        run_gold(config, spark)
    finally:
        spark.stop()


@task(name="validate")
def validate_task(config_path: str) -> None:
    config = load_config(config_path)
    spark = build_spark(config)
    try:
        results = run_validations(config, spark)
        failed = [result for result in results if not result.passed]
        if failed:
            raise RuntimeError(f"Validation failed: {[result.name for result in failed]}")
    finally:
        spark.stop()


@flow(name="ecommerce-batch-medallion")
def batch_medallion_flow(config_path: str = "configs/local.yaml") -> None:
    configure_logging()
    logger.info("Starting Prefect medallion flow")
    bronze = bronze_task.submit(config_path)
    silver = silver_task.submit(config_path, wait_for=[bronze])
    gold = gold_task.submit(config_path, wait_for=[silver])
    validate_task.submit(config_path, wait_for=[gold])


def deployment_name(config_path: str) -> str:
    return f"ecommerce-batch-{Path(config_path).stem}"


if __name__ == "__main__":
    batch_medallion_flow()
