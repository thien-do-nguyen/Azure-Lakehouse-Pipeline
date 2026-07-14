from __future__ import annotations

import argparse

from ecommerce_pipeline.config import load_config
from ecommerce_pipeline.jobs.bronze import run_bronze
from ecommerce_pipeline.jobs.gold import run_gold
from ecommerce_pipeline.jobs.silver import run_silver
from ecommerce_pipeline.jobs.validate import print_results, run_validations
from ecommerce_pipeline.logging import configure_logging, get_logger
from ecommerce_pipeline.metrics import MetricsCollector
from ecommerce_pipeline.spark import build_spark

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run batch lakehouse pipeline.")
    parser.add_argument("--config", help="Path to local.yaml or azure.yaml.")
    parser.add_argument("--env", default=None, help="Environment name resolved as configs/<env>.yaml.")
    parser.add_argument(
        "--layers",
        default="bronze,silver,gold",
        help="Comma-separated layers to run: bronze,silver,gold.",
    )
    parser.add_argument("--validate", action="store_true", help="Run reconciliation checks before reporting success.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()
    config = load_config(args.config, env=args.env)
    requested_layers = {layer.strip().lower() for layer in args.layers.split(",") if layer.strip()}
    metrics = MetricsCollector()

    spark = build_spark(config)
    try:
        with metrics.timer("batch_job", environment=config.environment, layers=",".join(sorted(requested_layers))):
            if "bronze" in requested_layers:
                logger.info("Starting bronze layer")
                with metrics.timer("layer", environment=config.environment, layer="bronze"):
                    run_bronze(config, spark)
            if "silver" in requested_layers:
                logger.info("Starting silver layer")
                with metrics.timer("layer", environment=config.environment, layer="silver"):
                    run_silver(config, spark)
            if "gold" in requested_layers:
                logger.info("Starting gold layer")
                with metrics.timer("layer", environment=config.environment, layer="gold"):
                    run_gold(config, spark)
            if args.validate:
                logger.info("Starting reconciliation validation")
                results = run_validations(config, spark)
                print_results(results)
                failed = [result.name for result in results if not result.passed]
                if failed:
                    raise RuntimeError(f"Reconciliation failed: {', '.join(failed)}")
        logger.info("Batch job completed")
    except Exception:
        logger.exception("Batch job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
