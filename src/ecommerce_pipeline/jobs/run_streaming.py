from __future__ import annotations

import argparse
import signal
import time
from typing import Any

from ecommerce_pipeline.config import load_config
from ecommerce_pipeline.jobs.bronze_stream import run_bronze_stream
from ecommerce_pipeline.jobs.silver_stream import ensure_bronze_cdc_table, run_silver_stream
from ecommerce_pipeline.logging import configure_logging, get_logger
from ecommerce_pipeline.metrics import MetricsCollector
from ecommerce_pipeline.spark import build_spark

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run CDC streaming lakehouse jobs.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to local.yaml or azure.yaml.")
    parser.add_argument("--layers", default="bronze,silver,gold", help="Comma-separated streaming layers: bronze,silver,gold.")
    parser.add_argument("--once", action="store_true", help="Process available records once and stop.")
    return parser.parse_args()


def _stop_queries(queries) -> None:
    for query in queries:
        if query.isActive:
            query.stop()


def main() -> None:
    args = parse_args()
    configure_logging()
    config = load_config(args.config)
    requested_layers = {layer.strip().lower() for layer in args.layers.split(",") if layer.strip()}
    metrics = MetricsCollector()
    spark = build_spark(config)
    queries: list[Any] = []
    stopping = False

    def request_shutdown(_signum, _frame) -> None:
        nonlocal stopping
        stopping = True
        logger.info("Stopping streaming queries")
        _stop_queries(queries)

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    try:
        with metrics.timer("streaming_job", environment=config.environment, layers=",".join(sorted(requested_layers))):
            if "gold" in requested_layers and "silver" not in requested_layers:
                raise RuntimeError("Streaming gold requires the silver layer in the same run.")
            if requested_layers & {"bronze", "silver"}:
                # Avoid two streaming writers racing to create the empty Delta log.
                ensure_bronze_cdc_table(config, spark)
            if "bronze" in requested_layers:
                logger.info("Starting bronze CDC stream")
                queries.extend(run_bronze_stream(config, spark))
            if "silver" in requested_layers:
                logger.info("Starting silver CDC stream")
                queries.append(run_silver_stream(config, spark, update_gold="gold" in requested_layers))

            if args.once:
                for query in queries:
                    query.processAllAvailable()
                _stop_queries(queries)
            else:
                while not stopping and all(query.isActive for query in queries):
                    time.sleep(5)
    finally:
        _stop_queries(queries)
        spark.stop()


if __name__ == "__main__":
    main()
