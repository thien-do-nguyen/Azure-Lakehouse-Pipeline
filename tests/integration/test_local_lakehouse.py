from __future__ import annotations

from pathlib import Path

import pytest

from ecommerce_pipeline.config import load_config
from ecommerce_pipeline.jobs.validate import run_validations
from ecommerce_pipeline.spark import build_spark


@pytest.mark.integration
def test_existing_local_lakehouse_validates() -> None:
    if not Path("data/lake/gold/fact_sales/_delta_log").exists():
        pytest.skip("Run scripts/phase1_local_runbook.sh before this integration test.")

    config = load_config("configs/local.yaml")
    spark = build_spark(config)
    try:
        results = run_validations(config, spark)
    finally:
        spark.stop()

    assert all(result.passed for result in results)
