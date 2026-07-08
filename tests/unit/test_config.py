from __future__ import annotations

from ecommerce_pipeline.config import load_config


def test_load_local_config_merges_base() -> None:
    config = load_config("configs/local.yaml")

    assert config.environment == "local"
    assert config.postgres.source_schema == "customer_app"
    assert config.lakehouse.table_path("gold", "fact_sales").endswith("data/lake/gold/fact_sales")
    assert "orders" in config.batch.source_tables
    assert config.spark.config["spark.sql.session.timeZone"] == "UTC"


def test_load_config_by_env() -> None:
    config = load_config(env="local")

    assert config.environment == "local"
