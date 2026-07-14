from __future__ import annotations

from dataclasses import replace

import pytest

from ecommerce_pipeline.config import _validate_config, load_config


def test_load_local_config_merges_base() -> None:
    config = load_config("configs/local.yaml")

    assert config.environment == "local"
    assert config.postgres.source_schema == "customer_app"
    assert config.lakehouse.table_path("gold", "fact_sales").endswith("data/lake/gold/fact_sales")
    assert "orders" in config.batch.source_tables
    assert config.streaming.topic_for_table(config.postgres.source_schema, "orders") == "ecommerce.customer_app.orders"
    assert config.streaming.enabled is False
    assert config.streaming.gold_layer == "gold"
    assert config.lakehouse.format == "delta"
    assert config.streaming.topics == config.batch.source_tables
    assert config.spark.config["spark.sql.session.timeZone"] == "UTC"


def test_load_config_by_env() -> None:
    config = load_config(env="local")

    assert config.environment == "local"


@pytest.mark.parametrize(
    ("config_factory", "message"),
    [
        (lambda c: replace(c, batch=replace(c.batch, load_type="bad")), "batch.load_type"),
        (lambda c: replace(c, lakehouse=replace(c.lakehouse, format="csv")), "lakehouse.format"),
        (lambda c: replace(c, streaming=replace(c.streaming, delete_mode="archive")), "delete_mode"),
        (
            lambda c: replace(c, batch=replace(c.batch, source_tables=[*c.batch.source_tables, "orders"])),
            "duplicates",
        ),
        (lambda c: replace(c, streaming=replace(c.streaming, topics=["orders"])), "must match"),
        (lambda c: replace(c, streaming=replace(c.streaming, primary_keys={})), "Missing streaming.primary_keys"),
        (
            lambda c: replace(
                c,
                lakehouse=replace(c.lakehouse, format="parquet"),
                streaming=replace(c.streaming, enabled=True),
            ),
            "CDC requires Delta",
        ),
    ],
)
def test_validate_config_rejects_unsafe_contracts(config_factory, message) -> None:
    with pytest.raises(ValueError, match=message):
        _validate_config(config_factory(load_config("configs/local.yaml")))
