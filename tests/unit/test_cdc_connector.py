from __future__ import annotations

from ecommerce_pipeline.cdc.register_connector import build_debezium_postgres_config, is_connector_healthy


def test_build_debezium_postgres_config_uses_schema_table_include_list(local_config) -> None:
    payload = build_debezium_postgres_config(local_config)

    connector_config = payload["config"]

    assert payload["name"] == "ecommerce-postgres-cdc"
    assert connector_config["database.hostname"] == "postgres"
    assert connector_config["topic.prefix"] == "ecommerce"
    assert "customer_app.orders" in connector_config["table.include.list"]
    assert "customer_app.order_items" in connector_config["table.include.list"]
    assert "customer_app.payments" in connector_config["table.include.list"]
    assert "customer_app.shipments" in connector_config["table.include.list"]
    assert "customer_app.app_users" in connector_config["table.include.list"]
    assert "customer_app.products" in connector_config["table.include.list"]
    assert "customer_app.order_vouchers" in connector_config["table.include.list"]
    assert "ecommerce.customer_app.orders" not in connector_config["table.include.list"]
    assert connector_config["publication.autocreate.mode"] == "filtered"


def test_is_connector_healthy_requires_connector_and_tasks_running() -> None:
    assert is_connector_healthy(
        {
            "connector": {"state": "RUNNING"},
            "tasks": [{"state": "RUNNING"}],
        }
    )
    assert not is_connector_healthy(
        {
            "connector": {"state": "RUNNING"},
            "tasks": [{"state": "FAILED"}],
        }
    )
    assert not is_connector_healthy({"connector": {"state": "RUNNING"}, "tasks": []})
