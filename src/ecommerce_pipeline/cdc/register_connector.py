from __future__ import annotations

import argparse
import json
import time
from urllib import request
from urllib.error import HTTPError

from ecommerce_pipeline.config import AppConfig, load_config
from ecommerce_pipeline.logging import configure_logging, get_logger

logger = get_logger(__name__)


def _connector_database_host(config: AppConfig) -> str:
    if config.environment == "local" and config.postgres.host in {"localhost", "127.0.0.1"}:
        return "postgres"
    return config.postgres.host


def build_debezium_postgres_config(config: AppConfig, connector_name: str = "ecommerce-postgres-cdc") -> dict[str, object]:
    table_include_list = ",".join(f"{config.postgres.source_schema}.{table}" for table in config.streaming.topics)

    connector_config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": _connector_database_host(config),
        "database.port": str(config.postgres.port),
        "database.user": config.postgres.user,
        "database.password": config.postgres.password,
        "database.dbname": config.postgres.database,
        "topic.prefix": config.streaming.topic_prefix,
        "schema.include.list": config.postgres.source_schema,
        "table.include.list": table_include_list,
        "slot.name": "ecommerce_cdc_slot",
        "publication.name": "ecommerce_cdc_publication",
        "publication.autocreate.mode": "filtered",
        "snapshot.mode": "initial",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "include.schema.changes": "false",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
    }
    if config.postgres.sslmode:
        connector_config["database.sslmode"] = config.postgres.sslmode
    return {"name": connector_name, "config": connector_config}


def put_connector(connect_url: str, payload: dict[str, object]) -> None:
    connector_name = payload["name"]
    body = json.dumps(payload["config"]).encode("utf-8")
    endpoint = f"{connect_url.rstrip('/')}/connectors/{connector_name}/config"
    req = request.Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    try:
        with request.urlopen(req, timeout=30) as response:
            logger.info("Registered Debezium connector %s: HTTP %s", connector_name, response.status)
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Failed to register connector {connector_name}: HTTP {exc.code}: {detail}") from exc


def get_connector_status(connect_url: str, connector_name: str) -> dict[str, object]:
    endpoint = f"{connect_url.rstrip('/')}/connectors/{connector_name}/status"
    req = request.Request(endpoint, method="GET")
    try:
        with request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Failed to read connector status {connector_name}: HTTP {exc.code}: {detail}") from exc


def is_connector_healthy(status: dict[str, object]) -> bool:
    connector = status.get("connector", {})
    tasks = status.get("tasks", [])
    connector_running = isinstance(connector, dict) and connector.get("state") == "RUNNING"
    tasks_running = isinstance(tasks, list) and bool(tasks) and all(
        isinstance(task, dict) and task.get("state") == "RUNNING" for task in tasks
    )
    return connector_running and tasks_running


def wait_for_connector_healthy(connect_url: str, connector_name: str, timeout_seconds: int = 60) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    last_status: dict[str, object] = {}
    while time.monotonic() < deadline:
        last_status = get_connector_status(connect_url, connector_name)
        if is_connector_healthy(last_status):
            return last_status
        time.sleep(2)
    raise RuntimeError(f"Connector {connector_name} did not become healthy: {json.dumps(last_status, sort_keys=True)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register the Debezium PostgreSQL CDC connector.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to local.yaml or azure.yaml.")
    parser.add_argument("--connect-url", default="http://localhost:8083", help="Kafka Connect REST URL.")
    parser.add_argument("--name", default="ecommerce-postgres-cdc", help="Connector name.")
    parser.add_argument("--dry-run", action="store_true", help="Print the connector payload without registering it.")
    parser.add_argument("--status", action="store_true", help="Print connector status without registering it.")
    parser.add_argument("--wait", action="store_true", help="Wait until the connector and tasks are running.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()
    config = load_config(args.config)
    if args.status:
        print(json.dumps(get_connector_status(args.connect_url, args.name), indent=2, sort_keys=True))
        return
    payload = build_debezium_postgres_config(config, args.name)
    if args.dry_run:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    put_connector(args.connect_url, payload)
    if args.wait:
        status = wait_for_connector_healthy(args.connect_url, args.name)
        print(json.dumps(status, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
