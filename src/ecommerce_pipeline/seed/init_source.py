from __future__ import annotations

import argparse
from pathlib import Path

import psycopg

from ecommerce_pipeline.config import load_config


def initialize_source_schema(config_path: str, schema_path: str) -> None:
    config = load_config(config_path)
    sql_path = Path(schema_path)
    schema_sql = sql_path.read_text(encoding="utf-8")

    with psycopg.connect(config.postgres.psycopg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()

    print(
        f"Initialized source schema from {sql_path} "
        f"on {config.postgres.host}/{config.postgres.database}",
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize the PostgreSQL OLTP source schema.")
    parser.add_argument("--config", default="configs/azure.yaml")
    parser.add_argument("--schema", default="schema/oltpSchema.sql")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    initialize_source_schema(args.config, args.schema)


if __name__ == "__main__":
    main()
