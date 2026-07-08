from __future__ import annotations

import argparse

from ecommerce_pipeline.config import load_config
from ecommerce_pipeline.io import read_layer_table
from ecommerce_pipeline.spark import build_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect a local/cloud lakehouse table.")
    parser.add_argument("--config", default="configs/local.yaml")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--schema", action="store_true", help="Print table schema before rows.")
    parser.add_argument(
        "--sql",
        help="Optional SQL query. Use table alias `t`, for example: SELECT * FROM t LIMIT 10",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    spark = build_spark(config)
    try:
        df = read_layer_table(spark, config, args.layer, args.table)
        if args.schema:
            df.printSchema()

        if args.sql:
            df.createOrReplaceTempView("t")
            spark.sql(args.sql).show(args.limit, truncate=False)
        else:
            df.show(args.limit, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
