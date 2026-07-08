#!/usr/bin/env bash
set -euo pipefail

docker compose up -d postgres
python3 -m venv .venv
.venv/bin/python -m pip install .
rm -rf src/*.egg-info
export PYTHONPATH="$PWD/src"
.venv/bin/python -m ecommerce_pipeline.preflight --config configs/local.yaml
.venv/bin/python -m ecommerce_pipeline.seed.synthetic_data --config configs/local.yaml --customers 100 --orders 500 --reset
.venv/bin/python -m ecommerce_pipeline.jobs.run_batch --config configs/local.yaml
.venv/bin/python -m ecommerce_pipeline.jobs.validate --config configs/local.yaml
