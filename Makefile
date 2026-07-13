.PHONY: install install-cloud install-dev preflight init-source bootstrap-source seed seed-stream seed-once run-local run-cloud run-cloud-full run-cloud-incremental validate inspect test lint format docker-build clean

PYTHON ?= .venv/bin/python
CONFIG ?= configs/azure.yaml
export PYTHONPATH := $(CURDIR)/src

install:
	python3 -m venv .venv
	$(PYTHON) -m pip install .
# 	rm -rf src/*.egg-info

install-cloud:
	python3 -m venv .venv
	$(PYTHON) -m pip install ".[azure,orchestration]"

install-dev:
	python3 -m venv .venv
	$(PYTHON) -m pip install ".[dev,orchestration,azure]"
	$(PYTHON) -m pre_commit install
	rm -rf src/*.egg-info

preflight:
	$(PYTHON) -m ecommerce_pipeline.preflight --config $(CONFIG)

init-source:
	$(PYTHON) -m ecommerce_pipeline.seed.init_source --config $(CONFIG) --schema schema/oltpSchema.sql

bootstrap-source: init-source seed-once

seed:
	$(PYTHON) -m ecommerce_pipeline.seed.synthetic_data --config $(CONFIG) --customers 100 --orders 500 --reset --continuous --orders-per-batch 5 --interval-seconds 10

seed-stream:
	$(PYTHON) -m ecommerce_pipeline.seed.synthetic_data --config $(CONFIG) --continuous --orders-per-batch 5 --interval-seconds 10

seed-once:
	$(PYTHON) -m ecommerce_pipeline.seed.synthetic_data --config $(CONFIG) --customers 100 --orders 500 --reset

run-local:
	bash scripts/phase1_local_runbook.sh

run-cloud:
	$(PYTHON) -m ecommerce_pipeline.jobs.run_batch --config configs/azure.yaml

run-cloud-full:
	BATCH_LOAD_TYPE=full $(PYTHON) -m ecommerce_pipeline.jobs.run_batch --config configs/azure.yaml

run-cloud-incremental:
	BATCH_LOAD_TYPE=incremental $(PYTHON) -m ecommerce_pipeline.jobs.run_batch --config configs/azure.yaml

validate:
	$(PYTHON) -m ecommerce_pipeline.jobs.validate --config $(CONFIG)

inspect:
	$(PYTHON) -m ecommerce_pipeline.jobs.inspect --config $(CONFIG) --layer gold --table fact_sales --limit 20

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check src tests
	$(PYTHON) -m mypy src

format:
	$(PYTHON) -m ruff format src tests
	$(PYTHON) -m black src tests

docker-build:
	docker build -t ecommerce-pipeline:local .

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov dist build *.egg-info src/*.egg-info
	rm -f .coverage .coverage.*
