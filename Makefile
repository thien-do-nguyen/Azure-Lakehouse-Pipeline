.PHONY: install install-dev preflight seed run-local run-cloud validate inspect test lint format docker-build clean

PYTHON ?= .venv/bin/python
CONFIG ?= configs/local.yaml
export PYTHONPATH := $(CURDIR)/src

install:
	python3 -m venv .venv
	$(PYTHON) -m pip install .
# 	rm -rf src/*.egg-info

install-dev:
	python3 -m venv .venv
	$(PYTHON) -m pip install ".[dev,orchestration,azure]"
	$(PYTHON) -m pre_commit install
	rm -rf src/*.egg-info

preflight:
	$(PYTHON) -m ecommerce_pipeline.preflight --config $(CONFIG)

seed:
	$(PYTHON) -m ecommerce_pipeline.seed.synthetic_data --config $(CONFIG) --customers 100 --orders 500 --reset

run-local:
	bash scripts/phase1_local_runbook.sh

run-cloud:
	$(PYTHON) -m ecommerce_pipeline.jobs.run_batch --config configs/azure.yaml

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