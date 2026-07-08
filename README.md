# E-commerce Lakehouse Pipeline

Local-first, cloud-ready implementation for the architecture

The same Python code runs both modes:

- Local: PostgreSQL in Docker, lakehouse files under `data/lake`, Parquet format.

## Folder Structure

```text
configs/
  base.yaml           # Shared config
  local.yaml          # Local PostgreSQL + local lakehouse path
  azure.yaml          # Azure PostgreSQL + ADLS Gen2 paths
schema/
  oltpSchema.sql      # Source OLTP schema
  dwhSchema.sql       # Warehouse reference schema
src/ecommerce_pipeline/
  config.py           # Typed config loader with environment expansion
  filesystem.py       # fsspec file abstraction
  io.py               # Shared JDBC and lakehouse IO
  logging.py          # Structured JSON logging
  metrics.py          # JSONL metrics collector
  spark.py            # Shared SparkSession builder
  watermark.py        # fsspec-backed watermark state
  jobs/
    bronze.py         # JDBC source ingestion
    silver.py         # Cleaning, trimming, dedupe
    gold.py           # Star schema business tables
    gold_transforms.py # Gold table transformations
    run_batch.py      # Orchestrates Bronze -> Silver -> Gold
    inspect.py        # Table inspection helper
    validate.py       # Reconciliation validation
  seed/
    synthetic_data.py # OLTP synthetic data generator
  orchestration/
    prefect_flow.py   # Prefect flow/tasks for local/cloud orchestration
tests/
  unit/
  integration/
data/lake/            # Local Bronze/Silver/Gold output
```

## Automation

Common commands:

```bash
make install
make preflight
make seed
make run-local
make validate
make test
make lint
make docker-build
```

CI/CD lives in:

```text
.github/workflows/ci.yml
```

It runs lint, mypy, pytest with coverage, package build, and Docker build.

Pre-commit hooks live in:

```text
.pre-commit-config.yaml
```

Install them with:

```bash
make install-dev
```

## Layer Contract

Bronze keeps source tables as-is plus ingestion metadata:

- `_bronze_ingested_at`
- `_source_schema`
- `_source_table`

Silver applies narrow, deterministic quality steps:

- trims string columns
- removes technical Bronze metadata
- deduplicates by primary business key

Gold builds analytical star-schema tables:

- `dim_date`
- `dim_time`
- `dim_customer`
- `dim_location`
- `dim_shop`
- `dim_category`
- `dim_product`
- `dim_promotion`
- `dim_payment`
- `dim_shipping`
- `fact_sales`

The fact grain is one row per source `order_items` row.

## Azure Migration Notes

For Azure, set environment variables from `.env.example`, then use `configs/azure.yaml`.

Expected Azure services from the architecture:

- Azure Database for PostgreSQL Flexible Server
- Azure Databricks
- ADLS Gen2
- Databricks SQL / Power BI for consumption
- Key Vault for secrets after the MVP step

The code already isolates these differences in config:

- JDBC host/user/password
- lakehouse `base_path`
- storage format `parquet` vs `delta`
- Spark settings

No pipeline logic should be forked between local and Azure.

## Orchestration

The pipeline core remains plain Python/Spark. Prefect is a thin orchestration layer:

```bash
.venv/bin/python -m pip install prefect
export PYTHONPATH="$PWD/src"
.venv/bin/python -m ecommerce_pipeline.orchestration.prefect_flow
```

The flow order is:

```text
bronze -> silver -> gold -> validate
```

This replaces the shell script for scheduled/orchestrated runs while keeping the same jobs.

## Secrets

Local uses environment variables. Azure config supports Managed Identity + Key Vault through optional dependencies:

```bash
.venv/bin/python -m pip install azure-identity azure-keyvault-secrets adlfs
```

Set:

```text
AZURE_KEY_VAULT_URL
```

and use `secrets.provider: azure_key_vault` in `configs/azure.yaml`.

## Incremental Design

The default local run is still a full refresh:

```yaml
batch:
  load_type: full
```

To test incremental ingestion, switch to:

```yaml
batch:
  load_type: incremental
```

Bronze will then:

- read only configured tables with `updated_at > previous_watermark - lookback`
- append new rows into Bronze
- store watermarks in `data/state/watermarks.json`

Silver and Gold still rebuild from available Bronze data in this phase. This keeps reruns simple while preparing the project for proper incremental merge/SCD Type 2 later.

## Delta Lake Merge

When `lakehouse.format: delta`, Gold uses Delta merge semantics:

- `fact_sales`: `upsert_to_delta()` with keys `source_order_id, source_order_item_id`
- `dim_customer`: `scd2_merge()` with natural key `source_customer_id` and `scd_hash`
- Other Gold tables: standard Delta overwrite for MVP simplicity

Local config stays Parquet by default. Azure config uses Delta:

```yaml
lakehouse:
  format: delta
```

Delta support is configured in `build_spark()` and `configs/azure.yaml` for Databricks/Delta Lake.
