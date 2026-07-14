# E-commerce Lakehouse Pipeline

Local-first, cloud-ready implementation for the architecture

See [`docs/enterprise-readiness.md`](docs/enterprise-readiness.md) for the
evidence-based readiness assessment and Azure production gaps.

The same Python code runs both modes:

- Local: PostgreSQL, Redpanda and Debezium in Docker; Delta Lake under `data/lake`.

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

CDC/streaming local commands:

```bash
CONFIG=configs/local.yaml make cdc-up
CONFIG=configs/local.yaml make init-source
CONFIG=configs/local.yaml make seed-once
CONFIG=configs/local.yaml make cdc-register
CONFIG=configs/local.yaml make cdc-status
CONFIG=configs/local.yaml make seed-stream
make run-stream-local
```

Always bootstrap the common Delta tables with a full batch before starting CDC.
The shortest reproducible flow is:

```bash
make cdc-up
CONFIG=configs/local.yaml make seed-once
make run-batch-local
make cdc-register
# terminal 1
make run-stream-local
# terminal 2
CONFIG=configs/local.yaml make seed-stream
```

`cdc-up` starts PostgreSQL with logical replication plus Redpanda and Debezium
Connect. `cdc-register` creates the PostgreSQL Debezium connector and waits for
the connector/tasks to become healthy. It monitors every table used by batch:

```text
app_users, user_addresses, shops, categories, products, product_variants,
vouchers, orders, order_items, order_vouchers, payments, shipments
```

The local Kafka-compatible topics follow this contract:

```text
ecommerce.customer_app.<table_name>
```

The streaming Bronze job writes raw CDC events to:

```text
data/lake/bronze/cdc_events
```

Invalid CDC records are written to:

```text
data/lake/bronze/cdc_dead_letters
```

`make seed` resets and bootstraps the OLTP database, then keeps inserting small
order batches every few seconds for incremental/CDC demos. Use `make seed-once`
for the old one-shot reset seed, or `make seed-stream` to append realtime-like
changes without resetting existing source data.

## Hybrid Student-Azure Mode

For the low-cost student setup, keep PostgreSQL and ADLS Gen2 on Azure but run
Spark transforms locally:

```bash
make install-cloud
make preflight
make bootstrap-source
make seed-stream
make run-cloud-full
make run-cloud-incremental
make validate
```

The default `CONFIG` is `configs/azure.yaml`. That config uses local Spark
(`local[*]`) while reading PostgreSQL over JDBC and writing Delta tables to
`abfss://lakehouse@<storage-account>.dfs.core.windows.net/ecommerce`.

Required `.env` values:

```text
AZURE_POSTGRES_HOST
AZURE_POSTGRES_USER
AZURE_POSTGRES_PASSWORD
AZURE_STORAGE_ACCOUNT
AZURE_STORAGE_ACCOUNT_KEY
```

`AZURE_CONTAINER`, `AZURE_STORAGE_AUTH_TYPE`, PostgreSQL port/database, and
SSL mode all have defaults in `configs/azure.yaml`.

If the Azure PostgreSQL database is empty because you created it manually,
run `make bootstrap-source` once. It creates the `customer_app` schema from
`schema/oltpSchema.sql` and inserts baseline synthetic data.

Use `make run-cloud-full` for the first lakehouse build or an intentional
backfill. Use `make run-cloud-incremental` for the regular CDC-style runs after
that. Keep `.env` for stable infrastructure settings only; load type is a run
parameter.

Use `CONFIG=configs/local.yaml make <target>` when you want the old fully local
Docker/PostgreSQL/Redpanda mode. Local and Azure both use Delta semantics.

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
- lakehouse base path and Spark runtime/storage authentication
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
- merge new rows into the existing Delta Bronze table by primary key
- store watermarks in `data/state/watermarks.json`

Silver and Gold read the same unified tables used by CDC. Batch incremental is a
fallback/reconciliation path; deletes are authoritative through CDC.

## CDC / Streaming Design

Local CDC uses PostgreSQL logical replication, Debezium Connect, and Redpanda.
This keeps the local architecture close to Kafka-compatible cloud options such
as Azure Event Hubs Kafka API, Confluent Cloud, or a Kafka cluster.

Bronze streaming stores raw Debezium envelopes first. It keeps Kafka metadata,
operation type, source metadata, `before` JSON, `after` JSON, and ingestion time.
Bad records are routed to a dead-letter Delta table instead of stopping the
stream.

Silver streaming reads the immutable Bronze CDC event log, validates table
schemas, deduplicates by event id and primary key, then merges the latest row
into the same `bronze/<table>` current-state Delta table created by batch. It
immediately applies the shared Silver transform and performs row-level
`DELETE/MERGE` on `silver/<table>`. No CDC micro-batch overwrites Silver. Deletes
default to soft delete in Bronze and are removed from Silver; `delete_mode: hard`
physically deletes them from both current-state layers.

Gold CDC resolves both `before` and `after` relationships to impacted `order_id`
values. Dimensions use Delta MERGE and `fact_sales` replaces only rows inside
that order scope, so source deletes remove stale facts without a full Gold
overwrite. There are no `silver_cdc` or `gold_cdc` data products.

Each Silver/Gold micro-batch is tracked in Delta control tables:

```text
_control/cdc_batch_commits
_control/cdc_table_watermarks
```

A failed or interrupted batch remains retryable; a committed batch is skipped.
Watermarks are stored per topic/partition rather than as a process-local JSON
offset. The ledger key fingerprints topic/partition offset ranges, so checkpoint
replay cannot confuse a new batch boundary with an old `batch_id`.

Run the automated Docker restart/replay lifecycle with `make cdc-e2e`. This
opt-in test intentionally resets the local Compose volumes and generated lake
data, then cleans the Docker stack when it finishes.

Batch Bronze remains available for full refreshes, backfills, reconciliation,
and simpler local debugging.

For Azure, keep the same job code and change only config:

- `streaming.bootstrap_servers`
- `streaming.checkpoint_path`
- `streaming.silver_checkpoint_path`
- `streaming.schema_registry_path`
- `lakehouse.base_path`
- Spark/auth settings

## Delta Lake Merge

When `lakehouse.format: delta`, Gold uses Delta merge semantics:

- `fact_sales`: full-source Delta synchronization (upsert plus delete of stale facts)
- `dim_customer`: `scd2_merge()` with natural key `source_customer_id` and `scd_hash`
- Other Gold tables: standard Delta overwrite for MVP simplicity

Local and Azure both use Delta so local tests cover ACID and `MERGE` behavior:

```yaml
lakehouse:
  format: delta
```

Delta support is configured in `build_spark()` and `configs/azure.yaml` for Databricks/Delta Lake.
