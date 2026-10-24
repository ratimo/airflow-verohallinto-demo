# Architecture and implementation status

## Target flow

```text
Verohallinto page + passenger-car XLS
  -> Airflow 3.3.0 (existing home server; standalone)
  -> direct HTTPS download, parse, and BigQuery batch load (no GCS bucket)
  -> BigQuery Bronze (parsed source values and lineage)
  -> dbt models: Silver -> Gold
  -> private Looker Studio report
```

The repository implements source discovery, local `.xls` parsing, normalization/inspection utilities, and an Airflow DAG draft for download, parsing, and direct Bronze batch loading. The DAG has not been deployed or tested in the home-server Airflow or against BigQuery. No live load has been run. dbt models, Terraform resources, and the Looker Studio report remain unimplemented.

## Data model principles

- Identify each downloaded workbook version by SHA-256. Bronze preserves parsed source cells and lineage; it does not preserve the exact original XLS bytes. The task-local workbook file is not a durable archive.
- Retain `source_file_sha256`, source sheet, and one-based source row for parsed records.
- Treat source row identity as a snapshot key (hash + sheet + row), not as a permanent vehicle or business key.
- Keep Bronze focused on parsed source values. Use dbt Silver models for typed conversion and explicit data-quality reporting; do not silently discard invalid rows in those models.
- The tax decision year/date is distinct from first-registration date and must not be confused with model year.
- Apply downstream transformations only to observed source fields. The current workbook has mileage but no CO2 column.
- Do not call applied tax values realized transaction prices or complete market values.

## Airflow deployment

The target Airflow environment is reported as `apache/airflow:3.3.0`, Python 3.13, started with `airflow standalone`. DAG deployment path, mounts, Python package installation method, outbound networking, and Google Cloud libraries/credentials remain unverified. Before deployment, determine how the existing standalone container receives DAG and project files. Do not replace or reconfigure the existing Airflow installation as part of this repository work.

## GCP, authentication, and cost gate

The user reports that BigQuery tables have been created in `airflow-verohallinto-demo.autot_veroarvot`. This has not been independently verified from this Mac; the exact schemas, data state, and view existence are unknown. No GCP resources or live data loads were created by this agent. The current ingestion design writes directly from the home server to the existing Bronze table and does not use GCS. Billing status, dataset location, APIs, IAM permissions, and project number remain unknown; this Mac has no `gcloud` or `bq` CLI. Do not recreate existing objects. Keep the existing dataset outside Terraform. The bootstrap SQL is a schema/reference artifact; dbt should own ongoing Silver/Gold models so transformations are not duplicated across SQL scripts and dbt.

The stated spending ceiling is €10/month. A GCP budget is an alert, not a guaranteed hard spend cap; use manual DAG runs initially, bounded BigQuery query bytes, minimal retention, and cost monitoring. A non-Google workload identity provider is not available on the home server, so workload authentication must be resolved before connecting it. Never commit credentials or Terraform state.

No GCS bucket is currently planned. The original XLS bytes are not durably archived by the direct-load design; confirm whether task-local retention is acceptable before treating Bronze as a complete replayable raw archive.

## Planned implementation sequence

1. Source parsing and validation utilities (implemented locally).
2. Airflow DAG for source download, parse, and direct Bronze loading; test in the existing home-server runtime before deployment.
3. Verify the existing Bronze schema and configure least-privilege BigQuery authentication without creating a GCS bucket.
4. Build dbt Silver/Gold models and tests from the observed source schema.
5. Decide whether to retain the original workbook in an existing persistent home-server volume; do not assume task-local storage is durable.
6. Build the private Looker Studio report from dbt Gold outputs.
