# Architecture and implementation status

## Target flow

```text
Verohallinto page + passenger-car XLS
  -> Airflow 3.3.0 (existing home server; standalone)
  -> immutable original XLS + metadata in Cloud Storage
  -> BigQuery Bronze (parsed raw values and lineage)
  -> BigQuery Silver (typed, validated records and explicit quality issues)
  -> BigQuery Gold (reporting views)
  -> private Looker Studio report
```

The repository currently implements source-link discovery, in-memory source retrieval with SHA-256/HTTP metadata, local `.xls` parsing, normalization, and a privacy-conscious inspection summary. No DAG, GCP client, Terraform resource, or Looker report has been created yet.

## Data model principles

- Keep the original workbook byte-for-byte in the raw layer and identify versions by SHA-256.
- Retain `source_file_hash`, source sheet, and one-based source row for parsed records.
- Treat source row identity as a snapshot key (hash + sheet + row), not as a permanent vehicle or business key.
- Preserve original source values alongside normalized fields. Do not silently discard invalid rows; retain them and report issues.
- The tax decision year/date is distinct from first-registration date and must not be confused with model year.
- Apply downstream transformations only to observed source fields. The current workbook has mileage but no CO2 column.
- Do not call applied tax values realized transaction prices or complete market values.

## Airflow deployment

The target Airflow environment is reported as `apache/airflow:3.3.0`, Python 3.13, started with `airflow standalone`. DAG deployment path, mounts, Python package installation method, outbound networking, and Google Cloud libraries/credentials remain unverified. Before a DAG is deployed, determine how the existing standalone container receives DAG and project files. Do not replace or reconfigure the existing Airflow installation as part of this repository work.

## GCP, authentication, and cost gate

No GCP project or billing account is configured yet. GCS/BigQuery/Terraform implementation is pending a separate setup decision. The stated target is a new project with a €10 monthly cost ceiling. A GCP budget is an alert, not a guaranteed hard spend cap; further controls should include manual DAG runs initially, bounded BigQuery query bytes, minimal retention, and cost monitoring. A non-Google workload identity provider is not available on the home server, so the workload authentication design must be resolved before connecting it. Never commit credentials or Terraform state.

Choose and align GCS and BigQuery locations before creating either. The cost/locality tradeoff needs explicit review because Cloud Storage's Always Free allowance applies only to selected US regions, not Finland.

## Planned implementation sequence

1. Local source parsing and validation (implemented).
2. Source discovery and immutable raw-file storage with metadata.
3. Unit/integration tests for update detection, schema drift, and row-level quality reports.
4. Airflow DAG and deployment instructions matching the existing standalone setup.
5. Terraform definitions and least-privilege GCS/BigQuery resources, only after project/billing/auth approval.
6. Bronze/Silver/Gold load and SQL models.
7. Private Looker Studio report with source caveats and refresh metadata.
