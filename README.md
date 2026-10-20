# Used Car Tax Value Analytics Platform

A portfolio-scale data engineering demo built around the Finnish Tax Administration's applied vehicle tax-value tables. The project is being developed incrementally; the current implementation is a local `.xls` parser and validation/inspection layer.

## Source

The current scope is the latest passenger-car workbook published on the [Verohallinto source page](https://www.vero.fi/henkiloasiakkaat/omaisuus/autovero/autoveron-maara/ajoneuvojen-sovellettuja-verotusarvoja/). The 2026 workbook has monthly sheets, 29,114 populated rows from January through September, and empty October–December sheets in the inspected copy. The workbook is not committed to this repository.

The source describes records based on tax decisions, not completed vehicle sales. It notes that vehicle condition and equipment affect retail value but are not presented in the table, that some vehicles have had tax benefits, and that later changes to an individual tax decision are not reflected in the table. Do not interpret the applied tax value as a realized sale price or a definitive market value. See [`docs/source-data.md`](docs/source-data.md) for the observed schema and limitations.

## Local workbook inspection

Requires Python 3.13+ and `xlrd` (declared in `pyproject.toml`). Use a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

Download the latest passenger-car `.xls` from the source page to a local, non-repository location, then run:

```bash
python -m tax_values.cli inspect /path/to/henkiloautojen-sovelletut-verotusarvot-2026.xls
```

The command reports the source filename, byte size, SHA-256, populated row counts by month, and quality-issue counts. It does not print vehicle-level values or write an output file. Schema changes fail loudly; invalid field values remain represented in the normalized result and are counted as quality issues rather than silently discarded.

Run the tests with:

```bash
python -m unittest discover -s tests -v
```

## BigQuery bootstrap DDL

[`sql/00_create_tables_and_gold_view.sql`](sql/00_create_tables_and_gold_view.sql) contains copy-and-run DDL for the Bronze raw-row table, Silver typed-record table, Silver quality-issue table, pipeline-run audit table, and an initial Gold monthly-by-model view. It targets the existing `airflow-verohallinto-demo.autot_veroarvot` dataset. Running it creates empty objects only; it does not upload or transform data. Execute it in the BigQuery SQL editor with the correct processing location selected. The script uses `CREATE TABLE IF NOT EXISTS`; it will not migrate a table that already exists with a different schema.

## Current status and next steps

Implemented:

- Discover the latest passenger-car workbook through the official page's direct `.xls` link and retrieve it in memory with a SHA-256 and HTTP metadata.
- Parse the legacy Excel `.xls` workbook and find monthly data tables by their headers.
- Preserve original column values with source sheet and row lineage.
- Normalize verified dates and numeric fields, retaining raw values and reporting invalid fields.
- Produce a privacy-conscious local inspection summary.

Not implemented yet:

- Airflow DAG and deployment to the existing Airflow 3.3.0 installation.
- GCS raw-file persistence and BigQuery data loading/transformation automation.
- Terraform resources and Looker Studio report.
- Persistent row-level quality report and warehouse audit writes.

The user reports that BigQuery tables have been created in `airflow-verohallinto-demo.autot_veroarvot`. This has not been independently verified from this Mac; the exact objects and whether the Gold view was also created are unconfirmed. No GCP resources have been created by this agent. The tables are expected to be empty until ingestion is implemented. Billing status, dataset location, and IAM permissions remain unknown. Do not recreate existing objects. Keep the dataset and SQL-managed objects outside Terraform; Terraform may later manage the GCS bucket and least-privilege IAM after location, authentication, and cost are confirmed. See [`docs/architecture.md`](docs/architecture.md).
