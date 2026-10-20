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

## Current status and next steps

Implemented:

- Discover the latest passenger-car workbook through the official page's direct `.xls` link and retrieve it in memory with a SHA-256 and HTTP metadata.
- Parse the legacy Excel `.xls` workbook and find monthly data tables by their headers.
- Preserve original column values with source sheet and row lineage.
- Normalize verified dates and numeric fields, retaining raw values and reporting invalid fields.
- Produce a privacy-conscious local inspection summary.

Not implemented yet:

- Airflow DAG and deployment to the existing Airflow 3.3.0 installation.
- Google Cloud Storage and BigQuery integration, Terraform resources, or Looker Studio report.
- Persistent row-level quality report and warehouse audit tables.

No GCP project, billing, or cloud resources are created by the current code. These steps require separate approval and a confirmed cost/authentication setup. See [`docs/architecture.md`](docs/architecture.md).
