"""Manual Verohallinto passenger-car ingestion DAG.

The DAG intentionally does not use GCS. The workbook and NDJSON files are
bounded, task-local artifacts; Bronze keeps the parsed raw values and source
lineage in BigQuery. Live BigQuery writes require an explicit operator-run.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

# The repository layout keeps the package in src/ next to dags/.
_REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_REPO_SRC) not in sys.path:
    sys.path.insert(0, str(_REPO_SRC))

from airflow.sdk import dag, get_current_context, task
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, QueryJobConfig, SchemaField
from google.cloud.bigquery.enums import SourceFormat, WriteDisposition
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from tax_values.source import DownloadedWorkbook, download_latest_workbook
from tax_values.xls_parser import parse_xls_bytes

GCP_CONN_ID = os.environ.get("VERO_GCP_CONN_ID", "")
PROJECT_ID = os.environ.get("VERO_BQ_PROJECT", "")
DATASET_ID = os.environ.get("VERO_BQ_DATASET", "")
LOCATION = os.environ.get("VERO_BQ_LOCATION") or None
WORK_DIR = Path(os.environ.get("VERO_WORK_DIR", "/tmp/verohallinto-airflow"))

BRONZE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.bronze_vehicle_tax_rows"


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Not JSON serializable: {type(value).__name__}")


def _run_dir(run_id: str) -> Path:
    safe_id = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:24]
    path = WORK_DIR / safe_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _hook() -> BigQueryHook:
    return BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)


def _client() -> bigquery.Client:
    missing = [
        name
        for name, value in (
            ("VERO_GCP_CONN_ID", GCP_CONN_ID),
            ("VERO_BQ_PROJECT", PROJECT_ID),
            ("VERO_BQ_DATASET", DATASET_ID),
        )
        if not value.strip()
    ]
    if missing:
        raise RuntimeError(
            "Missing required Airflow environment variable(s): " + ", ".join(missing)
        )
    return _hook().get_client(project_id=PROJECT_ID, location=LOCATION)


def _query(client: bigquery.Client, sql: str, parameters: list[Any] | None = None) -> Any:
    config = QueryJobConfig(maximum_bytes_billed=100_000_000)
    if parameters:
        config.query_parameters = parameters
    return client.query(sql, job_config=config, location=LOCATION).result()


def _metadata_path(run_id: str) -> Path:
    return _run_dir(run_id) / "manifest.json"


def _load_config(schema: list[SchemaField]) -> LoadJobConfig:
    return LoadJobConfig(
        schema=schema,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=False,
    )


@dag(
    dag_id="verohallinto_vehicle_tax_ingestion",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["verohallinto", "bigquery", "manual"],
)
def verohallinto_vehicle_tax_ingestion():
    @task(retries=3, retry_delay=timedelta(minutes=2))
    def discover_and_download() -> dict[str, Any]:
        context = get_current_context()
        run_id = context["dag_run"].run_id
        workbook: DownloadedWorkbook = download_latest_workbook()
        directory = _run_dir(run_id)
        workbook_path = directory / "source.xls"
        workbook_path.write_bytes(workbook.content)
        manifest = {
            "run_id": run_id,
            "workbook_path": str(workbook_path),
            "source_url": workbook.link.url,
            "source_file_name": Path(workbook.link.url).name,
            "decision_year": workbook.link.decision_year,
            "source_file_sha256": workbook.sha256,
            "retrieved_at_utc": workbook.retrieved_at_utc,
            "content_type": workbook.content_type,
            "etag": workbook.etag,
            "last_modified": workbook.last_modified,
        }
        _metadata_path(run_id).write_text(json.dumps(manifest), encoding="utf-8")
        return {"manifest_path": str(_metadata_path(run_id))}

    @task
    def parse_source_rows(download: dict[str, Any]) -> dict[str, Any]:
        manifest_path = Path(download["manifest_path"])
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        rows = parse_xls_bytes(Path(manifest["workbook_path"]).read_bytes())
        if not rows:
            raise ValueError("No passenger-car data rows found; refusing an empty Bronze load.")
        directory = manifest_path.parent
        bronze_path = directory / "bronze.ndjson"
        loaded_at = datetime.now(timezone.utc).isoformat()
        with bronze_path.open("w", encoding="utf-8") as bronze_file:
            for row in rows:
                source_row_id = hashlib.sha256(
                    f'{manifest["source_file_sha256"]}:{row.source_sheet}:{row.source_row}'.encode(
                        "utf-8"
                    )
                ).hexdigest()
                bronze = {
                    "source_row_id": source_row_id,
                    "source_file_sha256": manifest["source_file_sha256"],
                    "source_file_name": manifest["source_file_name"],
                    "source_url": manifest["source_url"],
                    "decision_year": manifest["decision_year"],
                    "source_sheet": row.source_sheet,
                    "source_row_number": row.source_row,
                    "retrieved_at_utc": manifest["retrieved_at_utc"],
                    "raw_values": row.values,
                    "loaded_at_utc": loaded_at,
                }
                bronze_file.write(json.dumps(bronze, default=_json_default, ensure_ascii=False) + "\n")
        manifest.update(
            {
                "bronze_path": str(bronze_path),
                "rows_read": len(rows),
                "rows_parsed": len(rows),
            }
        )
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        return {"manifest_path": str(manifest_path)}

    @task
    def load_bronze(parsed: dict[str, Any]) -> dict[str, Any]:
        manifest_path = Path(parsed["manifest_path"])
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        client = _client()
        duplicate_rows = list(
            _query(
                client,
                f"SELECT COUNT(*) AS row_count FROM `{BRONZE_TABLE}` WHERE source_file_sha256 = @sha",
                [bigquery.ScalarQueryParameter("sha", "STRING", manifest["source_file_sha256"])],
            )
        )
        if duplicate_rows and duplicate_rows[0].row_count:
            manifest["bronze_load"] = "skipped_existing_source_hash"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            return {"manifest_path": str(manifest_path), "skipped": True}

        bronze_schema = [
            SchemaField("source_row_id", "STRING", mode="REQUIRED"),
            SchemaField("source_file_sha256", "STRING", mode="REQUIRED"),
            SchemaField("source_file_name", "STRING", mode="REQUIRED"),
            SchemaField("source_url", "STRING", mode="REQUIRED"),
            SchemaField("decision_year", "INT64", mode="REQUIRED"),
            SchemaField("source_sheet", "STRING", mode="REQUIRED"),
            SchemaField("source_row_number", "INT64", mode="REQUIRED"),
            SchemaField("retrieved_at_utc", "TIMESTAMP", mode="REQUIRED"),
            SchemaField("raw_values", "JSON", mode="REQUIRED"),
            SchemaField("loaded_at_utc", "TIMESTAMP", mode="REQUIRED"),
        ]
        with Path(manifest["bronze_path"]).open("rb") as source_file:
            load_job = client.load_table_from_file(
                source_file,
                BRONZE_TABLE,
                job_config=_load_config(bronze_schema),
                job_id=f"verohallinto_bronze_{manifest['source_file_sha256'][:24]}",
                location=LOCATION,
            )
            load_job.result()
        manifest["bronze_load"] = "loaded"
        manifest["rows_loaded"] = load_job.output_rows
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        return {"manifest_path": str(manifest_path), "skipped": False}

    downloaded = discover_and_download()
    parsed = parse_source_rows(downloaded)
    load_bronze(parsed)


verohallinto_vehicle_tax_ingestion()
