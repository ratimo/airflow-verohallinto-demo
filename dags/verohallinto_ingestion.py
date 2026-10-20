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
from tax_values.validation import NormalizedRow, normalize_row
from tax_values.xls_parser import parse_xls_bytes

GCP_CONN_ID = os.environ.get("VERO_GCP_CONN_ID", "")
PROJECT_ID = os.environ.get("VERO_BQ_PROJECT", "")
DATASET_ID = os.environ.get("VERO_BQ_DATASET", "")
LOCATION = os.environ.get("VERO_BQ_LOCATION") or None
WORK_DIR = Path(os.environ.get("VERO_WORK_DIR", "/tmp/verohallinto-airflow"))

BRONZE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.bronze_vehicle_tax_rows"
QUALITY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.silver_quality_issues"
SILVER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.silver_vehicle_tax_values"
RUNS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.pipeline_runs"


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
    def parse_and_validate(download: dict[str, Any]) -> dict[str, Any]:
        manifest_path = Path(download["manifest_path"])
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        rows = parse_xls_bytes(Path(manifest["workbook_path"]).read_bytes())
        normalized = [normalize_row(row) for row in rows]
        directory = manifest_path.parent
        bronze_path = directory / "bronze.ndjson"
        quality_path = directory / "quality.ndjson"
        loaded_at = datetime.now(timezone.utc).isoformat()
        bronze_count = 0
        quality_count = 0
        with bronze_path.open("w", encoding="utf-8") as bronze_file, quality_path.open(
            "w", encoding="utf-8"
        ) as quality_file:
            for row in normalized:
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
                    "raw_values": row.raw_values,
                    "loaded_at_utc": loaded_at,
                }
                bronze_file.write(json.dumps(bronze, default=_json_default, ensure_ascii=False) + "\n")
                bronze_count += 1
                for issue in row.issues:
                    quality = {
                        "source_row_id": source_row_id,
                        "source_file_sha256": manifest["source_file_sha256"],
                        "source_sheet": row.source_sheet,
                        "source_row_number": row.source_row,
                        "field_name": issue.field,
                        "issue_code": issue.code,
                        "issue_message": issue.message,
                        "detected_at_utc": loaded_at,
                    }
                    quality_file.write(json.dumps(quality, ensure_ascii=False) + "\n")
                    quality_count += 1
        manifest.update(
            {
                "bronze_path": str(bronze_path),
                "quality_path": str(quality_path),
                "rows_read": len(rows),
                "rows_loaded": bronze_count,
                "quality_issues_total": quality_count,
                "rows_with_issues": sum(bool(row.issues) for row in normalized),
            }
        )
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        return {"manifest_path": str(manifest_path)}

    @task
    def load_bronze_and_quality(parsed: dict[str, Any]) -> dict[str, Any]:
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

        quality_path = Path(manifest["quality_path"])
        if quality_path.stat().st_size:
            quality_schema = [
                SchemaField("source_row_id", "STRING", mode="REQUIRED"),
                SchemaField("source_file_sha256", "STRING", mode="REQUIRED"),
                SchemaField("source_sheet", "STRING", mode="REQUIRED"),
                SchemaField("source_row_number", "INT64", mode="REQUIRED"),
                SchemaField("field_name", "STRING", mode="REQUIRED"),
                SchemaField("issue_code", "STRING", mode="REQUIRED"),
                SchemaField("issue_message", "STRING", mode="REQUIRED"),
                SchemaField("detected_at_utc", "TIMESTAMP", mode="REQUIRED"),
            ]
            with quality_path.open("rb") as quality_file:
                quality_job = client.load_table_from_file(
                    quality_file,
                    QUALITY_TABLE,
                    job_config=_load_config(quality_schema),
                    job_id=f"verohallinto_quality_{manifest['source_file_sha256'][:24]}",
                    location=LOCATION,
                )
                quality_job.result()
        manifest["bronze_load"] = "loaded"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        return {"manifest_path": str(manifest_path), "skipped": False}

    @task
    def transform_silver_and_audit(loaded: dict[str, Any]) -> None:
        manifest_path = Path(loaded["manifest_path"])
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if loaded.get("skipped"):
            return
        client = _client()
        sha = bigquery.ScalarQueryParameter("sha", "STRING", manifest["source_file_sha256"])
        run = bigquery.ScalarQueryParameter("run_id", "STRING", manifest["run_id"])
        sql = f"""
        INSERT INTO `{SILVER_TABLE}`
        SELECT
          source_row_id, source_file_sha256, source_file_name, decision_year,
          source_sheet, source_row_number, retrieved_at_utc,
          JSON_VALUE(raw_values, '$."Merkki"'), JSON_VALUE(raw_values, '$."Malli"'),
          SAFE_CAST(SAFE_CAST(JSON_VALUE(raw_values, '$."Cm3"') AS NUMERIC) AS INT64),
          JSON_VALUE(raw_values, '$."Käyttövoima"'),
          SAFE_CAST(SAFE_CAST(JSON_VALUE(raw_values, '$."OvienLkm"') AS NUMERIC) AS INT64),
          JSON_VALUE(raw_values, '$."Korimalli"'), JSON_VALUE(raw_values, '$."Vetotapa"'),
          JSON_VALUE(raw_values, '$."Vaihteisto"'),
          SAFE_CAST(SAFE_CAST(JSON_VALUE(raw_values, '$."Kw"') AS NUMERIC) AS INT64),
          JSON_VALUE(raw_values, '$."Yksilölliset ominaisuudet"'),
          SAFE.PARSE_DATE('%Y%m%d', JSON_VALUE(raw_values, '$."Päätöspäivä"')),
          SAFE.PARSE_DATE('%Y%m%d', JSON_VALUE(raw_values, '$."Ensirekisteröintipäivä"')),
          SAFE_CAST(JSON_VALUE(raw_values, '$."Ajokm/1000"') AS NUMERIC),
          SAFE_CAST(JSON_VALUE(raw_values, '$."Ajokm/1000"') AS NUMERIC) * 1000,
          SAFE_CAST(JSON_VALUE(raw_values, '$."Verotusarvo"') AS NUMERIC),
          SAFE_CAST(JSON_VALUE(raw_values, '$."Veroprosentti"') AS NUMERIC),
          SAFE_CAST(JSON_VALUE(raw_values, '$."Autovero"') AS NUMERIC),
          COALESCE((SELECT COUNT(*) FROM `{QUALITY_TABLE}` q WHERE q.source_row_id = b.source_row_id), 0),
          CURRENT_TIMESTAMP()
        FROM `{BRONZE_TABLE}` b
        WHERE source_file_sha256 = @sha
          AND NOT EXISTS (
            SELECT 1 FROM `{SILVER_TABLE}` s WHERE s.source_row_id = b.source_row_id
          )
        """
        _query(client, sql, [sha])
        audit_sql = f"""
        INSERT INTO `{RUNS_TABLE}`
          (run_id, source_url, source_file_name, source_file_sha256, decision_year,
           started_at_utc, finished_at_utc, status, rows_read, rows_loaded,
           rows_with_issues, quality_issues_total, error_message)
        VALUES (@run_id, @source_url, @source_file_name, @sha, @decision_year,
                TIMESTAMP(@retrieved_at), CURRENT_TIMESTAMP(), 'success',
                @rows_read, @rows_loaded, @rows_with_issues, @quality_issues_total, NULL)
        """
        params = [
            run,
            bigquery.ScalarQueryParameter("source_url", "STRING", manifest["source_url"]),
            bigquery.ScalarQueryParameter("source_file_name", "STRING", manifest["source_file_name"]),
            sha,
            bigquery.ScalarQueryParameter("decision_year", "INT64", manifest["decision_year"]),
            bigquery.ScalarQueryParameter("retrieved_at", "STRING", manifest["retrieved_at_utc"]),
            bigquery.ScalarQueryParameter("rows_read", "INT64", manifest["rows_read"]),
            bigquery.ScalarQueryParameter("rows_loaded", "INT64", manifest["rows_loaded"]),
            bigquery.ScalarQueryParameter("rows_with_issues", "INT64", manifest["rows_with_issues"]),
            bigquery.ScalarQueryParameter("quality_issues_total", "INT64", manifest["quality_issues_total"]),
        ]
        _query(client, audit_sql, params)

    downloaded = discover_and_download()
    parsed = parse_and_validate(downloaded)
    loaded = load_bronze_and_quality(parsed)
    transform_silver_and_audit(loaded)


verohallinto_vehicle_tax_ingestion()
