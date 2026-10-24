"""Manuaalinen Verohallinnon henkilöautojen veroarvojen Bronze-lataus.

DAG ei käytä GCS:ää. Työkirja ja NDJSON ovat rajatun kokoisia, tehtävän
sisäisiä välituotteita, joita ei kirjoiteta pysyvään levytilaan. Bronze säilyttää
jäsennetyt raakaarvot ja lähdejäljen BigQueryssä. Oikea kirjoitus BigQueryyn
edellyttää, että operaattori ajaa DAGin käsin (schedule=None).

Rakenne on tarkoituksella yksi tehtävä: lataus, jäsennys ja BigQuery-lataus
tehdään samassa taskissa, koska välitiedostot eivät säily taskien välillä, jos
taskit ajetaan eri workereilla (Celery/Kubernetes). Uudelleenajo on turvallinen,
koska lataus tunnistaa jo ladatun tiedoston sen tiivisteestä (SHA-256).
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

# Repositorion rakenne: paketti on src/-kansiossa dags/-kansion vieressä.
# Parempi pitkällä aikavälillä: asenna paketti (pip install -e .) ja poista tämä.
_REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_REPO_SRC) not in sys.path:
    sys.path.insert(0, str(_REPO_SRC))

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sdk import dag, get_current_context, task
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from tax_values.source import download_latest_workbook
from tax_values.xls_parser import parse_xls_bytes

GCP_CONN_ID = os.environ.get("VERO_GCP_CONN_ID", "")
PROJECT_ID = os.environ.get("VERO_BQ_PROJECT", "")
DATASET_ID = os.environ.get("VERO_BQ_DATASET", "")
LOCATION = os.environ.get("VERO_BQ_LOCATION") or None

# Huom: ympäristömuuttujat validoidaan vasta ajon aikana (_client), jotta
# DAGin jäsennys ei kaadu puuttuvaan konfiguraatioon.
BRONZE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.bronze_vehicle_tax_rows"

# Kustannussuoja: yksittäinen kysely ei saa skannata yli 100 MB.
MAX_BYTES_BILLED = 100_000_000

BRONZE_SCHEMA = [
    bigquery.SchemaField("source_row_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_file_sha256", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_file_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("decision_year", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("source_sheet", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_row_number", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("retrieved_at_utc", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("raw_values", "JSON", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at_utc", "TIMESTAMP", mode="REQUIRED"),
]


def _json_default(value: Any) -> Any:
    # date kattaa myös datetimen (datetime on dateen aliluokka). Alkuperäinen
    # versio käsitteli vain datetimen, ja pelkkä date olisi kaatanut ajon.
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)  # str säilyttää tarkkuuden, toisin kuin float
    raise TypeError(f"Not JSON serializable: {type(value).__name__}")


def _client() -> bigquery.Client:
    """Validoi konfiguraatio ja palauta BigQuery-asiakas.

    Kutsutaan ensimmäisenä, jotta puuttuva konfiguraatio paljastuu ennen
    lähdetiedoston latausta.
    """
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
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    return hook.get_client(project_id=PROJECT_ID, location=LOCATION)


def _already_loaded(client: bigquery.Client, source_sha256: str) -> bool:
    """Onko tämä täsmälleen sama lähdetiedosto jo Bronzessa?"""
    sql = f"SELECT 1 FROM `{BRONZE_TABLE}` WHERE source_file_sha256 = @sha LIMIT 1"
    config = bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES_BILLED,
        query_parameters=[bigquery.ScalarQueryParameter("sha", "STRING", source_sha256)],
    )
    try:
        result = client.query(sql, job_config=config, location=LOCATION).result()
    except NotFound:
        # Ensimmäinen ajo: taulua ei ole vielä. Alkuperäinen versio kaatui tähän.
        # Taulu luodaan latauksessa; pidemmän päälle se kannattaa luoda
        # Terraformilla ja klusteroida sarakkeella source_file_sha256.
        return False
    return next(iter(result), None) is not None


def _to_bronze_record(row: Any, meta: dict[str, Any], loaded_at: str) -> dict[str, Any]:
    """Muodosta yksi Bronze-rivi. Puhdas funktio, helppo yksikkötestata ilman Airflowta."""
    # Deterministinen rivitunniste: sama tiedosto + välilehti + rivi => sama tunniste.
    source_row_id = hashlib.sha256(
        f"{meta['sha256']}:{row.source_sheet}:{row.source_row}".encode("utf-8")
    ).hexdigest()
    return {
        "source_row_id": source_row_id,
        "source_file_sha256": meta["sha256"],
        "source_file_name": meta["file_name"],
        "source_url": meta["url"],
        "decision_year": meta["decision_year"],
        "source_sheet": row.source_sheet,
        "source_row_number": row.source_row,
        "retrieved_at_utc": meta["retrieved_at_utc"],
        "raw_values": row.values,
        "loaded_at_utc": loaded_at,
    }


@dag(
    dag_id="verohallinto_vehicle_tax_ingestion",
    schedule=None,  # vain manuaaliajo
    catchup=False,
    max_active_runs=1,  # estää rinnakkaiset ajot, joten tuplatarkistus riittää
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    tags=["verohallinto", "bigquery", "manual"],
)
def verohallinto_vehicle_tax_ingestion():
    # Retryt vain tähän yhteen paikkaan (aiemmin sekä default_args että taskissa).
    @task(retries=3, retry_delay=timedelta(minutes=2))
    def ingest() -> dict[str, Any]:
        # 1. Konfiguraatio ensin: epäonnistuu nopeasti ennen latausta.
        client = _client()
        try_number = get_current_context()["ti"].try_number

        # 2. Haetaan uusin työkirja. Sisältö pidetään muistissa (bytes);
        #    parseri ottaa suoraan bytet, joten levylle kirjoitus on turhaa.
        workbook = download_latest_workbook()
        sha256 = workbook.sha256

        # 3. Ohitus ennen jäsennystä: jos sama tiedosto on jo ladattu, ei
        #    kannata jäsentää sitä turhaan. (Aiemmin tarkistus tehtiin vasta
        #    jäsennyksen jälkeen.)
        if _already_loaded(client, sha256):
            return {"status": "skipped_existing_source_hash", "source_file_sha256": sha256}

        # 4. Jäsennys. Tyhjää ei ladata Bronzeen.
        rows = parse_xls_bytes(workbook.content)
        if not rows:
            raise ValueError("No passenger-car data rows found; refusing an empty Bronze load.")

        meta = {
            "sha256": sha256,
            # urlparse poistaa mahdollisen query-osan tiedostonimestä.
            "file_name": PurePosixPath(urlparse(workbook.link.url).path).name,
            "url": workbook.link.url,
            "decision_year": workbook.link.decision_year,
            "retrieved_at_utc": workbook.retrieved_at_utc,
        }
        loaded_at = datetime.now(timezone.utc).isoformat()

        # 5. NDJSON väliaikaistiedostoon, joka siivoutuu itsestään (aiemmin
        #    /tmp-hakemistoon jäi tiedostoja, joita ei koskaan poistettu).
        with tempfile.TemporaryFile() as ndjson:
            for row in rows:
                record = _to_bronze_record(row, meta, loaded_at)
                line = json.dumps(record, default=_json_default, ensure_ascii=False)
                ndjson.write((line + "\n").encode("utf-8"))
            ndjson.seek(0)

            load_job = client.load_table_from_file(
                ndjson,
                BRONZE_TABLE,
                job_config=bigquery.LoadJobConfig(
                    schema=BRONZE_SCHEMA,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    ignore_unknown_values=False,  # tuntematon kenttä = virhe, ei hiljaista pudotusta
                ),
                # Job-tunniste sisältää yrityskerran. Aiempi kiinteä tunniste
                # esti uudelleenyrityksen: BigQuery ei salli saman tunnisteen
                # käyttöä uudelleen, joten epäonnistuneen latausjobin jälkeen
                # kaikki retryt olisivat kaatuneet Conflict-virheeseen.
                job_id=f"verohallinto_bronze_{sha256[:24]}_try{try_number}",
                location=LOCATION,
            )
            load_job.result()

        # 6. Täsmäytys: ladattujen rivien määrän on vastattava jäsennettyjä rivejä.
        if load_job.output_rows != len(rows):
            raise RuntimeError(
                f"Row count mismatch: parsed {len(rows)}, loaded {load_job.output_rows}"
            )

        # Pieni yhteenveto XComiin seurantaa varten.
        return {
            "status": "loaded",
            "source_file_sha256": sha256,
            "rows_loaded": load_job.output_rows,
        }

    ingest()


verohallinto_vehicle_tax_ingestion()