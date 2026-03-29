"""
airflow/dags/dag_2_gcs_to_bigquery.py

DAG 2 — Load: GCS CSVs → BigQuery f1_raw
══════════════════════════════════════════

PURPOSE
-------
Reads every CSV file from the GCS raw/ prefix and loads it
into a corresponding table in the BigQuery f1_raw dataset.
This is the raw layer — data lands here exactly as it came
from Kaggle, with minimal transformation (just schema typing).

IDEMPOTENCY
-----------
write_disposition="WRITE_TRUNCATE" means every run replaces
the table contents from scratch. This is intentional — it makes
the pipeline safe to re-run after fixing source data issues.

DEPENDENCIES
------------
This DAG is triggered by dag_1_upload_to_gcs via TriggerDagRunOperator
(see the end of dag_1). It can also be triggered manually if you
want to reload specific tables without re-uploading the CSVs.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

# ── Constants ─────────────────────────────────────────────────────────────────
GCS_BUCKET = "{{ var.value.gcs_bucket_name }}"
BQ_PROJECT = "{{ var.value.gcp_project_id }}"
BQ_DATASET = "f1_raw"
GCS_PREFIX = "raw"
GCP_CONN_ID = "google_cloud_default"

# ── Table definitions ─────────────────────────────────────────────────────────
# Each entry: (gcs_filename, bq_table_name, schema)
# Schema uses BigQuery type names: STRING, INTEGER, FLOAT, DATE, BOOLEAN
TABLES = [
    (
        "circuits.csv",
        "circuits",
        [
            {"name": "circuitId", "type": "INTEGER"},
            {"name": "circuitRef", "type": "STRING"},
            {"name": "name", "type": "STRING"},
            {"name": "location", "type": "STRING"},
            {"name": "country", "type": "STRING"},
            {"name": "lat", "type": "FLOAT"},
            {"name": "lng", "type": "FLOAT"},
            {"name": "alt", "type": "STRING"},
            {"name": "url", "type": "STRING"},
        ],
    ),
    (
        "constructors.csv",
        "constructors",
        [
            {"name": "constructorId", "type": "INTEGER"},
            {"name": "constructorRef", "type": "STRING"},
            {"name": "name", "type": "STRING"},
            {"name": "nationality", "type": "STRING"},
            {"name": "url", "type": "STRING"},
        ],
    ),
    (
        "constructor_results.csv",
        "constructor_results",
        [
            {"name": "constructorResultsId", "type": "INTEGER"},
            {"name": "raceId", "type": "INTEGER"},
            {"name": "constructorId", "type": "INTEGER"},
            {"name": "points", "type": "FLOAT"},
            {"name": "status", "type": "STRING"},
        ],
    ),
    (
        "constructor_standings.csv",
        "constructor_standings",
        [
            {"name": "constructorStandingsId", "type": "INTEGER"},
            {"name": "raceId", "type": "INTEGER"},
            {"name": "constructorId", "type": "INTEGER"},
            {"name": "points", "type": "FLOAT"},
            {"name": "position", "type": "INTEGER"},
            {"name": "positionText", "type": "STRING"},
            {"name": "wins", "type": "INTEGER"},
        ],
    ),
    (
        "drivers.csv",
        "drivers",
        [
            {"name": "driverId", "type": "INTEGER"},
            {"name": "driverRef", "type": "STRING"},
            {"name": "number", "type": "STRING"},
            {"name": "code", "type": "STRING"},
            {"name": "forename", "type": "STRING"},
            {"name": "surname", "type": "STRING"},
            {"name": "dob", "type": "STRING"},
            {"name": "nationality", "type": "STRING"},
            {"name": "url", "type": "STRING"},
        ],
    ),
    (
        "driver_standings.csv",
        "driver_standings",
        [
            {"name": "driverStandingsId", "type": "INTEGER"},
            {"name": "raceId", "type": "INTEGER"},
            {"name": "driverId", "type": "INTEGER"},
            {"name": "points", "type": "FLOAT"},
            {"name": "position", "type": "INTEGER"},
            {"name": "positionText", "type": "STRING"},
            {"name": "wins", "type": "INTEGER"},
        ],
    ),
    (
        "lap_times.csv",
        "lap_times",
        [
            {"name": "raceId", "type": "INTEGER"},
            {"name": "driverId", "type": "INTEGER"},
            {"name": "lap", "type": "INTEGER"},
            {"name": "position", "type": "INTEGER"},
            {"name": "time", "type": "STRING"},
            {"name": "milliseconds", "type": "INTEGER"},
        ],
    ),
    (
        "pit_stops.csv",
        "pit_stops",
        [
            {"name": "raceId", "type": "INTEGER"},
            {"name": "driverId", "type": "INTEGER"},
            {"name": "stop", "type": "INTEGER"},
            {"name": "lap", "type": "INTEGER"},
            {"name": "time", "type": "STRING"},
            {"name": "duration", "type": "STRING"},
            {"name": "milliseconds", "type": "INTEGER"},
        ],
    ),
    (
        "qualifying.csv",
        "qualifying",
        [
            {"name": "qualifyId", "type": "INTEGER"},
            {"name": "raceId", "type": "INTEGER"},
            {"name": "driverId", "type": "INTEGER"},
            {"name": "constructorId", "type": "INTEGER"},
            {"name": "number", "type": "INTEGER"},
            {"name": "position", "type": "INTEGER"},
            {"name": "q1", "type": "STRING"},
            {"name": "q2", "type": "STRING"},
            {"name": "q3", "type": "STRING"},
        ],
    ),
    (
        "races.csv",
        "races",
        [
            {"name": "raceId", "type": "INTEGER"},
            {"name": "year", "type": "INTEGER"},
            {"name": "round", "type": "INTEGER"},
            {"name": "circuitId", "type": "INTEGER"},
            {"name": "name", "type": "STRING"},
            {"name": "date", "type": "STRING"},
            {"name": "time", "type": "STRING"},
            {"name": "url", "type": "STRING"},
            {"name": "fp1_date", "type": "STRING"},
            {"name": "fp1_time", "type": "STRING"},
            {"name": "fp2_date", "type": "STRING"},
            {"name": "fp2_time", "type": "STRING"},
            {"name": "fp3_date", "type": "STRING"},
            {"name": "fp3_time", "type": "STRING"},
            {"name": "quali_date", "type": "STRING"},
            {"name": "quali_time", "type": "STRING"},
            {"name": "sprint_date", "type": "STRING"},
            {"name": "sprint_time", "type": "STRING"},
        ],
    ),
    (
        "results.csv",
        "results",
        [
            {"name": "resultId", "type": "INTEGER"},
            {"name": "raceId", "type": "INTEGER"},
            {"name": "driverId", "type": "INTEGER"},
            {"name": "constructorId", "type": "INTEGER"},
            {"name": "number", "type": "STRING"},
            {"name": "grid", "type": "INTEGER"},
            {"name": "position", "type": "STRING"},
            {"name": "positionText", "type": "STRING"},
            {"name": "positionOrder", "type": "INTEGER"},
            {"name": "points", "type": "FLOAT"},
            {"name": "laps", "type": "INTEGER"},
            {"name": "time", "type": "STRING"},
            {"name": "milliseconds", "type": "STRING"},
            {"name": "fastestLap", "type": "STRING"},
            {"name": "rank", "type": "STRING"},
            {"name": "fastestLapTime", "type": "STRING"},
            {"name": "fastestLapSpeed", "type": "STRING"},
            {"name": "statusId", "type": "INTEGER"},
        ],
    ),
    (
        "seasons.csv",
        "seasons",
        [
            {"name": "year", "type": "INTEGER"},
            {"name": "url", "type": "STRING"},
        ],
    ),
    (
        "status.csv",
        "status",
        [
            {"name": "statusId", "type": "INTEGER"},
            {"name": "status", "type": "STRING"},
        ],
    ),
]

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "f1-pipeline",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_2_gcs_to_bigquery",
    default_args=default_args,
    description="Load F1 CSVs from GCS into BigQuery f1_raw dataset",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["f1", "bigquery", "load", "backfill"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    load_tasks = []
    for filename, table_name, schema in TABLES:
        task = GCSToBigQueryOperator(
            task_id=f"load_{table_name}",
            bucket=GCS_BUCKET,
            source_objects=[f"{GCS_PREFIX}/{filename}"],
            source_format="CSV",
            skip_leading_rows=1,
            destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}",
            schema_fields=schema,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            allow_quoted_newlines=True,
            gcp_conn_id=GCP_CONN_ID,
        )
        load_tasks.append(task)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transform",
        trigger_dag_id="dag_3_dbt_transform",
        wait_for_completion=False,  # fire and move on
        doc_md="Triggers the dbt transformation DAG after all tables are loaded.",
    )

    # All 13 load tasks run in parallel, then trigger dbt
    start >> load_tasks >> trigger_dbt >> end
