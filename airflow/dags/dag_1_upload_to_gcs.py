from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable


GCS_BUCKET   = "{{ var.value.gcs_bucket_name }}"
GCS_PREFIX   = "raw"          
LOCAL_DATA   = "/opt/airflow/data"
GCP_CONN_ID  = "google_cloud_default"

F1_FILES = [
    "circuits.csv",
    "constructors.csv",
    "constructor_results.csv",
    "constructor_standings.csv",
    "drivers.csv",
    "driver_standings.csv",
    "lap_times.csv",
    "pit_stops.csv",
    "qualifying.csv",
    "races.csv",
    "results.csv",
    "seasons.csv",
    "status.csv",
]

default_args = {
    "owner":            "f1-pipeline",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_1_upload_to_gcs",
    default_args=default_args,
    description="Backfill ingestion: upload F1 CSVs from local /data → GCS raw/",

    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   
    catchup=False,
    max_active_runs=1,
    tags=["f1", "ingestion", "gcs", "backfill"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")


    upload_tasks = []
    for filename in F1_FILES:
        task = LocalFilesystemToGCSOperator(
            task_id=f"upload_{filename.replace('.csv', '').replace('-', '_')}",
            src=f"{LOCAL_DATA}/{filename}",
            dst=f"{GCS_PREFIX}/{filename}",
            bucket=GCS_BUCKET,
            gcp_conn_id=GCP_CONN_ID,

            mime_type="text/csv",
            doc_md=f"Uploads {filename} to gs://{{{{ var.value.gcs_bucket_name }}}}/raw/{filename}",
        )
        upload_tasks.append(task)

    def verify_uploads(**context):
        """
        Lists all objects in the GCS raw/ prefix and confirms
        every expected file is present. Raises an error if any are missing.
        """
        hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        bucket = Variable.get("gcs_bucket_name")
        uploaded = hook.list(bucket_name=bucket, prefix=f"{GCS_PREFIX}/")

        uploaded_names = {obj.split("/")[-1] for obj in uploaded if obj.endswith(".csv")}
        missing = [f for f in F1_FILES if f not in uploaded_names]

        if missing:
            raise ValueError(f"Missing files in GCS: {missing}")

        print(f"✅ All {len(F1_FILES)} CSV files verified in GCS bucket: {bucket}/raw/")
        for obj in sorted(uploaded):
            print(f"   gs://{bucket}/{obj}")

    verify = PythonOperator(
        task_id="verify_gcs_upload",
        python_callable=verify_uploads,
        doc_md="Confirms all 13 CSV files are present in GCS before allowing downstream DAGs to run.",
    )

    # start → [all uploads in parallel] → verify → end
    start >> upload_tasks >> verify >> end
