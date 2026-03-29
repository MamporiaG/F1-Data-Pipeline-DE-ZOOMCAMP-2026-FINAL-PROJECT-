from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DBT_DIR      = "/opt/airflow/dbt"
DBT_PROFILES = "/opt/airflow/dbt"  

default_args = {
    "owner":            "f1-pipeline",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_3_dbt_transform",
    default_args=default_args,
    description="Run dbt staging → marts → tests → docs on BigQuery",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["f1", "dbt", "transform", "bigquery"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir {DBT_PROFILES}",
        doc_md="Installs dbt packages from packages.yml (e.g., dbt_utils, dbt_expectations).",
    )

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_DIR} && dbt run "
            f"--select staging "
            f"--profiles-dir {DBT_PROFILES} "
            f"--target prod"
        ),
        doc_md="""
        Runs all models in models/staging/ — these are BigQuery VIEWS.
        They clean, rename, and type-cast the raw tables.
        Output dataset: f1_staging
        """,
    )


    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_DIR} && dbt run "
            f"--select marts "
            f"--profiles-dir {DBT_PROFILES} "
            f"--target prod"
        ),
        doc_md="""
        Runs all models in models/marts/ — these are BigQuery TABLES.
        They join staging models and compute business metrics.
        Output dataset: f1_marts
        """,
    )


    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && dbt test "
            f"--profiles-dir {DBT_PROFILES} "
            f"--target prod"
        ),
        doc_md="""
        Runs all data quality tests defined in schema.yml files.
        Tests: unique, not_null, accepted_values, relationships.
        Fails the task (and alerts you) if any test fails.
        """,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_DIR} && dbt docs generate "
            f"--profiles-dir {DBT_PROFILES} "
            f"--target prod"
        ),
        doc_md="Generates the dbt documentation site (catalog.json + manifest.json).",
    )

    start >> dbt_deps >> dbt_staging >> dbt_marts >> dbt_test >> dbt_docs >> end
