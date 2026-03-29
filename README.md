# F1 Data Pipeline

End-to-end data pipeline for Formula 1 historical data (1950–2024). Takes 13 raw CSV files from Kaggle and turns them into clean, analysis-ready tables in BigQuery, with a Looker Studio dashboard on top.

Built as a portfolio project to practice data engineering.

---

## Stack

- **Airflow** (Docker) — orchestration
- **Terraform** — provisions GCS bucket and BigQuery datasets
- **Google Cloud Storage** — raw data lake
- **BigQuery** — data warehouse
- **dbt** — transformations
- **Looker Studio** — dashboard

---

## How it works

Three Airflow DAGs run in sequence:

1. `dag_1_upload_to_gcs` — uploads the 13 CSV files to GCS
2. `dag_2_gcs_to_bigquery` — loads them into BigQuery as raw tables
3. `dag_3_dbt_transform` — runs dbt to build staging views and mart tables


---

## Data model

```
f1_raw        → 13 tables, loaded directly from CSV, never touched
f1_staging    → 10 views, cleaned and typed (nulls fixed, columns renamed)
f1_marts      → 6 tables, joined and aggregated, ready for dashboards
```

Mart tables:
- `fct_race_results` — one row per driver per race, all context joined in
- `fct_driver_standings` — championship points after each race round
- `fct_constructor_standings` — same but for teams
- `dim_drivers` — career stats per driver (wins, podiums, win rate etc.)
- `dim_constructors` — career stats per team
- `dim_circuits` — circuit info with race counts and lat/lng for map charts

---

## Setup

Docker Desktop, Terraform, Python 3.11+, gcloud CLI, a GCP account.

```bash
# 1. Clone the repo and create the folders that aren't tracked by git
mkdir credentials data airflow/logs airflow/plugins dbt/macros dbt/seeds dbt/tests

# 2. Download the dataset from Kaggle and put all 13 CSVs in data/
# https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020

# 3. Download your GCP service account key and put it at:
# credentials/gcp_credentials.json

# 4. Copy .env.example to .env and fill in your GCP project ID and bucket name

# 5. Provision GCP resources
cd terraform
terraform init
terraform apply -var="project_id=YOUR_PROJECT_ID"

# 6. Start Airflow
cd ../docker
docker compose up -d --build
```

Then open `localhost:8080` (admin / admin), set up the `google_cloud_default` connection under Admin → Connections, add your `gcs_bucket_name` and `gcp_project_id` variables under Admin → Variables, and trigger DAG 1.

Full step-by-step setup is in `F1_Pipeline_Complete_Guide.docx`.

---

## Dataset

Kaggle — [Formula 1 World Championship (1950–2020)](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020) by Rohan Rao. Updated through 2024.

## Dashboard

Dashboard is available at: https://lookerstudio.google.com/reporting/66b96bc0-e0c0-45d4-9d8a-29518d92fef6/page/cZOtF 
