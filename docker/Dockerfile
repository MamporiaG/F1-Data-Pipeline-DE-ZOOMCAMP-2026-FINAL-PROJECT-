
FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow


RUN pip install --no-cache-dir \
    apache-airflow-providers-google==10.13.1 \
    dbt-bigquery==1.7.4 \
    dbt-core==1.7.4 \
    google-cloud-storage==2.14.0 \
    google-cloud-bigquery==3.17.1 \
    pandas==2.2.0 \
    pyarrow==15.0.0
