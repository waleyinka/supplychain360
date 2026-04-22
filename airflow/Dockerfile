FROM apache/airflow:3.2.0

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    awswrangler \
    apache-airflow-providers-dbt-cloud \
    dbt-core \
    dbt-snowflake