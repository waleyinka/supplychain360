from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from pendulum import datetime, duration

import include.utils.config as config
from include.load.supplychain_ingestion import ingest_master_data
from include.load.ingest_store_sales import ingest_store_sales
from include.load.ingest_s3_datasets import ingest_all_s3_source_data

from include.utils.email_handlers import notify_success
from airflow.providers.smtp.notifications.smtp import SmtpNotifier

failure_notifier = SmtpNotifier(
    from_email="omowalefst@gmail.com",
    to="iamomowale@example.com",
    subject="[Airflow Alert] DAG: {{ dag.dag_id }} | Task: {{ ti.task_id }} has FAILED",
)

default_args = {
    "owner": "waleyinka",
    "start_date": datetime(2024, 3, 14),
    "email": ["iamomowale@outlook.com"],
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(seconds=30)
}

with DAG(
    dag_id="supplychain360_dag",
    default_args=default_args,
    description='Full ingestion from Postgres and Google Sheets to Snowflake',
    schedule=None,
    catchup=False,
    on_success_callback=notify_success,
    on_failure_callback=[failure_notifier],
    tags=["supplychain", "snowflake", "s3", "dbt_cloud"],
) as dag:

    # Task 1: Google Sheets (Store Locations)
    ingest_store_locations = PythonOperator(
        task_id='ingest_google_sheets',
        python_callable=ingest_master_data
    )

    # Task 2: Postgres (Daily Sales)
    ingest_sales_data = PythonOperator(
        task_id='ingest_postgres_sales',
        python_callable=ingest_store_sales
    )

    # Task 3: Source S3 (Suppliers, Products, Inventory, etc.)
    ingest_s3_datasets = PythonOperator(
        task_id='ingest_source_s3_datasets',
        python_callable=ingest_all_s3_source_data
    )
    
    # Task 4: dbt Cloud Transformation (Marts)
    run_dbt_jobs = DbtCloudRunJobOperator(
        task_id="run_dbt_transformation",
        dbt_cloud_conn_id="dbt_cloud_default",
        job_id=config.DBT_JOB_ID,
        wait_for_termination=True,
        check_interval=30,
        timeout=3600,
    )
    
    [ingest_store_locations, ingest_sales_data, ingest_s3_datasets]  >> run_dbt_jobs
    
    # ingest_store_locations >> ingest_sales_data >> ingest_s3_datasets
    
