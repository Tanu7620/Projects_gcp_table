from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "tanu"
}

with DAG(
    dag_id="local_to_gcs_to_bigquery",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:

    # Task 1: Upload Local File to GCS
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src="/home/airflow/data/customer.csv",   # Change this path
        dst="customer/customer.csv",
        bucket="my-demo-bucket"
    )

    # Task 2: Load CSV into BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket="my-demo-bucket",
        source_objects=["customer/customer.csv"],
        destination_project_dataset_table="my_project.demo_dataset.customer",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name":"customer_id","type":"INTEGER"},
            {"name":"customer_name","type":"STRING"},
            {"name":"city","type":"STRING"},
            {"name":"age","type":"INTEGER"},
            {"name":"salary","type":"FLOAT"}
        ]
    )

    upload_file >> load_to_bq
