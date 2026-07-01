from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

default_args = {"owner": "tanu"}

with open("/opt/airflow/dags/sql/SQL_SCHEMA_CUSYOMERS.sql", "r") as f:
    load_sql = f.read()

with DAG(
    dag_id="gcs_to_bigquery_load",
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,
    catchup=False
) as dag:


    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_csv_file",
        bucket="your-bucket-name",
        object="customer/customer.csv",
        gcp_conn_id="google_cloud_default",
        timeout=300,
        poke_interval=30
    )

    # TASK 1 — Pull file from GCS into staging
    load_to_staging = GCSToBigQueryOperator(
        task_id="load_csv_to_staging",
        bucket="your-bucket-name",
        source_objects=["customer/customer.csv"],
        destination_project_dataset_table="bigquery7620.datasetddl.staging_sales_data",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id="google_cloud_default"
    )
    
  
    load_to_target = BigQueryInsertJobOperator(
        task_id="safe_cast_and_insert",
        configuration={
            "query": {
                "query": load_sql,
                "useLegacySql": False
            }
        },
        gcp_conn_id="google_cloud_default"
    )

    # PIPELINE ORDER
    wait_for_file >> load_to_staging >> load_to_target
