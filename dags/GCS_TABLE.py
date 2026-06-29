from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "tanu"
}

# read your sql file
with open("/opt/airflow/dags/sql/load_data.sql", "r") as f:
    load_sql = f.read()

with DAG(
    dag_id="gcs_to_bigquery_load",
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,
    catchup=False
) as dag:

    # ----------------------------------------------------------
    # TASK 1: Pull file from GCS and load directly into table
    # ----------------------------------------------------------
    load_to_table = GCSToBigQueryOperator(
        task_id="load_csv_to_bq",
        bucket="your-bucket-name",                 # ← your GCS bucket
        source_objects=["customer/customer.csv"],   # ← your CSV file
        destination_project_dataset_table="bigquery7620.datasetddl.sales_data",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )

    # ----------------------------------------------------------
    # TASK 2: Run SQL for SAFE_CAST and lastingestiontime
    # ----------------------------------------------------------
    run_sql = BigQueryInsertJobOperator(
        task_id="safe_cast_and_insert",
        configuration={
            "query": {
                "query": load_sql,
                "useLegacySql": False
            }
        },
        gcp_conn_id="google_cloud_default"
    )

    # ----------------------------------------------------------
    # PIPELINE: load file → run sql
    # ----------------------------------------------------------
    load_to_table >> run_sql
