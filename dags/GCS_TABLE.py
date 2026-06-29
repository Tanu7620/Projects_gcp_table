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
# ↑ DAG reads your SQL file and runs it

with DAG(
    dag_id="gcs_to_bigquery_load",
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,
    catchup=False
) as dag:

    # ----------------------------------------------------------
    # TASK 1: Pull file from GCS → load into staging table
    # ----------------------------------------------------------
    load_to_staging = GCSToBigQueryOperator(
        task_id="load_csv_to_staging",
        bucket="your-bucket-name",
        source_objects=["customer/customer.csv"],
        destination_project_dataset_table="bigquery7620.datasetddl.staging_sales_data",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )

    # ----------------------------------------------------------
    # TASK 2: Run SQL → SAFE_CAST and INSERT INTO target table
    # ----------------------------------------------------------
    load_to_target = BigQueryInsertJobOperator(
        task_id="safe_cast_and_insert",
        configuration={
            "query": {
                "query": load_sql,       # ← runs your load_data.sql
                "useLegacySql": False
            }
        },
        gcp_conn_id="google_cloud_default"
    )

    # ----------------------------------------------------------
    # PIPELINE: staging → safe cast → target table
    # ----------------------------------------------------------
    load_to_staging >> load_to_target
