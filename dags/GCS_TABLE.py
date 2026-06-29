from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# ---------------------------------------------------
# Update ingestion timestamp
# ---------------------------------------------------
def update_ingestion_time():
    hook = BigQueryHook(gcp_conn_id="google_cloud_default")

    sql = """
        UPDATE `bigquery7620.datasetddl.sales_data`
        SET lastingestiontime = CURRENT_TIMESTAMP()
        WHERE lastingestiontime IS NULL
    """

    hook.run(sql)


default_args = {
    "owner": "tanu"
}

with DAG(
    dag_id="gcs_to_bq_ingestion",
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,
    catchup=False
) as dag:

    # ---------------------------------------------------
    # Task 1: Load file from GCS to BigQuery
    # ---------------------------------------------------
    load_data = GCSToBigQueryOperator(
        task_id="load_to_bq",

        bucket="your-bucket-name",

        source_objects=["customer/customer.csv"],

        destination_project_dataset_table="bigquery7620.datasetddl.sales_data",

        source_format="CSV",

        skip_leading_rows=1,

        write_disposition="WRITE_TRUNCATE",

        autodetect=True
    )

    # ---------------------------------------------------
    # Task 2: Update ingestion timestamp
    # ---------------------------------------------------
    update_timestamp = PythonOperator(
        task_id="update_ingestion_time",
        python_callable=update_ingestion_time
    )

    # Task dependency
    load_data >> update_timestamp
