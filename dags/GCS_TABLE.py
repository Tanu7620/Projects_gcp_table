from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "tanu"
}

with DAG(
    dag_id="gcs_to_bigquery_load",
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,
    catchup=False
) as dag:

    load_data = GCSToBigQueryOperator(
        task_id="load_csv_to_bq",
        bucket="your-bucket-name",
        source_objects=["customer/customer.csv"],
        destination_project_dataset_table="bigquery7620.datasetddl.sales_data",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
