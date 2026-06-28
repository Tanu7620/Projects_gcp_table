from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "tanu"
}

with DAG(
    dag_id="gcs_to_bigquery_load",
    start_date=days_ago(1),
    schedule=None,
    catchup=False
) as dag:

    load_data = GCSToBigQueryOperator(
        task_id="load_csv_to_bq",

        bucket="your-bucket-name",

        source_objects=["customer/customer.csv"],

        destination_project_dataset_table="de-project.sales.customer",

        source_format="CSV",

        skip_leading_rows=1,

        write_disposition="WRITE_TRUNCATE",

        schema_fields=[
            {"name": "customer_name", "type": "STRING"},
            {"name": "customer_id", "type": "INTEGER"},
            {"name": "quantity", "type": "NUMERIC"},
            {"name": "salary", "type": "FLOAT"},
            {"name": "created_at", "type": "TIMESTAMP"}
        ]
    )

    load_data
