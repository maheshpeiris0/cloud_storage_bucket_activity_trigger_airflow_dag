from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    'gcp_to_gbq',
    default_args=default_args,
    schedule_interval=None,
    tags=['GCP_to_GBQ'],
) as dag:
    gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='gcs_bucket',
        source_objects=['file.csv'],
        destination_project_dataset_table='project.dataset.table',
        schema_fields=[
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=True,
    )

    gcs_to_bq
    
