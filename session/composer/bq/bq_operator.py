from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryDeleteDatasetOperator, BigQueryDeleteTableOperator, BigQueryExecuteQueryOperator, BigQueryGetDatasetOperator, BigQueryInsertJobOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 0
}

with DAG(
    dag_id='Create_empty_dataset',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # task_1 = BigQueryCreateEmptyDatasetOperator(
    #     task_id='create_dataset',
    #     project_id=Variable.get('project_id'),
    #     dataset_id='bwt_airflow_ds',
    #     location='US',
    #     gcp_conn_id='gcp_admin'
    # )

    task_2 = BigQueryCreateEmptyTableOperator(
        task_id='create_table',
        project_id=Variable.get('project_id'),
        dataset_id='bwt_airflow_ds',
        table_id='employee_tbl',
        schema_fields=[
            {"name":"id", "type":"integer","mode":"REQUIRED"},
            {"name":"name", "type":"string","mode":"nullable"}

        ],
        location='US'
    )

    task_2