from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
}

with DAG('bigquery_insert_job_example', default_args=default_args, schedule_interval='@daily') as dag:
    run_bigquery_query = BigQueryInsertJobOperator(
        task_id='run_bigquery_query',
        configuration={
            "query": {
                "query": "SELECT * FROM `bwt-learning-2024.bwt_session_cl.airport` LIMIT 1000",
                "useLegacySql": False,
            }
        },
    )

    run_bigquery_query
