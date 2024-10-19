from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 5),
    'retries': 0
}

with DAG(
        dag_id='GCS_Sensor_Operators',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False
) as dag:
    task_1 = GCSObjectExistenceSensor(
        task_id='check_File',
        bucket='bwt-session-2024',
        object='airport1/sample_textfile.txt',
        google_cloud_conn_id='airflow_gcs_admin',
        poke_interval=30,
        timeout=300
    )

    task_2 = BashOperator(
        task_id='print_message',
        bash_command='echo "Hellooo... "',

    )

    task_1 >> task_2
