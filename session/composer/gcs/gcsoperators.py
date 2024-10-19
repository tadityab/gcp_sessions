from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 5),
    'retries': 0
}

# Define DAG
dag = DAG(
    dag_id='GCS_Operators',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

task_1 = GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name='bwt_gcs_airflow',
    storage_class='STANDARD',
    location='US',
    project_id=Variable.get('project_id'),
    gcp_conn_id='airflow_gcs_admin'
)

task_2 = GCSListObjectsOperator(
    task_id='list_objects',
    bucket='bwt-session-2024',
    prefix='airport/',
    delimiter='.csv',
    gcp_conn_id='airflow_gcs_admin'
)


def call_function(ti):
    files = ti.xcom_pull(task_ids='list_objects')
    print(files)


task_3 = PythonOperator(
    task_id='print_objects',
    python_callable=call_function,
    provide_context=True
)

task_4 = GCSToGCSOperator(
    task_id='transfer_file',
    source_bucket='bwt-session-2024',
    source_object='airport/*',
    destination_bucket='bwt-session-2024',
    destination_object='airport1/',
    move_object=False,
    gcp_conn_id='airflow_gcs_admin',
    dag=dag
)

task_4