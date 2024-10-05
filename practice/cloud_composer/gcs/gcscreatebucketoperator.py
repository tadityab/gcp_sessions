from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSListObjectsOperator
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 4),
    'retries': 1
}

# Define DAG
dag = DAG(
    dag_id='gcscreatebucketoperator',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

task_1 = GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name='gcs_airflow',
    storage_class='STANDARD',
    location='US',
    project_id=Variable.get("project_id"),
    gcp_conn_id='cloud_storage_admin_details'
)

task_2 = GCSListObjectsOperator(
    task_id='list_object',
    bucket='bwt-session-2024',
    prefix='airport/',
    gcp_conn_id='cloud_storage_admin_details',
    dag=dag
    # prefix='airport/'  # Optional: Use a prefix to list files in a subfolder (e.g., 'data/')
    # delimiter='.csv',        # Optional: Filter objects with a certain delimiter (e.g., list only .csv files)
)


# Python function to process the listed objects
def process_files(**kwargs):
    # Retrieve the list of objects from XCom
    ti = kwargs['ti']
    files = ti.xcom_pull(task_ids='list_object')
    print(f"List of files in GCS: {files}")


# Task to process the list of files
process_gcs_files = PythonOperator(
    task_id='process_gcs_files',
    python_callable=process_files,
    provide_context=True,
    dag=dag
)

task_2 >> process_gcs_files
