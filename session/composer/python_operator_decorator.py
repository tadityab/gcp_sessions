from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 4),
    'retries': 1
}

# Define DAG
dag = DAG(
    dag_id='python_operator_decorator_first',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)


@task(dag=dag)
def call_function(name:str):
    print(f"Hello World How are you?")


task1 = call_function()
