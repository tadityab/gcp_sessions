from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 4),
    'retries': 1
}

# Define DAG
dag = DAG(
    dag_id='python_operator_airflow_variable',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)


def call_function(name):
    print(f"{name} how are you...!")


# Retrieve variable
name_var = Variable.get("name")

task = PythonOperator(
    task_id='process_task',
    python_callable=call_function,
    op_args=[name_var],
    dag=dag
)

task