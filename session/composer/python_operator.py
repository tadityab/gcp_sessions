from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from simple_python_script import call_function


# Define python function
def simple_python_function():
    print(f"Hello, Welcome to PythonOperator...!")


def simple_python_arg_function(name):
    print(f"Hello, {name} welcome to PythonOperator")


# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 4),
    'retries': 1
}

# Define DAG
dag = DAG(
    dag_id='python_operator_first',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

task1 = PythonOperator(
    task_id='call_function',
    python_callable=simple_python_function,
    dag=dag
)

task2 = PythonOperator(
    task_id='call_arg_function',
    python_callable=simple_python_arg_function,
    op_args=['Cloud Composer'],
    dag=dag
)

task3 = PythonOperator(
    task_id='call_python_script',
    python_callable=call_function,
    op_kwargs={'name': 'cloudComposer'},
    dag=dag
)

task1
task2
task3
