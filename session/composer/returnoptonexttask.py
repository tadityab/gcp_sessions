from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 5),
    'retries': 1
}

# Define DAG
dag = DAG(
    dag_id='return_output_to_next_task',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)


def return_val_function():
    return 'Cloud Composer'


def print_val_function(ti):
    val = ti.xcom_pull(task_ids='task_1')
    print(f"Hello {val}")


task_1 = PythonOperator(
    task_id='task_1',
    python_callable=return_val_function,
    dag=dag
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=print_val_function,
    dag=dag,
    provide_context=True
)

task_1 >> task_2
