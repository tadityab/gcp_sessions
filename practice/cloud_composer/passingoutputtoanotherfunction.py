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
    dag_id='pythonoperator_passingoutput_to_task',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)


def first_function():
    message = 'Hello from the first task..!'
    return message


def second_function(ti):
    msg = ti.xcom_pull(task_ids='task_1')
    print(f"Received message: {msg}")


task_1 = PythonOperator(
    task_id='task_1',
    python_callable=first_function,
    dag=dag
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=second_function,
    dag=dag,
    provide_context=True  # This allow access to the task instance('ti')
)

task_1 >> task_2
