# pip install apache-airflow-providers-google
# pip install airflow

from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 3),
    'retries': 1
}

# Define DAG
dag = DAG(
    dag_id='bash_operator_1',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Define BashOperator task
bash_task1 = BashOperator(
    task_id='bash_first_task',
    bash_command='echo "Hello BWT Class...! How are you?"',
    dag=dag
)

bash_task2 = BashOperator(
    task_id='bash_second_task',
    bash_command='echo "we are good."',
    dag=dag
)

bash_task3 = BashOperator(
    task_id='bash_third_task',
    bash_command='echo "Hello, This is variable value: $MY_VAR"',
    env={'MY_VAR': 'CloudComposer'},
    dag=dag
)

bash_task4 = BashOperator(
    task_id='bash_fourth_task',
    bash_command='echo "4th task"',
    dag=dag
)

bash_task1 >> [bash_task2, bash_task3] >> bash_task4
