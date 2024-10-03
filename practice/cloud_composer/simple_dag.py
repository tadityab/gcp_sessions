# Here we have covered, Simple Bash Operator

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 28),
    'retries': 1
}

# define DAG
dag = DAG(
    dag_id='simple_bash_operator',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Define a BashOperator task
bash_task = BashOperator(
    task_id='run_simple_bash_command',
    bash_command='echo "Hello, World! This is a bash command running in Cloud Composer."',
    dag=dag
)

# You can add more tasks if needed
bash_task_2 = BashOperator(
    task_id='another_bash_command',
    bash_command='date',
    dag=dag
)


bash_task_3 = BashOperator(
    task_id='env_variable_task',
    bash_command='echo "The value of the variable is $MY_VAR"',
    env={'MY_VAR': 'ApacheAirflow'},
    dag=dag
)

# Task dependencies (if needed)
bash_task >> bash_task_2 >> bash_task_3
