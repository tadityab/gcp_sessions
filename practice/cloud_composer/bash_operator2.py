from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 28),
    'retries': 1
}

# Define a DAG
dag = DAG(
    dag_id='call_bash_script',
    default_args=default_args,
    description='To call bash script',
    schedule_interval=None,
    catchup=False
)

# task which will call the bash script
run_script = BashOperator(
    task_id='execute_bash_script',
    bash_command='pwd; sh /home/airflow/gcs/dags/simple_bash.sh',
    dag=dag
)
