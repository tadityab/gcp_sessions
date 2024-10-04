# 1. simple pythonoperator
# 2. passing argument to pythonoperator
# 3. calling python script using pythonoperator

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 3),
    'retries': 1
}

# Define DAG
dag = DAG(
    dag_id='bash_operator_script_call',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

run_script = BashOperator(
    task_id='call_bash_Script',
    bash_command='gsutil cp gs://us-central1-cloud-composer--c8efab36-bucket/dags/simple_bash_script.sh /tmp/simple_bash_script.sh && chmod +x /tmp/simple_bash_script.sh && ls -l /tmp && sh /tmp/simple_bash_script.sh ',
    dag=dag
)

run_script1 = BashOperator(
    task_id='call_bash_Script1',
    bash_command='sh /home/airflow/gcs/dags/simple_bash_script.sh ',
    dag=dag
)

run_script
run_script1