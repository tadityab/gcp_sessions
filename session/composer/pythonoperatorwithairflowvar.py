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
    dag_id='passing_airflow_var',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# retrieve value of airflow variable
name_var = Variable.get('airflow_bwt_var')

# define function
def call_function(name):
    print(f"Airflow Variable value: {name_var}")


task_1= PythonOperator(
    task_id='call_airflow_var',
    python_callable=call_function,
    op_args=[name_var],
    dag=dag
)


