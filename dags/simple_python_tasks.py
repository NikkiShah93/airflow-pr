from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'nshk'
}

def python_task():
    print('The Python task has been executed!')

with DAG(
    dag_id = 'simple_python_tasks',
    description='Simple Python tasks',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['python','test']
) as dag:
    task = PythonOperator(
        task_id='python_task',
        python_callable=python_task
    )

task

