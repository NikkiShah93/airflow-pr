import time
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'nshk'
}

def task1():
    print('The task1 has been executed!')


def task2():
    time.sleep(4)
    print('The task2 has been executed!')

    print('The task1 has been executed!')

with DAG(
    dag_id = 'simple_python_tasks',
    description='Simple Python tasks',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['python','test']
) as dag:
    task1 = PythonOperator(
        task_id='task1',
        python_callable=task1
    )
    task2 = PythonOperator(
        task_id='task2',
        python_callable=task2
    )

task1 >> task2

