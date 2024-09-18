import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'nshk'
}

def greeting(name):
    print(f'Hello {name}!')

def greeting_with_city(name, city):
    print(f'Hello {name} from {city}!')

with DAG(
    dag_id = 'simple_python_params',
    description = 'Simple Python ops with params',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['test','python','param']
) as dag:
    task1 = PythonOperator(
        task_id='greeting',
        python_callable=greeting,
        op_kwargs={'name':'Nikki'}
    )
    task2 = PythonOperator(
        task_id='greeting_with_city',
        python_callable=greeting_with_city,
        op_kwargs={'name':'Nikki', 'city':'Toronto'}
    )

task1 >> task2