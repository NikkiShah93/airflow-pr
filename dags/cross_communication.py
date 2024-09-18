import time
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'nshk'
}

def add_one(value):
    print(f'The input value: {value}')
    return value + 1
def mult_100(ti):
    value=ti.xcom_pull(task_ids='add_one')
    print(f'The pulled value: {value}')
    return value * 100
def sub_nine(ti):
    value=ti.xcom_pull(task_ids='mult_100')
    print(f'The pulled value: {value}')
    return value - 9 
def final_result(ti):
    result = ti.xcom_pull(task_ids='sub_nine')
    print(f'The final result: {result}')

with DAG(
    dag_id='cross_communication',
    description='XCom sample',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=timedelta(1),
    tags=['python','test','xcom']
) as dag:
    task1 = PythonOperator(
        task_id='add_one',
        python_callable=add_one,
        op_kwargs={'value':10}
    )
    task2 = PythonOperator(
        task_id='mult_100',
        python_callable=mult_100
    )
    task3 = PythonOperator(
        task_id='sub_nine',
        python_callable=sub_nine
    )
    task4 = PythonOperator(
        task_id='final_result',
        python_callable=final_result
    )

task1 >> task2 >> task3 >> task4