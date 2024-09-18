from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator
import os 

cwd = os.getcwd()

default_args = {
    'owner':'nshk'
}

with DAG(
    dag_id='multiple_bash_script',
    description='Executing multiple bash scripts',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    template_searchpath=f'{cwd.replace('\\','/')}/dags/bash_scripts',
    tags=['bash','multiple']
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='task1.sh'
    )
    task2 = BashOperator(
        task_id='task2',
        bash_command='task2.sh'
    ) 
    task3 = BashOperator(
        task_id='task3',
        bash_command='task3.sh'
    ) 
    task4 = BashOperator(
        task_id='task4',
        bash_command='task4.sh'
    ) 
    task5 = BashOperator(
        task_id='task5',
        bash_command='task5.sh'
    ) 
    task6 = BashOperator(
        task_id='task6',
        bash_command='task6.sh'
    ) 
    task7 = BashOperator(
        task_id='task7',
        bash_command='task7.sh'
    ) 

task1 >> [task2, task3, task4]
task5 << task2
task6 << task3
task7 << task4
