from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'nshk'
}
dag = DAG(
    dag_id='simple_bash_test',
    description='Simple Bash Operator',
    default_args = default_args,
    start_date=days_ago(1),
    tags=['bash','example']
)

task = BashOperator(
    task_id = 'simple_bash_task',
    bash_command='echo Hello World!',
    dag=dag
)

task
