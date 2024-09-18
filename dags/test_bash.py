from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'nshk'
}
with DAG(
    dag_id='simple_bash_test',
    description='Simple Bash Operator',
    default_args = default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['bash','example']
) as dag:

    task1 = BashOperator(
        task_id = 'simple_bash_task1',
        bash_command='echo task1 has been executed'
    )

    task2 = BashOperator(
        task_id='simple_bash_task2',
        bash_command='echo task2 has been executed'
    )

task1 >> task2
