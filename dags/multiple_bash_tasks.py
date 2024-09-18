from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'nshk'
}

with DAG(
    dag_id = 'multiple_bash_tasks',
    default_args=default_args,
    start_date=days_ago(2),
    schedule_interval=timedelta(1),
    tags=['bash', 'test','multiple']
) as dag:
    t1 = BashOperator(
        task_id='simple_counter',
        bash_command= """
        echo simple counter has started!
        for i in {1..10}
        do 
            echo $i       
        done
        echo simple counter has ended!
"""
    )

    t2 = BashOperator(
        task_id = 'second_task',
        bash_command='''
        echo second task has started!
        sleep 4
        echo second task has ended!
        '''
    )

    t3 = BashOperator(
        task_id='third_task',
        bash_command="echo third task has been executed!"
    )

    t4 = BashOperator(
        task_id='last_task',
        bash_command='echo last task has been executed!'
    )

t1 >> t2
t1 >> t3

t4 << t2
t4 << t3