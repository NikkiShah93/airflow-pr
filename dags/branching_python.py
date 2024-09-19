from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from random import choice

default_args = {
    'owner':'nshk'
}

def random_num_picker():
    return choice([1, 2, 3, 4])
def even_odd_branch(ti):
    val = ti.xcom_pull(task_ids='random_num_picker')
    if val%2==0:
        return 'even'
    else:
        return 'odd'
def even():
    print('The number picked was even!')
def odd():
    print('The number picked was odd!')
with DAG(
    dag_id='branching_python',
    description='Pipeline with branching',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['test','python','branching']
) as dag:
    random_num = PythonOperator(
        task_id='random_num_picker',
        python_callable=random_num_picker
    )
    branch = BranchPythonOperator(
        task_id='even_odd_branch',
        python_callable=even_odd_branch
    )
    even_num = PythonOperator(
        task_id='even',
        python_callable=even
    )
    odd_num = PythonOperator(
        task_id='odd',
        python_callable=odd
    )

random_num >> branch >> [even_num, odd_num]
    