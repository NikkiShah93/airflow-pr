from datetime import datetime, timedelta
from random import choice
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner':'nshk',
    'provide_context':'true'
}
def random_num_picker():
    return choice([1,2,3,4])
def branch(ti):
    val = ti.xcom_pull(task_ids='random_num_picker')
    if val%2==0:
        return 'even'
    else:
        return 'odd'
def even():
    print('The number is even!')
def odd():
    print('The number is odd')
with DAG(
    dag_id = 'cron_catchup_backfill',
    description='Catchup and backfill using cron expression',
    start_date=days_ago(30),
    schedule_interval='0 */12 * * 6,0',
    catchup=True,
    tags=['test', 'python','bash','cron','catchup','backfill']
 ) as dag:
    task_1 = BashOperator(
        task_id='task_1',
        bash_command='echo Task 1 has been executed!'
    )
    task_2 = PythonOperator(
        task_id='random_num_picker',
        python_callable=random_num_picker
    )
    branch = BranchPythonOperator(
        task_id = 'branch',
        python_callable=branch
    )
    even = PythonOperator(
        task_id='even',
        python_callable=even 
    )
    odd = PythonOperator(
        task_id = 'odd',
        python_callable=odd
    )
    final = EmptyOperator(
        task_id='final_step'
    )

task_1 >> task_2 >> branch >> [odd, even] 
odd >> final