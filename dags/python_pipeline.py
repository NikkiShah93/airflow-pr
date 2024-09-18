import time
import os
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed

cwd = os.getcwd()

default_args = {
    'owner':'nshk'
}

@dag(
    start_date=days_ago(1),
    schedule='@once',
    tags=['python','pipeline','test'],
    catchup=False
)
def python_pipeline():
    if not is_venv_installed():
        print('venv should be installed!')
    else:
        @task.virtualenv(
            task_id='read_file',
            requirements=['pandas'],
            system_site_packages=False
        )
        def read_csv():
            import pandas as pd
            df = pd.read_csv('https://raw.githubusercontent.com/NikkiShah93/airflow-pr/refs/heads/main/datasets/insurance.csv')
            print(df.head())
            return df.to_json()
        read_file = read_csv()
        @task.virtualenv(
            task_id='clean_file',
            requirements=['pandas'],
            system_site_packages=False
        )
        def clean_file(**kwargs):
            import pandas as pd
            ti = kwargs['ti']
            json_data = ti.xcom_pull(task_ids='read_file')
            df = pd.read_json(json_data)
            df = df.dropna()
            return df.to_json()
        clean_data = clean_file()
        read_file >> clean_data

python_pipeline = python_pipeline()