import time
import os
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed

cwd = os.getcwd()

default_args = {
    'owner':'nshk',
    'provide_context':True
}

with DAG(
    dag_id='python_pipeline',
    description='Python data pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['python','pipeline','test']
) as dag:
# def python_pipeline():
    if not is_venv_installed():
        print('venv should be installed!')
    else:
        @task.virtualenv(
            task_id='read_file',
            requirements=['pandas'],
            provide_context=True,
            system_site_packages=False
        )
        def read_csv():
            import pandas as pd
            df = pd.read_csv('https://raw.githubusercontent.com/NikkiShah93/airflow-pr/refs/heads/main/datasets/insurance.csv')
            print(df.head())
            return df.to_json()
        read_file = read_csv()
        @task.virtualenv(
            task_id='remove_nulls',
            requirements=['pandas'],
            provide_context=True,
            system_site_packages=False
        )
        def remove_nulls(ti):
            import pandas as pd
            json_data = ti.xcom_pull(task_ids='read_file')
            df = pd.read_json(json_data)
            df = df.dropna()
            return df.to_json()
        clean_data = remove_nulls()
        @task.virtualenv(
            task_id='group_smokers',
            requirements=['pandas'],
            provide_context=True,
            system_site_packages=False
        )
        def group_smokers(ti):
            import pandas as pd
            json_data = ti.xcom_pull(task_ids='remove_nulls')
            df = pd.read_json(json_data)
            df = df.groupby('smoker').agg({
                'age':'mean',
                'bmi':'mean',
                'charges':'mean'
            }).reset_index()
            df.to_csv(f'../dataset/smoker.csv', index=False)
            # return df.to_json()
        smoker_data = group_smokers()
        @task.virtualenv(
            task_id='group_region',
            requirements=['pandas'],
            provide_context=True,
            system_site_packages=False
        )
        def group_region(ti):
            import pandas as pd
            json_data = ti.xcom_pull(task_ids='remove_nulls')
            df = pd.read_json(json_data)
            df = df.groupby('region').agg({
                'age':'mean',
                'bmi':'mean',
                'charges':'mean'
            }).reset_index()
            df.to_csv(f'../dataset/region.csv', index=False)
            # return df.to_json()
        region_data = group_region()
read_file >> clean_data
clean_data >> [region_data, smoker_data]

# python_pipeline = python_pipeline()