import os
import pathlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import BranchPythonOperator, PythonVirtualenvOperator, is_venv_installed
from airflow.models import Variable

REGION = 'northwest'
CWD = os.getcwd()
PATH = pathlib.Path(CWD)
DATA_PATH = PATH / 'datasets'
DATA_PATH.mkdir(parents=True, exist_ok=True)
RAW_FILE_PATH = DATA_PATH / 'raw'
RAW_FILE_PATH.mkdir(parents=True, exist_ok=True)
CLEAN_FILE_PATH = DATA_PATH / 'clean'
CLEAN_FILE_PATH.mkdir(parents=True, exist_ok=True)
REPORT_FILE_PATH = DATA_PATH / 'report'
REPORT_FILE_PATH.mkdir(parents=True, exist_ok=True)
RAW_FILE_URL = 'https://raw.githubusercontent.com/NikkiShah93/airflow-pr/refs/heads/main/datasets/insurance.csv'
func_args = {
    'RAW_FILE_URL':RAW_FILE_URL,
    'RAW_FILE_PATH':RAW_FILE_PATH,
    'CLEAN_FILE_PATH':CLEAN_FILE_PATH,
    'REPORT_FILE_PATH':REPORT_FILE_PATH,
    'REGION':REGION
}
default_args = {
    'owner':'nshk',
    'provide_context':'true'
} 
def data_extract(**kwargs):
    import pandas as pd
    RAW_FILE_URL = kwargs['RAW_FILE_URL']
    RAW_FILE_PATH = kwargs['RAW_FILE_PATH']
    print('Downloading the file,...')
    df = pd.read_csv(RAW_FILE_URL)
    print(f'Download is compelete, dataset shape: {df.shape}')
    df.to_csv(f'{RAW_FILE_PATH}/raw_file.csv', index=False)
    print('File has been saved!')
    return f'{RAW_FILE_PATH}/raw_file.csv'
def branch_func(var_name):
    variable = Variable.get(var_name)
    if variable.startswith('filter'):
        return variable
    elif variable == 'groupby_task':
        return 'groupby_task'
def null_remover(**kwargs):
    import pandas as pd
    RAW_FILE_PATH = kwargs['RAW_FILE_PATH']
    CLEAN_FILE_PATH = kwargs['CLEAN_FILE_PATH']
    df = pd.read_csv(f'{RAW_FILE_PATH}/raw_file.csv')
    df = df.dropna()
    df.to_csv(f'{CLEAN_FILE_PATH}/clean_file.csv', index=False)
    return f'{CLEAN_FILE_PATH}/clean_file.csv'
def filter_region(**kwargs):
    import pandas as pd
    CLEAN_FILE_PATH = kwargs['CLEAN_FILE_PATH']
    REPORT_FILE_PATH = kwargs['REPORT_FILE_PATH']
    region = kwargs['REGION']
    df = pd.read_csv(f'{CLEAN_FILE_PATH}/clean_file.csv')
    df = df[df['region'] == region]
    df.to_csv(f'{REPORT_FILE_PATH}/filtered_by_region.csv', index=False)
def groupby(**kwargs):
    import pandas as pd
    CLEAN_FILE_PATH = kwargs['CLEAN_FILE_PATH']
    REPORT_FILE_PATH = kwargs['REPORT_FILE_PATH']
    df = pd.read_csv(f'{CLEAN_FILE_PATH}/clean_file.csv')
    smoker_df = df.groupby('smoker')[['age', 'bmi','charges']].mean().reset_index()
    smoker_df.to_csv(f'{REPORT_FILE_PATH}/grouped_by_smoker.csv', index=False)
    gender_df = df.groupby('sex')[['age', 'bmi','charges']].mean().reset_index()
    gender_df.to_csv(f'{REPORT_FILE_PATH}/grouped_gender.csv', index=False)
with DAG(
    dag_id='variable_based_branching',
    description='Variable based branching pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['variable','python','test','branching']
) as dag:
    if not is_venv_installed():
        print('The venv should be installed!')
    else:
        extract_date = PythonVirtualenvOperator(
            task_id='extract_data',
            python_callable=data_extract,
            requirements=['pandas'],
            provide_context=True,
            system_site_packages=False,
            op_kwargs=func_args
        )
        
        remove_nulls = PythonVirtualenvOperator(
            task_id = 'remove_nulls',
            python_callable=null_remover,
            requirements=['pandas'],
            provide_context=True,
            system_site_packages=False,
            op_kwargs=func_args
        )
        branch = BranchPythonOperator(
            task_id='branch',
            python_callable=branch_func,
            op_kwargs={'var_name':'transform_action'}
        )
        filter_by_region = PythonVirtualenvOperator(
            task_id='filter_by_region',
            python_callable=filter_region,
            requirements=['pandas'],
            provide_context=True,
            system_site_packages=False,
            op_kwargs=func_args
        )
        groupby_task = PythonVirtualenvOperator(
            task_id='groupby_task',
            python_callable=groupby,
            requirements=['pandas'],
            provide_context=True,
            system_site_packages=False,
            op_kwargs=func_args
        )
        

    extract_date >> remove_nulls >> branch >> [groupby_task,filter_by_region]
