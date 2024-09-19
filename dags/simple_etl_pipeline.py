from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task


default_args = {
    'owner':'nshk'
}

with DAG(
    dag_id = 'simple_etl_pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@monthly',
    tags=['bash','python', 'pipeline','etl']
) as dag:
    extract_data = BashOperator(
        task_id='extract_data',
        bash_command= "curl -o /localdir/raw_data/raw_data.json -L 'https://publicdata.com/data'"
    )
    @task.virtualenv(
            task_id='remove_nulls',
            requirements=['pandas'],
            provide_context=True,
            system_site_packages=False
        )
    def remove_nulls(ti):
        import pandas as pd
        df = pd.read_json('/localdir/raw_data/raw_data.json')
        df = df.dropna()
        df.to_csv('/localdir/clean_data/clean_data.csv', index=False)
    null_remover = remove_nulls()
    @task.virtualenv(
            task_id='aggregation_1',
            requirements=['pandas'],
            provide_context=True,
            system_site_packages=False
        )
    def aggregation_1():
        import pandas as pd
        df = pd.read_csv('/localdir/clean_data/clean_data.csv')
        df = df.group_by('attr_1').agg({'some_attr':'mean', 'other_attr':'sum'})
        df.to_csv('/localdir/aggr_data/aggr_attr_1.csv', index=False)
    aggr_1 = aggregation_1()
    @task.virtualenv(
            task_id='aggregation_2',
            requirements=['pandas'],
            provide_context=True,
            system_site_packages=False
        )
    def aggregation_2():
        import pandas as pd
        df = pd.read_csv('/localdir/clean_data/clean_data.csv')
        df = df.group_by('attr_2').agg({'some_attr':'mean', 'other_attr':'sum'})
        df.to_csv('/localdir/aggr_data/aggr_attr_2.csv', index=False)
    aggr_2 = aggregation_2()

extract_data >> null_remover >> [aggr_1, aggr_2]
 