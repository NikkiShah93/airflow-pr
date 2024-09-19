import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner':'nshk'
}

with DAG(
    dag_id = 'sql_pipeline',
    description='Simple SQL pipeline',
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['sql', 'pipeline','test']
) as dag:
    sql_query = """
    CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INT NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    conn_id = 'postgres_conn'
    create_table = SQLExecuteQueryOperator(
        task_id = 'create_table',
        conn_id=conn_id,
        sql=sql_query
    )

create_table