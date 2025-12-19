from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG definition
dag = DAG(
    dag_id='final_project_data_pipeline',
    default_args=default_args,
    description='Final project ETL pipeline',
    schedule_interval=None,
    catchup=False
)

# Unzip source data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='''
    mkdir -p /tmp/data &&
    tar -xvf /tmp/data/source_data.tar -C /tmp/data
    ''',
    dag=dag
)