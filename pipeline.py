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

# Extract data from CSV 
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='''
    cut -d"," -f1,2 /tmp/data/source_data.csv > /tmp/data/csv_data.csv
    ''',
    dag=dag
)

# Extract data from TSV file
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='''
    cut -f1,3 /tmp/data/source_data.tsv > /tmp/data/tsv_data.csv
    ''',
    dag=dag
)

# Extract data from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='''
    cut -c1-5,10-14 /tmp/data/source_data_fixed_width.txt > /tmp/data/fixed_width_data.csv
    ''',
    dag=dag
)