from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_transform',
    default_args=default_args,
    description='Runs dbt models daily',
    schedule='0 12 * * *',  # every day at noon
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'daily'],
)

# Define the dbt run command
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd ~/Development/loan_etl/dbt_transform && pipenv run dbt build',
    dag=dag,
)

gx_validation = BashOperator(
    task_id='gx_validation',
    bash_command='cd ~/Development/loan_etl/validation && pipenv run python debtor.py',
    dag=dag,
)

# Set task dependencies
dbt_run >> gx_validation