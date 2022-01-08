from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable, variable
import random

def say_hi(**kwargs):
    day = kwargs['ds']
    ti = kwargs['ti']
    print('Контекст:', day, type(day) )

with DAG("test_dag",start_date=datetime(2021,12,14,18,42,00), schedule_interval=None, catchup=True, tags = ['test'], dagrun_timeout=timedelta(seconds=15)) as dag:
    
    calculate_table_rows = PythonOperator(
        task_id = 'test_dag',
        python_callable = say_hi
    )

    [calculate_table_rows]