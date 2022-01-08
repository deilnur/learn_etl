from datetime import date, datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from sqlalchemy import create_engine

with DAG('dds_products', start_date=datetime(2022,1,1), schedule_interval='@daily', catchup=False, tags = ['dds']) as dag:
    dds_products = PostgresOperator(
        task_id = 'dds_products',
        sql = 'sql/load_dds_products.sql',
        postgres_conn_id = 'dwh'
    )
dds_products