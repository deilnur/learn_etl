from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from airflow.providers.postgres.operators.postgres import PostgresOperator

def transfer_products_to_stg(**kwargs):
    select_sql = '''select * from products
                    where last_update >= %(ds)s
                    and last_update < %(data_interval_end)s'''

    insert_sql = '''insert into stg.products(id, name, category, sub_category, created_dt, last_update,
                                            source_db, processed_dttm,execution_date) values %s'''
    now = datetime.datetime.now() + datetime.timedelta(hours = 3)
    
    src_hook = PostgresHook(postgres_conn_id = 'sales_db')
    dwh_hook = PostgresHook(postgres_conn_id = 'dwh')
   
    src_conn = src_hook.get_conn()
    dwh_conn = dwh_hook.get_conn()
    
    src_cur = src_conn.cursor()
    dwh_cur = dwh_conn.cursor()
    
    src_cur.execute(select_sql,vars = kwargs)
    #records = src_cur.fetchall()
    records = [i + ('SLS', now, kwargs['ds']) for i in src_cur.fetchall()]
    
    if records:
        execute_values(dwh_cur, insert_sql, records)
        dwh_conn.commit()
        print(f"Data transferred successfully! Transfered {len(records)} rows for period between {kwargs['ds']} and {kwargs['data_interval_end']}")
    else:
        print(f"No Data to transfer for period between {kwargs['ds']} and {kwargs['data_interval_end']} ")
    
    src_conn.close()
    dwh_conn.close()

with DAG('transfer_products',start_date = datetime.datetime(2022,1,1),schedule_interval='@daily',catchup=True, max_active_runs= 1) as dag:
    delete_data_from_stg = PostgresOperator(
        task_id = 'delete_data_from_stg',
        sql = 'sql/delete_from_table (depend on exec_date).sql',
        params = {'table_name': 'stg.products'},
        postgres_conn_id = 'dwh'
    )
    
    
    transfer_products_to_stg = PythonOperator(
        task_id = 'transfer_products_to_stg',
        python_callable=transfer_products_to_stg

    )

    delete_data_from_stg >> transfer_products_to_stg