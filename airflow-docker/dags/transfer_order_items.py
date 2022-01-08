from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values
from airflow.hooks.postgres_hook import PostgresHook

def transfer_order_items(**kwargs):
    now = datetime.datetime.now()
    select_sql = '''select * from order_items where last_update > %(ds)s and last_update < %(data_interval_end)s'''
    insert_sql = '''insert into stg.order_items(order_id, product_id,quantity,price,currency_id,discount,created,last_update, source_db,processed_dttm )
                        values %s'''
    src_hook = PostgresHook(postgres_conn_id = 'sales_db')
    dwh_hook = PostgresHook(postgres_conn_id = 'dwh')

    src_con = src_hook.get_conn()
    dwh_con = dwh_hook.get_conn()

    src_cur = src_con.cursor()
    dwh_cur = dwh_con.cursor()

    src_cur.execute(select_sql, vars = kwargs)
    records = [i + ('SLS', now) for i in src_cur.fetchall()]
    if records:
        execute_values(dwh_cur, insert_sql, records)
        dwh_con.commit()
        print(f"Data transferred successfully! Transfered {len(records)} rows for period between {kwargs['ds']} and {kwargs['data_interval_end']}")
    else:
        print(f"No Data to transfer for period between {kwargs['ds']} and {kwargs['data_interval_end']} ")
    
    src_con.close()
    dwh_con.close()

with DAG('transfer_order_items', start_date=datetime.datetime(2022,1,1), schedule_interval='@daily', catchup=False, tags=['transfer']) as dag:
    transfer_order_items = PythonOperator(
        task_id = 'transfer_order_items',
        python_callable=transfer_order_items       
    )
transfer_order_items
