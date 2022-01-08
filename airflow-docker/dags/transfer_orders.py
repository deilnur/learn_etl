from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from sqlalchemy.sql.sqltypes import DateTime

def transfer_orders(**kwargs):
    now = datetime.datetime.now()
    select_sql = '''select id,customer_id, order_date, ship_date, ship_mode from orders where last_update > %(ds)s and 
                                                                                last_update < %(data_interval_end)s'''
    insert_sql = '''insert into stg.orders 
                    (source_id
                    ,customer_id
                    ,order_date
                    ,ship_date
                    ,ship_mode
                    ,source_db
                    ,processed_dttm) values %s'''
    src_hook = PostgresHook(postgres_conn_id ='sales_db')
    dwh_hook = PostgresHook(postgres_conn_id = 'dwh')

    src_conn = src_hook.get_conn()
    dwh_conn = dwh_hook.get_conn()

    src_cur = src_conn.cursor()
    dwh_cur = dwh_conn.cursor()

    src_cur.execute(select_sql, vars = kwargs)
    records = [i + ('SLS', now) for i in src_cur.fetchall()]
    if records:
        execute_values(dwh_cur, insert_sql, records)
        dwh_conn.commit()
        print(f"Data transferred successfully! Transfered {len(records)} rows for period between {kwargs['ds']} and {kwargs['data_interval_end']}")
    else:
        print(f"No Data to transfer for period between {kwargs['ds']} and {kwargs['data_interval_end']} ")
    src_conn.close()
    dwh_conn.close()

with DAG('transfer_orders', start_date=datetime.datetime(2022, 1, 1), schedule_interval='@daily', catchup= False) as dag:
    transfer_orders = PythonOperator(
        task_id = 'transfer_orders',
        python_callable=transfer_orders
    )

transfer_orders