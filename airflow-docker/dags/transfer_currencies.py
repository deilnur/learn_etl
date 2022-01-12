import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

def transfer_currencies(**kwargs):
    now = datetime.datetime.now()
    select_sql = 'select * from currencies where last_update >= %(ds)s and last_update < %(data_interval_end)s '
    insert_sql = '''insert into stg.currencies (id, code,name,created,last_update,source_db,processed_dttm,execution_date)
                        values %s'''
    src_hook = PostgresHook(postgres_conn_id = 'sales_db')
    dwh_hook = PostgresHook(postgres_conn_id = 'dwh')

    src_conn = src_hook.get_conn()
    dwh_conn = dwh_hook.get_conn()

    src_cur = src_conn.cursor()
    dwh_cur = dwh_conn.cursor()

    src_cur.execute(select_sql, vars = kwargs)
    records = [i + ('SLS', now, kwargs['ds']) for i in src_cur.fetchall()]
    if records:
        execute_values(dwh_cur,insert_sql,records)
        dwh_conn.commit()
        print(f"Data transferred successfully! Transfered {len(records)} rows for period between {kwargs['ds']} and {kwargs['data_interval_end']}")
    else:
        print(f"No Data to transfer for period between {kwargs['ds']} and {kwargs['data_interval_end']} ")
    
    dwh_conn.close()
    src_conn.close()


with DAG('transfer_currencies', start_date = datetime.datetime(2022,1,1), schedule_interval='@daily', catchup=True,tags=['transfer']) as dag:
    delete_data_from_stg = PostgresOperator(
        task_id = 'delete_data_from_stg',
        sql = 'sql/delete_from_table (depend on exec_date).sql',
        params = {'table_name': 'stg.currencies'},
        postgres_conn_id = 'dwh'
    )
    
    transfer_currencies_to_stg = PythonOperator(
        task_id = 'transfer_currencies_to_stg',
        python_callable=transfer_currencies
    )

delete_data_from_stg >> transfer_currencies_to_stg