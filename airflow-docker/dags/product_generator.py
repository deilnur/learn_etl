from airflow import DAG
from datetime import datetime, date, timedelta
from airflow.operators.python import PythonOperator, task
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

def check_products(ti):
    file = '/opt/airflow/dic/products.csv'
    products = pd.read_csv(file)
    new_products = products.loc[products['downloaded'] == 0, ['Product Name', 'Category', 'Sub-Category']]
    product_to_insert = new_products.sample()
    name, category, sub_category = [i[0] for i in product_to_insert.T.values]
    row = '\'' + name + '\'' + ', ' + '\'' + category +  '\'' + ', ' + '\'' + sub_category + '\''
    ti.xcom_push(key = 'row_to_insert', value = row)

    products.loc[product_to_insert.index, ['downloaded']] = 1
    products.to_csv(file, index = False)

def insert_products(ti):
    row_to_insert = ti.xcom_pull(key = 'row_to_insert', task_ids = ['check_products'])[0]
        
    sql_products = f'''insert into products
                        (name, category, sub_category)
                        values
                            ({row_to_insert});'''
    postgres_hook = PostgresHook(postgres_conn_id="sales_db")
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_products)
    connection.commit()

with DAG('product_generator', start_date = datetime(2021,10,1), schedule_interval=None, catchup=False, tags = ['gen']) as dag:

    check_products = PythonOperator(
        task_id = 'check_products',
        python_callable = check_products
    )

    insert_products = PythonOperator(
        task_id = 'insert_products',
        python_callable = insert_products
    )

check_products >> insert_products