from airflow import DAG
from datetime import datetime, date, timedelta
from airflow.operators.python import PythonOperator, task
from airflow.hooks.postgres_hook import PostgresHook
import random

def download_customers_and_products_id (ti):
    sql_customers = 'select id from customers'
    sql_products = 'select id from products'
    postgres_hook = PostgresHook(postgres_conn_id="sales_db")
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_customers)
    records = cursor.fetchall()
    customers_id = []
    for record in records:
        customers_id.append(record[0])
    ti.xcom_push(key = 'customers', value = customers_id)
    
    cursor.execute(sql_products)
    records = cursor.fetchall()
    products_id = []
    for record in records:
        products_id.append(record[0])
    ti.xcom_push(key = 'products', value = products_id)
    
def generate_order(ti):
    customers = ti.xcom_pull(key = 'customers', task_ids = ['download_customers_and_products_id'])
    customer_id = random.choice(customers[0])
    order_date = date.today()
    ship_date = date.today() + timedelta(days = random.randint(1,100))
    ship_mode = random.choice(['First Class', 'Second Class', 'Standard Class']) 
    sql_insert = f'''insert into orders(
        customer_id
        ,order_date
        ,ship_date
        ,ship_mode
    ) values
        ({customer_id},'{order_date}','{ship_date}','{ship_mode}');'''
    postgres_hook = PostgresHook(postgres_conn_id="sales_db")
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_insert)
    connection.commit()

    # select created orders id
    sql_select = 'select max(id) from orders'
    cursor.execute(sql_select)
    order_id = cursor.fetchall()[0][0]
    ti.xcom_push(key = 'created_order_id', value = order_id)

def generate_order_items(ti):
    order_id = ti.xcom_pull(key = 'created_order_id', task_ids = ['generate_order'])[0]
    products = ti.xcom_pull(key = 'products', task_ids = ['download_customers_and_products_id'])[0]
    sql_insert = '''insert into order_items(
                        order_id
                        ,product_id
                        ,quantity
                        ,price
                        ,currency_id
                        ,discount) values '''
    values = ''
    rows_count = random.randint(0,random.randint(0,len(products) - 1))
    for i in range(0,rows_count):
        sep = ','
        if i == rows_count - 1:
            sep = ';'
        product_id = products.pop(random.randint(0,len(products) - 1))
        row = f'({order_id}, {product_id}, {random.randint(1,100)}, {random.randint(1000, 10000) / 10}, 1, {random.randrange(0,20) / 100})' + sep
        values += row
    sql_insert += values
    postgres_hook = PostgresHook(postgres_conn_id="sales_db")
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_insert)
    connection.commit()




    

with DAG('orders_generator', start_date = datetime(2021,1,12),schedule_interval=None, catchup=False,tags=['gen']) as dag:
    download_customers_and_products_id = PythonOperator(
        task_id = 'download_customers_and_products_id',
        python_callable=download_customers_and_products_id
    )
    
    generate_orders = PythonOperator(
        task_id = 'generate_order',
        python_callable = generate_order
    )

    generate_order_items = PythonOperator(
        task_id = 'generate_order_items',
        python_callable = generate_order_items
 

    )


    download_customers_and_products_id >> generate_orders >> generate_order_items