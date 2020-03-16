import datetime as dt

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.mysql_operator import MySqlOperator


default_args = {
    'owner': 'taiyikuo',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 3, 14, 0, 0, 0),
    'concurrency': 1,
    'retries': 2,
    'email': 'roger-airflow@grindr.com',
    'email_on_retry': False,
    'email_on_failure': False
}


def provide_owner():
    owner = 'taiyikuo'
    print('ETL is starting for ' + owner)
    return owner


def load_files_to_db():
    print('Loading files to MySQL...')
    connection = MySqlHook(mysql_conn_id='mysql_default')
    connection.run('data/sql/load_sales_data.sql', autocommit=True)
    return True


with DAG('sales_data_etl', 
          default_args=default_args,
          schedule_interval='@daily') as dag:

    op_provide_owner = PythonOperator(
        task_id='provide_owner', python_callable=provide_owner)
    
    op_create_table = MySqlOperator(
        task_id='create_table', 
        sql='create_sales_table.sql',
        autocommit=True)
    
    op_load_files_to_db = MySqlOperator(
        task_id='load_files_to_db', 
        sql='load_sales_data.sql',
        autocommit=True)
    
    op_aggregate_monthly_sales = MySqlOperator(
        task_id='aggregate_monthly_sales', 
        sql='aggregate_monthly_sales.sql',
        autocommit=True)

    op_send_email = EmailOperator(
        task_id='send_email',
        to='roger-airflow@grindr.com',
        subject='Airflow Sales ETL Finished',
        html_content=""" <h3>DONE</h3> """,
        dag=dag)

op_provide_owner >> op_create_table >> op_load_files_to_db >> op_aggregate_monthly_sales>> op_send_email
