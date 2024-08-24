from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
from pandasql import sqldf
from sqlalchemy import create_engine
import pandas as pd


default_args = {
    "owner": "Saipul",
    "start_date": datetime(2024, 7, 21),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "msaipulrx@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def db_connection():
    # Konfigurasi database
    db_uri = 'postgresql+psycopg2://postgres:12345@host.docker.internal:5432/demo_intro_sql'
    #f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
    # Buat engine SQLAlchemy
    engine = create_engine(db_uri)
    return engine

def etl_data():
    conn = db_connection()
    #get data from csv file
    df_covid_all = pd.read_csv('/opt/airflow/dags/dataset/owid-covid-data.csv')

    #transform process
    #filter data total_case is not null
    #and total_deaths is not null
    #and location indonesia
    #and new_deaths more than 5
    df_idn = sqldf(''' select * from df_covid_all where total_cases is not null and total_deaths is not null and location ='Indonesia' and 
    new_deaths > 5 ''')

    #jumlah record yang akan di save ke table
    count_data = df_idn.shape[0]

    #load data after transformation to postgresql
    TABLE_NAME = 'covid_idn_test'
    df_idn.to_sql(name=TABLE_NAME, con=conn,index=False)
    print(f'Total Record has been inserted are {count_data} to table {TABLE_NAME} ')

def welcome():
    print('Welcome to Apache Airflow')
    print('First run this task then execute task transform_csv and transform_json parallel')
    print('Then finally run task end_task')

def end_task():
    print('This task is executed after run task welcome, transform_csv and trans- form_json')
    print('End')

with DAG('etl_owid_covid', schedule_interval="@once", default_args=default_args, catchup=False) as dag:
    task_welcome = PythonOperator(task_id='say_welcome', python_callable=welcome)
    task_etl = PythonOperator(task_id='task_etl', python_callable=etl_data)
    task_end = PythonOperator(task_id='say_end', python_callable=end_task)

task_welcome >> task_etl >> task_end