from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
import pandas as pd

default_args = {
    "owner": "Saipul",
    "start_date": datetime(2024, 6, 7),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "msaipulrx@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def transform_csv():
    # get data csv file
    data = pd.read_csv('https://media.geeksforgeeks.org/wp-content/uploads/nba.csv')
    data.dropna(inplace=True)

    # tranformation split column and convert data type to int64 # split firstname and lastname
    new = data['Name'].str.split(" ", n = 1, expand = True)
    data['Firstname'] = new[0]
    data['Lastname'] = new[1]
    data.drop(columns = ['Name'], inplace = True)

    # convert data type column age become int64
    data['Age'] = data['Age'].astype('int64')

    # convert data type column number become int64
    data['Number'] = data['Number'].astype('int64')

    # tampilkan data setelah transformasi
    print(data.head(10))

    # save to csv file
    data.to_csv('/opt/airflow/dags/dataset/data_output/data_after_transform.csv')
    print('Data berhasil disimpan ke csv file')

def welcome():
    print('Welcome to Apache Airflow')
    print('First run this task then execute task transform_csv and transform_json parallel')
    print('Then finally run task end_task')

def end_task():
    print('This task is executed after run task welcome, transform_csv and trans- form_json')
    print('End')

with DAG('transform_file', schedule_interval="@once", default_args=default_args, catchup=False) as dag:
    task_welcome = PythonOperator(task_id='say_welcome', python_callable=welcome)
    task_transform_csv = PythonOperator(task_id='trans_csv', python_callable=transform_csv)
    task_end = PythonOperator(task_id='say_end', python_callable=end_task)

task_welcome >> task_transform_csv >> task_end