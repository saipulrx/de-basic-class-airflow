import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def greet():
	now = dt.datetime.now()
	t = now.strftime("%Y-%m-%d %H:%M")
	print('Halo Salam dari Apache Airflow' + t)

def respond():
    return 'Greet Responded Again'

default_args = {
    'owner': 'airflow',
    #'start_date': dt.datetime(2018, 9, 24, 10, 00, 00),
    'start_date': days_ago(2),
    'concurrency': 1,
    'retries': 0
}

with DAG('example_paralel_dag',
         catchup=False,
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         # schedule_interval=None,
         ) as dag:
    opr_hello = BashOperator(task_id='say_Hi',
                             bash_command='echo "Hi!!"')
    
    opr_hello2 = BashOperator(task_id='say_hello',
                             bash_command='echo "Hi hi!!"')

    opr_greet = PythonOperator(task_id='greet',
                               python_callable=greet)
    opr_sleep = BashOperator(task_id='sleep_me',
                             bash_command='sleep 5')

    opr_respond = PythonOperator(task_id='respond',
                                 python_callable=respond)

opr_hello >> opr_greet >> opr_sleep >> opr_respond
opr_hello >> opr_hello2 >> opr_respond