from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

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

with DAG('etl_owid_covid_bash', schedule_interval="@once", default_args=default_args, catchup=False) as dag:
    task_welcome = BashOperator(
        task_id ='say_welcome',
        bash_command = 'echo "Hallo !"'
    )
    task_process_etl = BashOperator(
        task_id='task_process_etl',
        bash_command='python /opt/airflow/dags/script/etl_usecase1.py'
    )
    task_end = BashOperator(
        task_id = 'say_end',
        bash_command = 'echo "Good Bye ..."'
    )

task_welcome >> task_process_etl >> task_end