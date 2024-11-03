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

with DAG('etl_usecase123_new', schedule_interval="@once", default_args=default_args, catchup=False) as dag:
    task_welcome = BashOperator(
        task_id ='say_welcome',
        bash_command = 'echo "Hallo !"'
    )

    task_process_etl_usecase1 = BashOperator(
        task_id='task_process_etl_usecase1',
        bash_command='python /opt/airflow/dags/script/etl_usecase1-bahrain.py'
    )

    task_process_etl_usecase2 = BashOperator(
        task_id='task_process_etl_usecase2',
        bash_command='python /opt/airflow/dags/script/etl_usecase2-vendor2.py'
    )
    task_process_etl_usecase3 = BashOperator(
        task_id='task_process_etl_usecase3',
        bash_command='python /opt/airflow/dags/script/etl_usecase3-vendor2.py'
    )
    task_end = BashOperator(
        task_id = 'say_end',
        bash_command = 'echo "Good Bye ..."'
    )

task_welcome >> task_process_etl_usecase1 >> task_end
task_welcome >> task_process_etl_usecase2 >> task_process_etl_usecase3 >> task_end