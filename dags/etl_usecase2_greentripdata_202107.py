from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

@dag(
    start_date=datetime(2025,1,7),
    schedule='@daily',
    catchup=False,
    tags=['postgresql','airflow'],
    default_args = default_args
)

def ingest_greentripdata_202107():
    start_job = BashOperator(
        task_id = 'start_job',
        bash_command = 'echo start ETL Pipeline green trip data 2021-07 to PostgreSQl Using Airflow'
    )

    etl_job = BashOperator(
        task_id = 'etl_job',
        bash_command = 'python /opt/airflow/dags/script/etl_usecase2_greentripdata_202107.py'
    )

    end_job = BashOperator(
        task_id = 'end_job',
        bash_command = 'echo end of ETL Pipeline green trip data 2021-07 to PostgreSQl Using Airflow'
    )

    start_job >> etl_job >> end_job
    
ingest_greentripdata_202107()